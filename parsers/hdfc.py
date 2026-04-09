"""
parsers/hdfc.py
────────────────
HDFC Bank Statement Parser — Collapsed Table Format

Structure per page: 1 giant table row, all transactions newline-separated.
Col 0: Dates (DD/MM/YY) — 1:1 with transactions
Col 1: Narrations — some wrap across 2 lines; page boundaries cause overflow
Col 2: Ref numbers
Col 4: Withdrawals (DR amounts only)
Col 5: Deposits (CR amounts only)
Col 6: Closing balances — 1:1 with transactions

Fixes applied:
1. Strip page-boundary overflow lines from narration col start
2. Group narrations using VALID_START (explicit prefixes only, no generic digits)
3. CR/DR from balance diff — exact, no guessing
4. Amount = abs(diff) — exact match
"""

import re
import pdfplumber

from parsers.base    import BaseParser
from core.normalizer import normalize, normalize_date
from core.utils      import parse_amt

_DATE_PAT = re.compile(r'^\d{2}/\d{2}/\d{2,4}$')
_AMT_PAT  = re.compile(r'^[\d,]+\.\d{2}$')

_SUMMARY_RE = re.compile(
    r'opening\s*balance\s*dr\s*count\s*cr\s*count\s*debits\s*credits\s*closing\s*bal'
    r'[\s\r\n]+([\d,]+\.\d{2})',
    re.IGNORECASE
)

# ONLY explicit known prefixes — no generic [0-9]{5,} to avoid digit-overflow false positives
_VALID_START = re.compile(
    r'^(NEFT|IMPS|POS|FUEL|REV|SALARY|IB\s?BILL|CASH|EMI|FT[\s\-]|CREDIT|AIRTEL|'
    r'3RD|GHDF|CHQ|05301|00141|27881|50100|100358)',
    re.IGNORECASE
)


def _strip_page_overflow(narr_lines: list) -> list:
    """Remove leading overflow lines from previous page's last narration."""
    result = []
    stripping = True
    for line in narr_lines:
        if stripping and not _VALID_START.match(line):
            continue
        stripping = False
        result.append(line)
    return result


def _group_narrations(narr_lines: list, n_txns: int) -> list:
    """
    Group narration lines into exactly n_txns groups.
    Each VALID_START line begins a new group.
    All other lines are appended to the previous group.
    """
    groups = []
    for line in narr_lines:
        if _VALID_START.match(line):
            groups.append(line)
        else:
            if groups:
                groups[-1] = (groups[-1] + ' ' + line).strip()
            else:
                groups.append(line)

    # Too many groups → merge trailing ones
    while len(groups) > n_txns and len(groups) > 1:
        groups[-2] = (groups[-2] + ' ' + groups[-1]).strip()
        groups.pop()

    # Too few → pad with empty
    while len(groups) < n_txns:
        groups.append('')

    return groups[:n_txns]


class HDFCParser(BaseParser):

    _IFSC_PREFIX    = 'hdfc0'
    _COLUMN_SIGNALS = ['narration', 'closing balance']

    def detect(self, pdf_path: str) -> bool:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = (pdf.pages[0].extract_text() or '').lower()
                return ('hdfc0' in text and 'narration' in text and
                        ('closing balance' in text or 'closingbalance' in text))
        except Exception:
            return False

    def parse(self, pdf_path: str) -> list:
        all_dates      = []
        all_balances   = []
        all_narrations = []
        all_refs       = []
        opening_bal    = None
        full_text      = ''

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    full_text += (page.extract_text() or '') + '\n'
                    tables = page.extract_tables()
                    if not tables:
                        continue

                    table = max(tables, key=len)

                    for row in table:
                        if not row or len(row) < 5:
                            continue

                        col0 = str(row[0] or '')
                        col1 = str(row[1] or '')
                        col2 = str(row[2] or '')
                        col6 = str(row[6] or '') if len(row) > 6 else ''
                        col5 = str(row[5] or '') if len(row) > 5 else ''

                        if 'date' in col0.lower() or 'narration' in col1.lower():
                            continue

                        dates = [d.strip() for d in col0.split('\n')
                                 if _DATE_PAT.match(d.strip())]
                        if not dates:
                            continue

                        bal_src = col6 if col6.strip() else col5
                        balances = [b.strip() for b in bal_src.split('\n')
                                    if _AMT_PAT.match(b.strip().replace(',', ''))]

                        if not balances or len(dates) != len(balances):
                            self._log(f"Skip: dates={len(dates)} bals={len(balances)}")
                            continue

                        n = len(dates)
                        narr_lines_raw = [ln.strip() for ln in col1.split('\n') if ln.strip()]
                        ref_lines      = [r.strip()  for r  in col2.split('\n') if r.strip()]

                        narr_clean  = _strip_page_overflow(narr_lines_raw)
                        narr_groups = _group_narrations(narr_clean, n)

                        for i, (d, b) in enumerate(zip(dates, balances)):
                            all_dates.append(d)
                            all_balances.append(parse_amt(b))
                            all_narrations.append(narr_groups[i] if i < len(narr_groups) else '')
                            all_refs.append(ref_lines[i] if i < len(ref_lines) else '')

        except Exception as e:
            import traceback
            self._log(f"Fatal: {e}")
            traceback.print_exc()
            return []

        opening_bal = self._extract_opening_balance(full_text)
        self._log(f"OB={opening_bal}  Txns={len(all_dates)}")

        if not all_dates:
            return []

        transactions = []
        for i, (date_raw, bal) in enumerate(zip(all_dates, all_balances)):
            prev_bal = (opening_bal if opening_bal is not None else all_balances[0]) \
                       if i == 0 else all_balances[i - 1]

            diff = round(bal - prev_bal, 2)

            if diff > 0:
                txn_type = 'CR'
                amount   = diff
            elif diff < 0:
                txn_type = 'DR'
                amount   = abs(diff)
            else:
                txn_type = 'DR'
                amount   = 0.0

            transactions.append({
                'date':            normalize_date(date_raw) or date_raw,
                'desc':            all_narrations[i] if i < len(all_narrations) else '',
                'amount':          round(amount, 2),
                'type':            txn_type,
                'balance':         bal,
                'reference':       all_refs[i] if i < len(all_refs) else '',
                'opening_balance': opening_bal if i == 0 else None,
                '_type_locked':    True,
            })

        self._log(f"Built: {len(transactions)} transactions")
        result = normalize(transactions, opening_balance=opening_bal)
        self._log(f"Final: {len(result)}")
        return result

    def _extract_opening_balance(self, full_text: str) -> float:
        m = _SUMMARY_RE.search(full_text)
        if m:
            val = parse_amt(m.group(1))
            if val >= 0:
                self._log(f"OB from summary: {val}")
                return val
        lines = full_text.splitlines()
        for i, line in enumerate(lines):
            if 'opening balance' in line.lower():
                for j in range(i, min(i + 6, len(lines))):
                    nums = re.findall(r'[\d,]+\.\d{2}', lines[j])
                    if nums:
                        val = parse_amt(nums[0])
                        if val >= 0:
                            return val
        return None
    