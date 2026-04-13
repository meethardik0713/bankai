"""
parsers/hdfc.py
────────────────
HDFC Bank Statement Parser — Text-Line Based (v5.1)

Raw text format per line:
  DD/MM/YY  NARRATION  REF_NO  DD/MM/YY  [AMT1]  [AMT2]  CLOSING_BAL

CR/DR = determined by balance diff (prev_balance → closing_balance)
Amount = abs(diff) — exact match always

v5.1: Fixed ref number pattern to allow alphanumeric (e.g. 0000GF4640465798)
      All 73 transactions now correctly extracted.
"""

import re
import pdfplumber

from parsers.base    import BaseParser
from core.normalizer import normalize, normalize_date
from core.utils      import parse_amt

_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{2,4}$')

_SUMMARY_RE = re.compile(
    r'opening\s*balance[\s\S]{0,200}?(\d{1,3}(?:,\d{3})*\.\d{2})',
    re.IGNORECASE
)

_SKIP_RE = re.compile(
    r'^(PageNo|AccountBranch|Address|BESIDE|CHANDA|City|State|Phoneno|ODLimit|'
    r'Currency|Email|CustID|AccountNo|A\/COpen|AccountStatus|RTGS|BranchCode|'
    r'HDFCBANK|Contents|Registered|MR\.|SHORE|PLOT|KAVURI|HYDERABAD500|'
    r'TELANGANA|JOINT|Nomination|Statementof|From\s*:|Generated|MICR|Product|'
    r'\*Closing|HDFCBank|Date\s+Narration|Closingbalance|'
    r'DrCount|CrCount|Debits|Credits|ClosingBal|STATEMENTSumm)',
    re.IGNORECASE
)

# Ref no: alphanumeric, 10-20 chars (covers pure digit AND mixed like 0000GF4640465798)
_TXN_RE = re.compile(
    r'^(\d{2}/\d{2}/\d{2,4})\s+'       # date
    r'(.+?)\s+'                          # narration
    r'([A-Z0-9]{10,20})\s+'             # ref no (alphanumeric)
    r'(\d{2}/\d{2}/\d{2,4})\s+'        # value date
    r'((?:[\d,]+\.\d{2}\s+){0,2})'     # 0-2 middle amounts
    r'([\d,]+\.\d{2})\s*$'             # closing balance
)


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
        raw_lines   = []
        full_text   = ''

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text() or ''
                    full_text += text + '\n'
                    for line in text.split('\n'):
                        raw_lines.append(line.strip())
        except Exception as e:
            import traceback
            self._log(f"Fatal PDF read: {e}")
            traceback.print_exc()
            return []

        opening_bal = self._extract_opening_balance(full_text)
        self._log(f"Opening balance: {opening_bal}")

        raw_txns = []
        pending  = None

        for line in raw_lines:
            if not line:
                continue
            if _SKIP_RE.match(line):
                if pending:
                    raw_txns.append(pending)
                    pending = None
                continue

            m = _TXN_RE.match(line)
            if m:
                if pending:
                    raw_txns.append(pending)

                pending = {
                    'date_raw':  m.group(1),
                    'desc':      m.group(2).strip(),
                    'reference': m.group(3).strip(),
                    'balance':   parse_amt(m.group(6)),
                }
            else:
                # Continuation line — append to narration
                if pending and line:
                    if (not _DATE_RE.match(line) and
                        not re.match(r'^[\d,]+\.\d{2}$', line) and
                        not _SKIP_RE.match(line)):
                        pending['desc'] = (pending['desc'] + ' ' + line).strip()

        if pending:
            raw_txns.append(pending)

        self._log(f"Raw txns extracted: {len(raw_txns)}")

        if not raw_txns:
            return []

        # CR/DR from balance diff
        transactions = []
        prev_bal = opening_bal if opening_bal is not None else 0.0

        for i, t in enumerate(raw_txns):
            curr_bal = t['balance']
            diff     = round(curr_bal - prev_bal, 2)

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
                'date':            normalize_date(t['date_raw']) or t['date_raw'],
                'desc':            t['desc'],
                'amount':          round(amount, 2),
                'type':            txn_type,
                'balance':         curr_bal,
                'reference':       t['reference'],
                'opening_balance': opening_bal if i == 0 else None,
                '_type_locked':    True,
            })

            prev_bal = curr_bal

        self._log(f"Built {len(transactions)} transactions")
        result = normalize(transactions, opening_balance=opening_bal)
        self._log(f"Final after normalize: {len(result)}")
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
    