"""
parsers/sbi.py
───────────────
State Bank of India Statement Parser v4.
Layout: raw text, branch-printed format.
Ground truth: Opening 74171.84 | Dr:86 Cr:30 | Closing 638176.32
"""

import re
import pdfplumber
from datetime import datetime, timedelta

from parsers.base    import BaseParser
from core.normalizer import normalize
from core.utils      import parse_amt

_RE_VALUE_DATE = re.compile(r'\(?\s*[Vv]alue\s+[Dd]ate\s*:\s*[\d\-/\.]+\s*\)?')
_RE_WHITESPACE = re.compile(r'\s+')

_SBI_SKIP_PHRASES = [
    'statement of account', 'state bank of india',
    'branch code', 'branch phone', 'ifsc', 'micr',
    'account no', 'product', 'currency',
    'cleared balance', 'uncleared amount', 'mod bal',
    'monthly average balance', 'limit :', 'drawing power',
    'int. rate', 'nominee', 'account open date', 'account status',
    'statement from', 'statement summary',
    'in case your account', 'end of statement',
    'sirsa road', 'vpo ', 'disstt ',
    'distt ', 's/o ', 'e-mail',
    'bappa state bank',
    'post date', 'value date',
    'debit credit balance',
    'report date', 'date :', 'time :',
    'dr. count', 'cr. count',
]

_TXN_START   = re.compile(
    r'^(\d{2}/\d{2}/\d{2,4})(?:\s+\d{2}/\d{2}/\d{2,4})?\s+(.*)',
    re.DOTALL
)
_AMOUNT_PAT  = re.compile(r'[\d,]+\.\d{2}')
_OPENING_BAL = re.compile(
    r'brought\s+forward\s*:?\s*([\d,]+\.\d{2})',
    re.IGNORECASE
)


class SBIParser(BaseParser):

    _DETECT_KEYWORDS = [
        'state bank of india', 'onlinesbi', 'sbi yono',
        'sbin0', 'sbi bank'
    ]

    def detect_from_text(self, text_low: str) -> bool:
        return any(k in text_low for k in self._DETECT_KEYWORDS)

    def detect(self, pdf_path: str) -> bool:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = (pdf.pages[0].extract_text() or '').lower()
                return self.detect_from_text(text)
        except Exception:
            return False

    def parse(self, pdf_path: str) -> list:
        raw_txns        = []
        pdf_opening_bal = None

        try:
            with pdfplumber.open(pdf_path) as pdf:

                # Pass 1: opening balance
                for pg in pdf.pages[:2]:
                    text = pg.extract_text() or ''
                    m    = _OPENING_BAL.search(text)
                    if m:
                        pdf_opening_bal = self._to_float(m.group(1))
                        self._log(f"Opening balance: ₹{pdf_opening_bal:,.2f}")
                        break

                # Pass 2: block-based extraction
                current_date  = None
                current_lines = []

                for page in pdf.pages:
                    text  = page.extract_text() or ''
                    lines = text.splitlines()

                    for raw_line in lines:
                        line = raw_line.strip()
                        if not line:
                            continue
                        if self._skip_line(line):
                            continue
                        if re.match(r'^(brought|carried)\s+forward', line, re.IGNORECASE):
                            continue

                        anchor = _TXN_START.match(line)
                        if anchor:
                            self._flush(current_date, current_lines, raw_txns)
                            current_date  = anchor.group(1)
                            rest          = anchor.group(2).strip()
                            current_lines = [rest] if rest else []
                        else:
                            if current_date is not None:
                                current_lines.append(line)

                self._flush(current_date, current_lines, raw_txns)

        except Exception as e:
            import traceback
            self._log(f"Fatal: {e}")
            traceback.print_exc()
            return []

        self._log(f"Raw transactions parsed: {len(raw_txns)}")

        if not raw_txns:
            return []

        # Sort by date
        raw_txns.sort(key=self._safe_date_sort)

        # Date-range filter (remove stray future-dated entries)
        if len(raw_txns) >= 2:
            bulk_dates = [self._safe_date_sort(t) for t in raw_txns[:-2]]
            if bulk_dates:
                max_bulk = max(bulk_dates)
                raw_txns = [
                    t for t in raw_txns
                    if self._safe_date_sort(t) <= max_bulk + timedelta(days=7)
                ]
                self._log(f"After date-range filter: {len(raw_txns)} transactions")

        # CR/DR resolution
        prev_bal = pdf_opening_bal if pdf_opening_bal is not None else 0.0
        for txn in raw_txns:
            curr_bal   = txn['balance']
            amt        = txn['amount']
            diff       = round(curr_bal - prev_bal, 2)
            tol        = max(1.0, round(abs(amt) * 0.015, 2))
            desc_upper = txn.get('desc', '').upper()

            forced_type = self._forced_type_from_desc(desc_upper)

            if forced_type:
                txn['type'] = forced_type
                if abs(abs(diff) - amt) > tol:
                    txn['amount'] = round(abs(diff), 2)
            elif abs(diff - amt) <= tol:
                txn['type'] = 'CR'
            elif abs(diff + amt) <= tol:
                txn['type'] = 'DR'
            else:
                txn['type']   = 'CR' if diff > 0 else 'DR'
                txn['amount'] = round(abs(diff), 2)

            prev_bal = curr_bal

        result = normalize(raw_txns, opening_balance=pdf_opening_bal)

        if result and pdf_opening_bal is not None:
            result[0]['opening_balance'] = round(pdf_opening_bal, 2)

        self._log(f"Final transactions: {len(result)}")
        return result

    # ── Helpers ───────────────────────────────────────────

    @staticmethod
    def _forced_type_from_desc(desc_upper: str):
        if any(k in desc_upper for k in [
            'UPI/DR', 'WDL TFR', 'WDL TRF', 'ATM WDL',
            'DEBIT-PM', 'TO TRANSFER'
        ]):
            return 'DR'
        if any(k in desc_upper for k in [
            'UPI/CR', 'DEP TFR', 'BY TRANSFER',
            'CASH DEPOSIT', 'SALARY TRF', 'IMPS BRN SALARY'
        ]):
            return 'CR'
        return None

    def _flush(self, date_str, lines, result_list):
        if not date_str or not lines:
            return
        amount, balance = self._get_amounts(lines)
        if balance == 0.0:
            return
        narr = self._build_narration(lines)
        result_list.append({
            'date':    date_str,
            'desc':    narr,
            'amount':  round(amount, 2),
            'balance': round(balance, 2),
            'type':    'DR',
        })

    def _get_amounts(self, lines: list):
        for line in reversed(lines):
            nums = _AMOUNT_PAT.findall(line)
            if len(nums) >= 2:
                return self._to_float(nums[-2]), self._to_float(nums[-1])
            elif len(nums) == 1:
                balance = self._to_float(nums[0])
                for prev in reversed(lines[:-1]):
                    pnums = _AMOUNT_PAT.findall(prev)
                    if pnums:
                        return self._to_float(pnums[-1]), balance
                return 0.0, balance
        return 0.0, 0.0

    @staticmethod
    def _build_narration(lines: list) -> str:
        parts = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if re.match(r'^[_\-=]{5,}$', line):
                continue
            if re.match(r'^\d+$', line):
                continue
            if re.match(r'^[\d,\.\s]+$', line):
                continue
            line = re.sub(r'\s+[\d,]+\.\d{2}.*$', '', line).strip()
            line = re.sub(r'\b\d{9,}\b', '', line).strip()
            line = re.sub(r'^4[89]\d{7,}\s*', '', line).strip()
            if line:
                parts.append(line)

        narr = ' '.join(parts)
        narr = re.sub(r'(UPI/(?:DR|CR)/)\d{6,}/', r'\1', narr, flags=re.IGNORECASE)
        narr = re.sub(r'([A-Z]{2,})\s+([A-Z]{2,})(?=/)', r'\1\2', narr)
        narr = _RE_VALUE_DATE.sub('', narr)
        narr = _RE_WHITESPACE.sub(' ', narr).strip()
        narr = re.sub(r'^[-–/\s]+', '', narr).strip()
        narr = re.sub(r'\s*-\s*$', '', narr).strip()
        narr = re.sub(r'(DEPOSIT|TRANSFER|WITHDRAWAL)([A-Z])', r'\1 \2', narr)
        return narr[:200]

    @staticmethod
    def _skip_line(line: str) -> bool:
        lo = line.lower().strip()
        if not lo:
            return True
        if re.match(r'^page\s*no\s*\.?\s*:?\s*\d+', lo):
            return True
        if re.match(r'^\*-+\s*end of statement', lo):
            return True
        if re.match(r'^post\s+date\s+value\s+date', lo):
            return True
        return any(p in lo for p in _SBI_SKIP_PHRASES)

    @staticmethod
    def _to_float(s: str) -> float:
        try:
            return float(re.sub(r'[,\s]', '', s))
        except Exception:
            return 0.0

    @staticmethod
    def _safe_date_sort(txn):
        for fmt in ('%d/%m/%y', '%d/%m/%Y'):
            try:
                return datetime.strptime(txn['date'], fmt)
            except Exception:
                continue
        return datetime.min
    