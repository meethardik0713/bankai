"""
parsers/canara.py
──────────────────
Canara Bank ePassbook Parser.
Layout: raw text, no table borders.
"""

import re
import pdfplumber

from parsers.base    import BaseParser
from core.normalizer import normalize, normalize_date
from core.utils      import parse_amt, extract_opening_balance_from_pdf

_RE_VALUE_DATE  = re.compile(r'\(?\s*[Vv]alue\s+[Dd]ate\s*:\s*[\d\-/\.]+\s*\)?')
_RE_CHQ_SUFFIX  = re.compile(r'\s*Chq\s*[:.]?\s*[\dA-Za-z]*\s*$', re.IGNORECASE)
_RE_WHITESPACE  = re.compile(r'\s+')

_SKIP_PHRASES = [
    'opening balance', 'closing balance', 'brought forward',
    'carried forward', 'page total', 'grand total',
    'statement summary', 'this is a computer',
    'generated on', 'printed on', 'account summary',
    'nominee', 'ifsc code', 'end of statement',
]

_ADDRESS_FRAGMENTS = [
    'ghaziabad', 'uttar pradesh', 'delhi', 'noida',
    'gurugram', 'faridabad', 'mumbai', 'bangalore',
    'siddharth vihar', 'siddhartham', 'gaur ',
    'tower ', 'sector-', 'plot no', ' in 2010',
    'vihar ',
]


class CanaraParser(BaseParser):

    _DETECT_KEYWORDS = ['canara bank', 'cnrb', 'canara aspire',
                        'canara savings', 'canarabank']

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
        pdf_opening_bal = extract_opening_balance_from_pdf(pdf_path)

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text          = page.extract_text() or ''
                    lines         = [l.strip() for l in text.splitlines()]
                    narration_buf = []

                    for line in lines:
                        if not line:
                            continue
                        if self._skip_line(line):
                            continue
                        txn = self._parse_txn_line(line)
                        if txn:
                            txn['desc'] = self._clean_desc(' '.join(narration_buf))
                            narration_buf = []
                            raw_txns.append(txn)
                            continue
                        if re.match(r'^Chq\s*:', line, re.IGNORECASE):
                            continue
                        narration_buf.append(line)

        except Exception as e:
            import traceback
            self._log(f"Fatal: {e}")
            traceback.print_exc()

        self._log(f"Raw transactions: {len(raw_txns)}")
        result = normalize(raw_txns)

        if result and pdf_opening_bal is not None:
            result[0]['opening_balance'] = round(pdf_opening_bal, 2)
            self._log(f"Opening balance overridden from PDF: ₹{pdf_opening_bal:,.2f}")

        return result

    # ── Helpers ───────────────────────────────────────────

    def _skip_line(self, line: str) -> bool:
        lo = line.lower().strip()
        if re.match(r'^date\s+particulars\s+deposits\s+withdrawals\s+balance', lo):
            return True
        if re.match(r'^page\s+\d+', lo):
            return True
        if any(p in lo for p in _SKIP_PHRASES):
            return True
        if any(lo.startswith(p) for p in [
            'statement for', 'branch code', 'customer id', 'branch name',
            'phone', 'product code', 'product name', 'address',
            'ifsc code', 'name ', 'a/c ', 'account no',
        ]):
            return True
        if any(frag in lo for frag in _ADDRESS_FRAGMENTS):
            return True
        return False

    def _parse_txn_line(self, line: str) -> dict:
        date_match = re.match(r'^(\d{1,2}-\d{2}-\d{4})\s+(.*)', line)
        if not date_match:
            return None

        date_str  = date_match.group(1)
        remainder = date_match.group(2).strip()
        nums      = re.findall(r'-?[\d,]+\.\d{2}', remainder)

        if len(nums) < 2:
            return None

        amt_raw = nums[-2]
        bal_raw = nums[-1]
        amt     = parse_amt(amt_raw)
        balance = parse_amt(bal_raw)

        if amt == 0:
            return None

        raw_signed = amt_raw.replace(',', '').strip()
        try:
            signed_val = float(raw_signed)
        except ValueError:
            signed_val = amt

        typ = 'CR' if signed_val < 0 else 'DR'
        amt = abs(signed_val)

        date_parsed = normalize_date(date_str)
        if not date_parsed:
            return None

        return {
            'date':      date_parsed,
            'desc':      '',
            'amount':    round(amt, 2),
            'balance':   round(balance, 2),
            'type':      typ,
            'reference': '',
        }

    @staticmethod
    def _clean_desc(desc: str) -> str:
        desc = _RE_VALUE_DATE.sub('', desc)
        desc = _RE_CHQ_SUFFIX.sub('', desc)
        desc = re.sub(r'\b\d{2}:\d{2}:\d{2}\b', '', desc)
        desc = re.sub(r'\b[A-Fa-f0-9]{16,}\b', '', desc)
        desc = _RE_WHITESPACE.sub(' ', desc).strip()
        return desc
    