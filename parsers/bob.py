"""
parsers/bob.py
───────────────
Bank of Baroda (BOB) Statement Parser.

Actual PDF format (bob World app): 5 columns
  Col 0: DESC\nSerial Date Date\ntrailing  (merged cell)
  Col 1: empty / cheque number
  Col 2: Debit   ('-' if not debit)
  Col 3: Credit  ('-' if not credit)
  Col 4: Balance

Date format: DD-MM-YYYY
Opening Balance: Row with description "Opening Balance"

v1.1 fix: Handles malformed dates like '02-092022' (missing hyphen)
          that appear as PDF extraction artifacts in bob World statements.
"""

import re
import pdfplumber
from parsers.base    import BaseParser
from core.normalizer import normalize
from core.utils      import parse_amt


# Primary: DD-MM-YYYY
# Fallback: DD-MMYYYY (missing second hyphen, e.g. 02-092022)
_DATE_RE = re.compile(r'\b(\d{2}-\d{2}-\d{4}|\d{2}-\d{2}\d{4})\b')

_SKIP_RE = re.compile(
    r'(no\s+date\s+date|transaction\s+date|value\s+date|cheque\s+number|'
    r'serial\s+no|computer.generated|do\'s\s+and\s+don|scan\s+qr|'
    r'bob\s+world\s+mobile|download\s+app|registration|page\s+\d+\s+of)',
    re.IGNORECASE
)
# Trailing PDF artifacts to strip from descriptions
_TRAILING_JUNK = re.compile(r'\s+[a-zA-Z0-9]{1,2}$')


def _normalize_date(raw: str) -> str:
    """
    Normalize any matched date string to DD-MM-YYYY.
    Handles:
      '02-09-2022' → '02-09-2022'  (already correct)
      '02-092022'  → '02-09-2022'  (missing hyphen fix)
    """
    # Already correct format
    if re.match(r'^\d{2}-\d{2}-\d{4}$', raw):
        return raw
    # Missing second hyphen: DD-MMYYYY → DD-MM-YYYY
    m = re.match(r'^(\d{2})-(\d{2})(\d{4})$', raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return raw


class BOBParser(BaseParser):

    _DETECT_KEYWORDS = [
        'bank of baroda', 'bob world', 'barb0', 'bob.in',
        'बैंक ऑफ़ बड़ौदा', 'बैंक ऑफ बड़ौदा'
    ]

    def detect(self, pdf_path: str) -> bool:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = (pdf.pages[0].extract_text() or '').lower()

                has_ifsc = 'barb' in text

                has_columns = all(h in text for h in [
                    'debit', 'credit', 'balance', 'description'
                ])

                return has_ifsc and has_columns
        except Exception:
            return False

    # ─────────────────────────────────────────────────────
    def parse(self, pdf_path: str) -> list:
        transactions    = []
        opening_balance = None

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    if not tables:
                        continue
                    for table in tables:
                        for row in table:
                            result = self._parse_row(row)
                            if result is None:
                                continue
                            if result == 'opening':
                                opening_balance = self._extract_balance_from_row(row)
                                if opening_balance:
                                    self._log(f"Opening balance: Rs.{opening_balance:,.2f}")
                                continue
                            transactions.append(result)

        except Exception as e:
            import traceback
            self._log(f"Fatal: {e}")
            traceback.print_exc()
            return []

        self._log(f"Raw transactions parsed: {len(transactions)}")
        if not transactions:
            return []

        if opening_balance is not None:
            transactions[0]['opening_balance'] = opening_balance

        result = normalize(transactions, opening_balance=opening_balance)
        self._log(f"Final transactions: {len(result)}")
        return result

    # ─────────────────────────────────────────────────────
    def _parse_row(self, row):
        """
        BOB actual 5-column layout:
          0 → merged: description + serial + dates + trailing chars
          1 → empty / cheque
          2 → Debit  ('-' if credit)
          3 → Credit ('-' if debit)
          4 → Balance
        """
        if not row or len(row) < 3:
            return None

        col0 = str(row[0] or '').strip()

        if _SKIP_RE.search(col0) or not col0:
            return None

        if 'opening balance' in col0.lower():
            return 'opening'

        # Must contain a valid date (including malformed ones)
        dates = _DATE_RE.findall(col0)
        if not dates:
            return None

        date_str   = _normalize_date(dates[0])   # fix malformed dates
        desc       = self._extract_desc(col0)

        debit_raw  = str(row[2] or '').strip() if len(row) > 2 else ''
        credit_raw = str(row[3] or '').strip() if len(row) > 3 else ''
        bal_raw    = str(row[4] or '').strip() if len(row) > 4 else ''

        debit  = parse_amt(debit_raw)
        credit = parse_amt(credit_raw)
        bal    = parse_amt(bal_raw)

        if debit > 0:
            amount, txn_type = debit, 'DR'
        elif credit > 0:
            amount, txn_type = credit, 'CR'
        else:
            return None

        return {
            'date':    date_str,
            'desc':    desc,
            'amount':  round(amount, 2),
            'type':    txn_type,
            'balance': round(bal, 2) if bal else None,
        }

    # ─────────────────────────────────────────────────────
    @staticmethod
    def _extract_desc(col0: str) -> str:
        """
        col0 examples:
          "UPI/201921164575/.../bharatpe.905000575\n2 01-06-2022 01-06-2022\n2"
          "12 07-06-2022 07-06-2022 MBK/200740880173/18:34:43/surbhai"
          "IMPS/P2A/.../ok\n11 07-06-2022 07-06-2022"
          "i UPI/208183687875/.../euronetgpay.pay@ic\n243 02-12-2022 02-12-2022"
          "UPI/208184645072/...\n244 02-092022 02-092022"  ← malformed date
        """
        lines  = col0.split('\n')
        parts  = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if _DATE_RE.search(line):
                # Strip serial number and both date occurrences
                clean = _DATE_RE.sub('', line)
                clean = re.sub(r'^\d+\s*', '', clean)      # leading serial
                clean = re.sub(r'\s+', ' ', clean).strip()
                # Only keep if meaningful content remains (not single char)
                if clean and len(clean) > 2 and not re.match(r'^[a-zA-Z0-9]{1,2}$', clean):
                    parts.append(clean)
            else:
                # Pure description line — strip leading single-char PDF artifacts
                clean = re.sub(r'^[a-zA-Z]\s+', '', line)  # leading 'i ', 'f ' etc.
                clean = _TRAILING_JUNK.sub('', clean)       # trailing 'a', '2' etc.
                clean = re.sub(r'\s+', ' ', clean).strip()
                if clean:
                    parts.append(clean)

        desc = ' '.join(parts)
        desc = re.sub(r'\s+', ' ', desc).strip()
        return desc[:200]

    @staticmethod
    def _extract_balance_from_row(row) -> float:
        """Get balance from Opening Balance row — last non-empty cell."""
        for c in reversed(row):
            if c and str(c).strip() not in ('', '-'):
                val = parse_amt(str(c))
                if val > 0:
                    return round(val, 2)
        return None
    