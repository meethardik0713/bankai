"""
parsers/icici_bank.py
──────────────────────────────────────────────────────────────────────────────
ICICI Bank Statement Parser — Production Grade (Text-Based)

Real PDF analysis shows pdfplumber extract_tables() fails for ICICI.
All data comes from extract_text() as plain lines.

Actual format observed:
  Line with date:     "02-04-2019 DEBIT CARD VPS/ACT /201904021337/... 4,377.00 61,354.31"
  Continuation line:  "/201904021337/909208028740/HYDERABAD"
  B/F line:           "01-04-2019 B/F 65,731.31"
  CR/DR logic:        Balance goes UP = CR, balance goes DOWN = DR
──────────────────────────────────────────────────────────────────────────────
"""

import re
import pdfplumber
from parsers.base    import BaseParser
from core.normalizer import normalize
from core.utils      import parse_amt


# ── Patterns ──────────────────────────────────────────────────────────────
_DATE_RE   = re.compile(r'^\d{2}-\d{2}-\d{4}$')
_AMOUNT_RE = re.compile(r'^[\d,]+\.\d{2}$')
_TOTAL_RE  = re.compile(r'^TOTAL\b', re.IGNORECASE)
_BF_RE     = re.compile(r'\bB/?F\b', re.IGNORECASE)
_INT_RE    = re.compile(r'Int\.Pd:', re.IGNORECASE)

# Lines to skip entirely
_SKIP_RE = re.compile(
    r'(Page \d+ of \d+|Visit www\.|Dial your Bank|Did you know|'
    r'KYC compliant|Relationship Manager|Summary of Accounts|'
    r'ACCOUNT DETAILS|ACCOUNT TYPE|A/c BALANCE|FIXED DEPOSITS|'
    r'TOTAL BALANCE|NOMINATION|Savings A/c|Statement of Trans|'
    r'DATE\s+MODE|PARTICULARS\s+DEPOSITS|Legends for|VAT/MAT|'
    r'RTGS|Mode is available|Income tax|REGD ADDRESS|'
    r'Authorised Signatory|authenticated|customers are|'
    r'For ICICI Bank|khayaal aapka|^\s*$|'
    r'MHW\d|Your Base Branch|PANJAGUTTA|TELANGANA)',
    re.IGNORECASE
)


class ICICIBankParser(BaseParser):

    def detect(self, pdf_path: str) -> bool:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = (pdf.pages[0].extract_text() or '').upper()
                has_icici = (
                    'ICICIBANK.COM' in text or
                    'KHAYAAL AAPKA' in text
                )
                has_columns = 'DEPOSITS' in text and 'WITHDRAWALS' in text
                return has_icici and has_columns
        except Exception:
            return False

    def parse(self, pdf_path: str) -> list:
        lines = self._extract_lines(pdf_path)
        transactions = self._build_transactions(lines)
        # Lock types before normalize — ICICI CR/DR is already correct
        # _resolve_opening_balance in normalizer would override first txn type
        for t in transactions:
            t['_type_locked'] = True
        return normalize(transactions)

    # ── Step 1: Extract all text lines from all pages ──────────────────────
    def _extract_lines(self, pdf_path: str) -> list:
        lines = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ''
                for line in text.split('\n'):
                    line = line.strip()
                    if line:
                        lines.append(line)
        return lines

    # ── Step 2: Build transactions from lines ─────────────────────────────
    def _build_transactions(self, lines: list) -> list:
        transactions = []
        opening_balance = None
        pending = None

        for line in lines:

            # Skip noise
            if _SKIP_RE.search(line):
                if pending:
                    transactions.append(pending)
                    pending = None
                continue

            # Skip TOTAL row
            if _TOTAL_RE.match(line):
                if pending:
                    transactions.append(pending)
                    pending = None
                continue

            # Check if line starts with a date
            parts = line.split()
            if not parts:
                continue

            is_date_line = _DATE_RE.match(parts[0])

            if not is_date_line:
                # Continuation line — append to pending desc
                if pending:
                    pending['desc'] = (pending['desc'] + ' ' + line).strip()
                continue

            # ── This line starts with a date ──────────────────────────────
            date_str = parts[0]
            rest = parts[1:]  # everything after the date

            # ── B/F (opening balance) ──────────────────────────────────────
            if any(_BF_RE.match(t) for t in rest):
                bal = _last_amount(rest)
                if bal is not None:
                    opening_balance = bal
                    if pending:
                        transactions.append(pending)
                        pending = None
                continue

            # ── Normal transaction line ────────────────────────────────────
            # Save previous pending
            if pending:
                transactions.append(pending)
                pending = None

            # Find amount positions from right
            amount_positions = [i for i, t in enumerate(rest) if _AMOUNT_RE.match(t)]

            if len(amount_positions) < 2:
                if len(amount_positions) == 1:
                    balance = parse_amt(rest[amount_positions[-1]])
                    desc_tokens = rest[:amount_positions[-1]]
                    desc = _clean_desc(desc_tokens)
                    pending = {
                        'date':            date_str,
                        'desc':            desc,
                        'amount':          None,
                        'balance':         balance,
                        'type':            None,
                        'opening_balance': None,
                        'category':        '',
                        'bank':            'ICICI Bank',
                    }
                continue

            # Last amount = balance, second-last = transaction amount
            bal_i = amount_positions[-1]
            amt_i = amount_positions[-2]

            balance    = parse_amt(rest[bal_i])
            txn_amount = parse_amt(rest[amt_i])
            desc_tokens = rest[:amt_i]
            desc = _clean_desc(desc_tokens)

            # ── CR/DR via balance diff ─────────────────────────────────────
            if transactions:
                prev_bal = transactions[-1]['balance']
            elif opening_balance is not None:
                prev_bal = opening_balance
            else:
                prev_bal = 0.0
            diff = round(balance - prev_bal, 2)
            txn_type = 'CR' if diff >= 0 else 'DR'

            # Interest rows always CR
            if _INT_RE.search(desc):
                txn_type = 'CR'

            pending = {
                'date':            date_str,
                'desc':            desc,
                'amount':          txn_amount,
                'balance':         balance,
                'type':            txn_type,
                'opening_balance': None,
                'category':        '',
                'bank':            'ICICI Bank',
            }

        # Flush last
        if pending:
            transactions.append(pending)

        # Stamp opening balance on first transaction
        if opening_balance is not None and transactions:
            transactions[0]['opening_balance'] = opening_balance

        self._log(
            f"Extracted {len(transactions)} transactions | "
            f"Opening bal: ₹{opening_balance:,.2f}"
            if opening_balance
            else f"Extracted {len(transactions)} transactions"
        )

        return transactions


# ── Helpers ────────────────────────────────────────────────────────────────

_MODE_WORDS = {
    'DEBIT CARD', 'MOBILE BANKING', 'NET BANKING',
    'ICICI ATM', 'OTHER ATMS', 'CASH DEPOSIT',
    'VISA REF', 'APBS',
}


def _clean_desc(tokens: list) -> str:
    """Join tokens into description, strip leading MODE keywords."""
    if not tokens:
        return ''
    result = []
    for t in tokens:
        if not t:
            continue
        upper = t.upper()
        if upper in _MODE_WORDS:
            continue
        if upper.startswith('CHEQUE') and len(t) < 15:
            continue
        result.append(t)
    return ' '.join(result).strip()


def _last_amount(tokens: list):
    """Return float of last amount-like token in list."""
    for t in reversed(tokens):
        if _AMOUNT_RE.match(t):
            return parse_amt(t)
    return None
