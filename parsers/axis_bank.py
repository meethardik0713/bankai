"""
parsers/axis_bank.py
─────────────────────────────────────────────────────────────
Axis Bank Statement Parser
Handles Axis Bank PDF statements (Current + Savings accounts)

Statement format observed:
  Tran Date | Value Date | Transaction Particulars | Chq No | Amount(INR) | DR/CR | Balance(INR) | Branch Name

Column structure (space-separated text line):
  DD-MM-YYYY  DD-MM-YYYY  <description>  <amount>  CR/DR  <balance>  <branch>

Opening balance: "OPENING BALANCE 6811.96"
Closing balance: "CLOSING BALANCE 8618.42"
Transaction total: "TRANSACTION TOTAL DR/CR 657486.54/659293.00"

Special cases handled:
  - Multi-line branch name bleed (e.g. "ATM RECONCILATION / CENTRE")
  - UPIP2PPAY/DECLINE refund transactions
  - GST/Consolidated bank charge lines (no UPI prefix)
  - MOB/TPFT mobile transfer lines
  - IMPS transactions with reference codes
  - Page continuation (no re-headers needed)
"""

import re
import pdfplumber
from parsers.base import BaseParser

# ── Constants ──────────────────────────────────────────────────────────────────

DATE_RE    = re.compile(r'^\d{2}-\d{2}-\d{4}$')
AMOUNT_RE  = re.compile(r'^\d{1,15}(?:\.\d{1,2})?$')

# A valid transaction line starts with two dates
TXN_LINE_RE = re.compile(
    r'^(\d{2}-\d{2}-\d{4})\s+'          # Tran Date
    r'(\d{2}-\d{2}-\d{4})\s+'           # Value Date
    r'(.+?)\s+'                          # Description (greedy, trimmed later)
    r'(\d{1,15}(?:\.\d{1,2})?)\s+'      # Amount
    r'(CR|DR)\s+'                        # Type
    r'(\d{1,15}(?:\.\d{1,2})?)'         # Balance
    r'(?:\s+.*)?$'                       # Branch (optional, discard)
)

OPENING_BAL_RE = re.compile(r'OPENING\s+BALANCE\s+([\d,]+(?:\.\d{1,2})?)', re.IGNORECASE)
CLOSING_BAL_RE = re.compile(r'CLOSING\s+BALANCE\s+([\d,]+(?:\.\d{1,2})?)', re.IGNORECASE)

# Lines to skip — not real transactions
SKIP_PATTERNS = [
    re.compile(r'^TRANSACTION\s+TOTAL', re.IGNORECASE),
    re.compile(r'^CLOSING\s+BALANCE', re.IGNORECASE),
    re.compile(r'^OPENING\s+BALANCE', re.IGNORECASE),
    re.compile(r'^Charge\s+Statement', re.IGNORECASE),
    re.compile(r'^Sr\.\s+No\.', re.IGNORECASE),
    re.compile(r'^Tran\s+Date', re.IGNORECASE),
    re.compile(r'^Statement\s+of\s+Axis', re.IGNORECASE),
    re.compile(r'^\+\+\+\+\s+End\s+of\s+Statement', re.IGNORECASE),
    re.compile(r'^Legends\s*:', re.IGNORECASE),
    re.compile(r'^REGISTERED\s+OFFICE', re.IGNORECASE),
    re.compile(r'^BRANCH\s+ADDRESS', re.IGNORECASE),
    re.compile(r'^Unless\s+the\s+constituent', re.IGNORECASE),
    re.compile(r'^The\s+closing\s+balance', re.IGNORECASE),
    re.compile(r'^We\s+would\s+like', re.IGNORECASE),
    re.compile(r'^With\s+effect', re.IGNORECASE),
    re.compile(r'^Deposit\s+Insurance', re.IGNORECASE),
    re.compile(r'^In\s+compliance', re.IGNORECASE),
    re.compile(r'^ATM\s+RECONCILATION$', re.IGNORECASE),
    re.compile(r'^Monthly\s+Service$', re.IGNORECASE),
    re.compile(r'^Monthly$', re.IGNORECASE),
    re.compile(r'^Avg\.Balance$', re.IGNORECASE),
    re.compile(r'^\d{1,2}\s+\d{2}-\d{4}\s+\d{4}-\d{2}-\d{2}'),  # charge table rows
]


# ── Helper ─────────────────────────────────────────────────────────────────────

def _parse_amount(val: str) -> float:
    try:
        return round(float(val.replace(',', '')), 2)
    except Exception:
        return 0.0


def _should_skip(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    for pat in SKIP_PATTERNS:
        if pat.search(stripped):
            return True
    return False


def _normalise_date(date_str: str) -> str:
    """Convert DD-MM-YYYY → DD/MM/YYYY for consistent output schema."""
    return date_str.replace('-', '/')


# ── Parser class ───────────────────────────────────────────────────────────────

class AxisBankParser(BaseParser):
    """
    Parser for Axis Bank PDF statements.
    Inherits from BaseParser for consistent pipeline integration.
    """

    BANK_NAME = "Axis Bank"

    _IFSC_PREFIX = 'utib'
    _COLUMN_SIGNALS = ['transaction particulars', 'dr/cr']

    def detect(self, pdf_path: str) -> bool:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = (pdf.pages[0].extract_text() or '').lower()
                has_ifsc = self._IFSC_PREFIX in text
                has_columns = all(c in text for c in self._COLUMN_SIGNALS)
                return has_ifsc and has_columns
        except Exception:
            return False

    def parse(self, pdf_path: str) -> list:
        transactions  = []
        opening_bal   = None
        closing_bal   = None
        raw_lines     = []

        # ── Extract all text lines across pages ───────────────────────────────
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                for line in text.split('\n'):
                    raw_lines.append(line.strip())

        # ── Pass 1: grab opening/closing balance ──────────────────────────────
        for line in raw_lines:
            if opening_bal is None:
                m = OPENING_BAL_RE.search(line)
                if m:
                    opening_bal = _parse_amount(m.group(1))

            if closing_bal is None:
                m = CLOSING_BAL_RE.search(line)
                if m:
                    closing_bal = _parse_amount(m.group(1))

        print(f"[axis_bank] Opening: {opening_bal}  Closing: {closing_bal}")

        # ── Pass 2: parse transactions ────────────────────────────────────────
        for line in raw_lines:
            if _should_skip(line):
                continue

            m = TXN_LINE_RE.match(line)
            if not m:
                continue

            tran_date  = _normalise_date(m.group(1))
            # value_date = m.group(2)   — available if needed
            desc       = m.group(3).strip()
            amount     = _parse_amount(m.group(4))
            txn_type   = m.group(5).upper()   # 'CR' or 'DR'
            balance    = _parse_amount(m.group(6))

            # Skip zero-amount lines (rare edge case)
            if amount == 0.0:
                print(f"[axis_bank] Skipping zero-amount line: {line[:80]}")
                continue

            transactions.append({
                'date':            tran_date,
                'desc':            desc,
                'amount':          amount,
                'type':            txn_type,
                'balance':         balance,
                'category':        '',
                'bank':            self.BANK_NAME,
                'opening_balance': opening_bal if len(transactions) == 0 else None,
            })

        # ── Attach opening balance to first txn only ──────────────────────────
        if transactions and opening_bal is not None:
            transactions[0]['opening_balance'] = opening_bal

        # ── Attach closing balance as metadata on last txn ────────────────────
        if transactions and closing_bal is not None:
            transactions[-1]['closing_balance'] = closing_bal

        print(f"[axis_bank] Parsed {len(transactions)} transactions")
        return transactions
    