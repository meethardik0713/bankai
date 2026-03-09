"""
parsers/pnb.py
───────────────
Punjab National Bank (PNB) Statement Parser.

Format: Table-based PDF (mPassBook / net banking)
Columns: Transaction Date | Cheque Number | Withdrawal | Deposit | Balance | Narration
Balance format: "570.53 Cr." (always Cr. suffix in this statement type)
Date format: DD/MM/YYYY
Narration: sometimes wraps to 2 lines — handled via row merging
"""

import re
import pdfplumber

from parsers.base    import BaseParser
from core.normalizer import normalize, normalize_date
from core.utils      import parse_amt

# ── Patterns ──────────────────────────────────────────────
_RE_DATE        = re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$')
_RE_BALANCE     = re.compile(r'([\d,]+\.\d{2})\s*(?:Cr\.?|Dr\.?)?', re.IGNORECASE)
_RE_AMOUNT      = re.compile(r'^[\d,]+\.\d{2}$')
_RE_WHITESPACE  = re.compile(r'\s+')

_SKIP_NARRATIONS = [
    'opening balance', 'closing balance', 'brought forward',
    'carried forward', 'page total', 'grand total',
]

_HEADER_FRAGMENTS = [
    'transaction date', 'cheque number', 'withdrawal',
    'deposit', 'balance', 'narration',
]


class PNBParser(BaseParser):

    _DETECT_KEYWORDS = [
        'punjab national bank',
        'pnb',
        'punb0',          # IFSC prefix
        'mpassbook',
        'punjab national',
    ]

    def detect(self, pdf_path: str) -> bool:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = (pdf.pages[0].extract_text() or '').lower()
                has_ifsc = 'punb' in text
                has_columns = all(h in text for h in [
                    'withdrawal', 'deposit', 'narration', 'cheque'
                ])
                return has_ifsc and has_columns
        except Exception as e:    
            return False

    def parse(self, pdf_path: str) -> list:
        raw_txns = []

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    # ── Try table extraction first ─────────
                    tables = page.extract_tables()
                    if tables:
                        for table in tables:
                            txns = self._parse_table(table)
                            raw_txns.extend(txns)
                    else:
                        # ── Fallback: raw text parsing ─────
                        text = page.extract_text() or ''
                        txns = self._parse_text(text)
                        raw_txns.extend(txns)

        except Exception as e:
            import traceback
            self._log(f"Fatal: {e}")
            traceback.print_exc()

        self._log(f"Raw transactions extracted: {len(raw_txns)}")

        # Infer opening balance from first transaction
        opening_bal = self._infer_opening_balance(raw_txns)

        result = normalize(raw_txns, opening_balance=opening_bal)
        self._log(f"After normalize: {len(result)} transactions")
        return result

    # ══════════════════════════════════════════════════════
    #  TABLE PARSING (primary method)
    # ══════════════════════════════════════════════════════

    def _parse_table(self, table: list) -> list:
        """
        Parse a pdfplumber table.
        Expected columns: Date | Cheque No | Withdrawal | Deposit | Balance | Narration
        Handles:
          - Header rows (skip)
          - Continuation rows (narration overflow — merge with previous)
          - None cells
        """
        txns        = []
        pending     = None   # last successfully parsed txn, may need narration appended

        for row in table:
            if not row:
                continue

            # Normalize cells — strip None and whitespace
            cells = [self._clean_cell(c) for c in row]

            # Skip header rows
            if self._is_header_row(cells):
                continue

            # Skip completely empty rows
            if all(c == '' for c in cells):
                continue

            # Check if this is a continuation row (narration overflow)
            # Continuation rows have: empty date, empty amounts, only narration text
            if self._is_continuation_row(cells):
                if pending is not None:
                    extra_narration = self._extract_narration_from_row(cells)
                    if extra_narration:
                        pending['desc'] = (pending['desc'] + ' ' + extra_narration).strip()
                continue

            # Try to parse as a full transaction row
            txn = self._parse_row(cells)
            if txn:
                if pending is not None:
                    txns.append(pending)
                pending = txn
            else:
                # Partial row — try to extract narration and append to pending
                if pending is not None:
                    extra = self._extract_narration_from_row(cells)
                    if extra:
                        pending['desc'] = (pending['desc'] + ' ' + extra).strip()

        # Don't forget last pending
        if pending is not None:
            txns.append(pending)

        return txns

    def _parse_row(self, cells: list) -> dict:
        """
        Parse a single table row into a transaction dict.
        Returns None if row is not a valid transaction.

        Column mapping (0-indexed):
          0 = Transaction Date
          1 = Cheque Number
          2 = Withdrawal
          3 = Deposit
          4 = Balance
          5 = Narration
        """
        if len(cells) < 5:
            return None

        # Column 0: Date
        date_str = cells[0].strip()
        if not _RE_DATE.match(date_str):
            return None

        date_parsed = normalize_date(date_str)
        if not date_parsed:
            return None

        # Column 2: Withdrawal (DR)
        # Column 3: Deposit (CR)
        withdrawal_str = cells[2] if len(cells) > 2 else ''
        deposit_str    = cells[3] if len(cells) > 3 else ''
        balance_str    = cells[4] if len(cells) > 4 else ''
        narration      = cells[5] if len(cells) > 5 else ''

        withdrawal = parse_amt(withdrawal_str)
        deposit    = parse_amt(deposit_str)
        balance    = self._parse_balance(balance_str)

        # Determine type and amount
        if withdrawal > 0 and deposit == 0:
            txn_type = 'DR'
            amount   = withdrawal
        elif deposit > 0 and withdrawal == 0:
            txn_type = 'CR'
            amount   = deposit
        elif deposit > 0 and withdrawal > 0:
            # Edge case: both filled — trust deposit
            txn_type = 'CR'
            amount   = deposit
        else:
            return None  # No amount — skip row

        if amount <= 0:
            return None

        # Skip footer/summary rows
        if any(p in narration.lower() for p in _SKIP_NARRATIONS):
            return None

        return {
            'date':      date_parsed,
            'desc':      self._clean_narration(narration),
            'amount':    round(amount, 2),
            'balance':   balance,
            'type':      txn_type,
            'reference': cells[1] if len(cells) > 1 else '',  # Cheque number
        }

    # ══════════════════════════════════════════════════════
    #  TEXT PARSING (fallback method)
    # ══════════════════════════════════════════════════════

    def _parse_text(self, text: str) -> list:
        """
        Fallback line-by-line parser for pages where table extraction fails.
        PNB text layout: Date  Withdrawal  Deposit  Balance Cr.  Narration
        """
        txns          = []
        lines         = [l.strip() for l in text.splitlines() if l.strip()]
        narration_buf = []
        current_txn   = None

        for line in lines:
            if self._is_header_line(line):
                continue

            txn = self._parse_text_line(line)
            if txn:
                if current_txn:
                    if narration_buf:
                        current_txn['desc'] = self._clean_narration(
                            ' '.join(narration_buf)
                        )
                    txns.append(current_txn)
                current_txn   = txn
                narration_buf = []
            else:
                # Check if this line is pure narration continuation
                if current_txn and not self._looks_like_amount(line):
                    narration_buf.append(line)

        # Flush last transaction
        if current_txn:
            if narration_buf:
                current_txn['desc'] = self._clean_narration(
                    ' '.join(narration_buf)
                )
            txns.append(current_txn)

        return txns

    def _parse_text_line(self, line: str) -> dict:
        """Parse a single text line into transaction dict."""
        # Must start with a date
        date_match = re.match(r'^(\d{1,2}/\d{1,2}/\d{4})\s+(.*)', line)
        if not date_match:
            return None

        date_str  = date_match.group(1)
        remainder = date_match.group(2).strip()

        date_parsed = normalize_date(date_str)
        if not date_parsed:
            return None

        # Find all amounts in the remainder
        # Pattern: optional withdrawal, optional deposit, balance (with Cr./Dr.)
        amounts = re.findall(r'[\d,]+\.\d{2}', remainder)
        if len(amounts) < 2:
            return None

        # Balance is second-to-last number, Cr/Dr suffix follows
        balance_str = amounts[-1]
        balance     = parse_amt(balance_str)

        # Figure out withdrawal vs deposit
        # Look for "Cr." or "Dr." suffix pattern near balance
        # In PNB text, layout is: [withdrawal] [deposit] [balance Cr.]
        if len(amounts) >= 3:
            # 3+ numbers: could be cheque_num area + withdrawal + balance
            # Try: amounts[-3] = withdrawal or deposit, amounts[-2] = other
            withdrawal = parse_amt(amounts[-3])
            deposit    = parse_amt(amounts[-2])
        elif len(amounts) == 2:
            # Only 2 numbers: one is amount, one is balance
            withdrawal = parse_amt(amounts[-2])
            deposit    = 0.0

        # Determine CR/DR from context (balance change will be fixed by normalizer)
        # For now, use column position heuristic
        if deposit > 0 and withdrawal == 0:
            txn_type = 'CR'
            amount   = deposit
        elif withdrawal > 0 and deposit == 0:
            txn_type = 'DR'
            amount   = withdrawal
        else:
            # Use balance direction — will be corrected by normalizer
            txn_type = 'CR'
            amount   = max(withdrawal, deposit)

        if amount <= 0:
            return None

        # Narration: everything after the numbers
        narration = re.sub(r'^[\d,\.\s/Cr\.Dr\.]+', '', remainder).strip()

        return {
            'date':      date_parsed,
            'desc':      self._clean_narration(narration),
            'amount':    round(amount, 2),
            'balance':   round(balance, 2),
            'type':      txn_type,
            'reference': '',
        }

    # ══════════════════════════════════════════════════════
    #  OPENING BALANCE
    # ══════════════════════════════════════════════════════

    def _infer_opening_balance(self, txns: list) -> float:
        """
        PNB statements don't always show opening balance explicitly.
        Infer it from first transaction: OB = Balance - Amount (if CR) or Balance + Amount (if DR)
        The normalizer will also attempt this, but we do it here for accuracy.
        """
        if not txns:
            return None

        first   = txns[0]
        balance = first.get('balance')
        amount  = first.get('amount', 0)
        typ     = first.get('type', '')

        if balance is None or amount == 0:
            return None

        if typ == 'CR':
            ob = round(balance - amount, 2)
        else:
            ob = round(balance + amount, 2)

        if ob >= 0:
            self._log(f"Inferred opening balance: ₹{ob:,.2f}")
            return ob

        return None

    # ══════════════════════════════════════════════════════
    #  HELPERS
    # ══════════════════════════════════════════════════════

    def _parse_balance(self, balance_str: str) -> float:
        """Parse '570.53 Cr.' or '570.53' into float."""
        if not balance_str:
            return None
        m = _RE_BALANCE.search(balance_str)
        if m:
            return round(parse_amt(m.group(1)), 2)
        return None

    def _clean_cell(self, cell) -> str:
        """Normalize a table cell to string."""
        if cell is None:
            return ''
        return _RE_WHITESPACE.sub(' ', str(cell)).strip()

    def _clean_narration(self, narration: str) -> str:
        """Clean up narration text."""
        if not narration:
            return ''
        # Collapse whitespace
        narration = _RE_WHITESPACE.sub(' ', narration).strip()
        # Remove trailing slashes and spaces
        narration = narration.rstrip('/ ')
        # Limit length
        return narration[:300]

    def _is_header_row(self, cells: list) -> bool:
        """Check if this row is a table header."""
        combined = ' '.join(cells).lower()
        return any(h in combined for h in _HEADER_FRAGMENTS)

    def _is_continuation_row(self, cells: list) -> bool:
        """
        A continuation row has no date, no amounts —
        only narration text spilling from previous row.
        """
        if not cells:
            return False
        # First cell (date) must be empty
        if cells[0] != '':
            return False
        # Amount cells must be empty
        amount_cells = cells[2:5] if len(cells) > 4 else cells[1:]
        if any(self._looks_like_amount(c) for c in amount_cells):
            return False
        # Must have some text content
        return any(c != '' for c in cells)

    def _extract_narration_from_row(self, cells: list) -> str:
        """Extract any narration text from a continuation row."""
        parts = [c for c in cells if c and not self._looks_like_amount(c)]
        return ' '.join(parts).strip()

    def _looks_like_amount(self, text: str) -> bool:
        return bool(_RE_AMOUNT.match(text.strip())) if text else False

    def _is_header_line(self, line: str) -> bool:
        lo = line.lower()
        return any(h in lo for h in _HEADER_FRAGMENTS + [
            'page ', 'branch name', 'branch address', 'customer name',
            'customer address', 'ifsc code', 'statement for',
            'account statement', 'generated through', 'unless constituent',
            'computer generated', 'cheque leaved', 'minimum average',
            'penal interest', 'abbreviations',
        ])
    