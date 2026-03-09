"""
parsers/sbi.py
───────────────
State Bank of India Statement Parser v6.1
Handles TWO distinct SBI statement formats:

  FORMAT A — Branch-printed (legacy)
    • Dates: dd/mm/yy (e.g. 29/12/23)
    • Layout: raw text blocks, no clean table structure
    • Opening: "BROUGHT FORWARD : 74171.84"
    • Extraction: text-based line-by-line with block flushing

  FORMAT B — Online / YONO / Internet Banking
    • Dates: dd Mon yyyy (e.g. 10 Dec 2021)
    • Layout: proper table with Debit/Credit/Balance columns
    • Opening: "Balance as on dd Mon yyyy : X,XX,XXX.XX"
    • Extraction: pdfplumber table extraction

Auto-detects format from first page content.

v6.1 fix: Page boundary duplicate transactions fixed.
          _flush() now called ONCE after all pages processed,
          not once per page (which caused splits to flush twice).
"""

import re
import pdfplumber
from datetime import datetime, timedelta

from parsers.base    import BaseParser
from core.normalizer import normalize
from core.utils      import parse_amt

_RE_VALUE_DATE = re.compile(r'\(?\s*[Vv]alue\s+[Dd]ate\s*:\s*[\d\-/\.]+\s*\)?')
_RE_WHITESPACE = re.compile(r'\s+')

# ── Branch format constants ──────────────────────────────

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

_TXN_START_BRANCH = re.compile(
    r'^(\d{2}/\d{2}/\d{2,4})(?:\s+\d{2}/\d{2}/\d{2,4})?\s+(.*)',
    re.DOTALL
)
_AMOUNT_PAT  = re.compile(r'[\d,]+\.\d{2}')
_OPENING_BAL_BRANCH = re.compile(
    r'brought\s+forward\s*:?\s*([\d,]+\.\d{2})',
    re.IGNORECASE
)

# ── Online format constants ──────────────────────────────

_OPENING_BAL_ONLINE = re.compile(
    r'[Bb]alance\s*(?:as\s*on|on)\s*\d{1,2}\s*\w+\s*\d{4}\s*:\s*([\d,]+\.\d{2})',
)
_MONTH_MAP = {
    'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
    'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
    'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
}


class SBIParser(BaseParser):

    _DETECT_KEYWORDS = [
        'state bank of india', 'onlinesbi', 'sbi yono',
        'sbin0', 'sbi bank', 'sbchq', 'sbnchq',
    ]

    def detect(self, pdf_path: str) -> bool:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = (pdf.pages[0].extract_text() or '').lower()

                has_ifsc = 'sbin' in text

                has_columns = (
                    ('post date' in text and 'value date' in text and 'debit' in text and 'credit' in text)
                    or
                    ('txn date' in text and 'debit' in text and 'credit' in text)
                )

                return has_ifsc and has_columns
        except Exception:
            return False

    def parse(self, pdf_path: str) -> list:
        """Auto-detect format and delegate to appropriate parser."""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                first_text = (pdf.pages[0].extract_text() or '')
                first_low  = first_text.lower()

                # Format B detection: online/YONO format has "Txn Date" or
                # "Account Statement from" with "dd Mon yyyy" style dates
                is_online = (
                    'txn date' in first_low
                    or bool(_OPENING_BAL_ONLINE.search(first_text))
                )
                # Format A detection: branch format has "BROUGHT FORWARD"
                # or "Post Date  Value Date  Details"
                is_branch = (
                    'brought forward' in first_low
                    or ('post date' in first_low and 'value date' in first_low and 'details' in first_low)
                )

                if is_online and not is_branch:
                    self._log("Detected: SBI Online/YONO format")
                    return self._parse_online(pdf)
                else:
                    self._log("Detected: SBI Branch format")
                    return self._parse_branch(pdf)

        except Exception as e:
            import traceback
            self._log(f"Fatal: {e}")
            traceback.print_exc()
            return []

    # ═════════════════════════════════════════════════════
    #  FORMAT B: Online / YONO / Internet Banking
    # ═════════════════════════════════════════════════════

    def _parse_online(self, pdf) -> list:
        """Parse SBI online/YONO statements using table extraction."""
        raw_txns        = []
        pdf_opening_bal = None

        # Find opening balance
        for pg in pdf.pages[:2]:
            text = pg.extract_text() or ''
            m = _OPENING_BAL_ONLINE.search(text)
            if m:
                pdf_opening_bal = self._to_float(m.group(1))
                self._log(f"Opening balance: ₹{pdf_opening_bal:,.2f}")
                break

        # Extract transactions from tables
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row or len(row) < 7:
                        continue

                    txn_date_raw = (row[0] or '').strip()
                    desc_raw     = (row[2] or '').strip()
                    debit_raw    = (row[4] or '').strip()
                    credit_raw   = (row[5] or '').strip()
                    balance_raw  = (row[6] or '').strip()

                    # Skip header rows
                    if not txn_date_raw:
                        continue
                    low_date = txn_date_raw.lower()
                    if 'txn' in low_date or 'date' in low_date:
                        continue

                    # Parse date (handles "10Dec\n2021" format from table extraction)
                    date_str = self._parse_online_date(txn_date_raw)
                    if not date_str:
                        continue

                    # Parse balance
                    balance = self._to_float(balance_raw)
                    if balance == 0.0:
                        continue

                    # Parse debit/credit amounts
                    debit_amt  = self._to_float(debit_raw) if debit_raw else 0.0
                    credit_amt = self._to_float(credit_raw) if credit_raw else 0.0

                    if debit_amt > 0:
                        amount = debit_amt
                        txn_type = 'DR'
                    elif credit_amt > 0:
                        amount = credit_amt
                        txn_type = 'CR'
                    else:
                        amount = 0.0
                        txn_type = 'DR'

                    # Clean description (remove newlines from table extraction)
                    desc = re.sub(r'\s+', ' ', desc_raw).strip()
                    desc = re.sub(r'\b\d{9,}\b', '', desc).strip()
                    desc = re.sub(r'\s+', ' ', desc).strip()
                    desc = desc[:200]

                    raw_txns.append({
                        'date':    date_str,
                        'desc':    desc,
                        'amount':  round(amount, 2),
                        'balance': round(balance, 2),
                        'type':    txn_type,
                    })

        self._log(f"Raw transactions parsed: {len(raw_txns)}")

        if not raw_txns:
            return []

        # Verify CR/DR with balance chain
        prev_bal = pdf_opening_bal if pdf_opening_bal is not None else 0.0
        for txn in raw_txns:
            curr_bal = txn['balance']
            diff     = round(curr_bal - prev_bal, 2)
            amt      = txn['amount']

            # If column-based type conflicts with balance movement, trust balance
            if txn['type'] == 'CR' and diff < 0:
                txn['type'] = 'DR'
            elif txn['type'] == 'DR' and diff > 0:
                txn['type'] = 'CR'

            # If amount doesn't match balance movement, fix it
            if amt > 0:
                expected_diff = amt if txn['type'] == 'CR' else -amt
                if abs(diff - expected_diff) > 1.0:
                    txn['amount'] = round(abs(diff), 2)

            prev_bal = curr_bal

        result = normalize(raw_txns, opening_balance=pdf_opening_bal)

        if result and pdf_opening_bal is not None:
            result[0]['opening_balance'] = round(pdf_opening_bal, 2)

        self._log(f"Final transactions: {len(result)}")
        return result

    @staticmethod
    def _parse_online_date(raw: str) -> str:
        """
        Parse dates like '10Dec\\n2021' or '10 Dec 2021' → 'dd/mm/yy' format.
        Returns None if not a valid date.
        """
        cleaned = re.sub(r'\s+', ' ', raw.replace('\n', ' ')).strip()

        m = re.match(r'(\d{1,2})\s*([A-Za-z]{3})\s*(\d{4})', cleaned)
        if m:
            day   = m.group(1).zfill(2)
            month = _MONTH_MAP.get(m.group(2).lower())
            year  = m.group(3)[2:]
            if month:
                return f"{day}/{month}/{year}"

        return None

    # ═════════════════════════════════════════════════════
    #  FORMAT A: Branch-printed (legacy)
    # ═════════════════════════════════════════════════════

    def _parse_branch(self, pdf) -> list:
        """Parse SBI branch-printed statements using text extraction."""
        raw_txns        = []
        pdf_opening_bal = None

        # Pass 1: opening balance
        for pg in pdf.pages[:2]:
            text = pg.extract_text() or ''
            m    = _OPENING_BAL_BRANCH.search(text)
            if m:
                pdf_opening_bal = self._to_float(m.group(1))
                self._log(f"Opening balance: ₹{pdf_opening_bal:,.2f}")
                break

        # Pass 2: block-based extraction
        # FIX v6.1: flush called ONCE after ALL pages, not once per page.
        # Flushing inside the page loop caused page-boundary-split transactions
        # to be added twice (once incomplete at page end, once complete at page start).
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

                anchor = _TXN_START_BRANCH.match(line)
                if anchor:
                    self._flush(current_date, current_lines, raw_txns)
                    current_date  = anchor.group(1)
                    rest          = anchor.group(2).strip()
                    current_lines = [rest] if rest else []
                else:
                    if current_date is not None:
                        current_lines.append(line)

        # Flush the final transaction ONCE after all pages are done
        self._flush(current_date, current_lines, raw_txns)

        self._log(f"Raw transactions parsed: {len(raw_txns)}")

        if not raw_txns:
            return []

        # v5+: NO date sorting — SBI prints in balance-continuity order

        # CR/DR resolution
        prev_bal = pdf_opening_bal if pdf_opening_bal is not None else 0.0
        for txn in raw_txns:
            curr_bal   = txn['balance']
            amt        = txn['amount']
            diff       = round(curr_bal - prev_bal, 2)
            desc_upper = txn.get('desc', '').upper()

            forced_type = self._forced_type_from_desc(desc_upper)

            if forced_type:
                txn['type'] = forced_type
                if amt == 0.0 and diff != 0.0:
                    txn['amount'] = round(abs(diff), 2)
            else:
                tol = max(1.0, round(abs(amt) * 0.015, 2))
                if abs(diff - amt) <= tol:
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

    # ── Shared Helpers ────────────────────────────────────

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
        """
        Extract (amount, balance) from transaction lines.
        Filters out CIF numbers and long digit sequences.
        """
        for line in reversed(lines):
            cleaned = re.sub(r'CIF\s*:?\s*[\d\-]+', '', line, flags=re.IGNORECASE)
            cleaned = re.sub(r'\b\d{9,}\b', '', cleaned)

            nums = _AMOUNT_PAT.findall(cleaned)
            if len(nums) >= 2:
                amt = self._to_float(nums[-2])
                bal = self._to_float(nums[-1])
                if 0.01 <= amt <= 9_999_999.99 and 0.01 <= bal <= 99_999_999.99:
                    return amt, bal
            elif len(nums) == 1:
                balance = self._to_float(nums[0])
                if not (0.01 <= balance <= 99_999_999.99):
                    continue
                for prev in reversed(lines[:-1]):
                    prev_cleaned = re.sub(r'CIF\s*:?\s*[\d\-]+', '', prev, flags=re.IGNORECASE)
                    prev_cleaned = re.sub(r'\b\d{9,}\b', '', prev_cleaned)
                    pnums = _AMOUNT_PAT.findall(prev_cleaned)
                    if pnums:
                        amt = self._to_float(pnums[-1])
                        if 0.01 <= amt <= 9_999_999.99:
                            return amt, balance
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
    