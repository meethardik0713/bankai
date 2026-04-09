"""
parsers/hdfc.py
────────────────
HDFC Bank Statement Parser.
Layout: collapsed table — multiple date/balance entries per row.
"""

import re
import pdfplumber

from parsers.base      import BaseParser
from core.normalizer   import normalize


class HDFCParser(BaseParser):

    _IFSC_PREFIX = 'hdfc0'
    _COLUMN_SIGNALS = ['narration', 'closing balance']
    _DATE_PAT = re.compile(r'^\d{2}/\d{2}/\d{2}$')

    def detect(self, pdf_path: str) -> bool:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = (pdf.pages[0].extract_text() or '').lower()
                has_ifsc = 'hdfc0' in text
                has_columns = 'narration' in text and (
                    'closing balance' in text or 'closingbalance' in text
                )
                return has_ifsc and has_columns
        except Exception as e:
            return False

    def parse(self, pdf_path: str) -> list:
        all_dates, all_balances, all_narrations = [], [], []

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    if not tables:
                        continue
                    for row in tables[0]:
                        if not row or not row[0]:
                            continue
                        if 'date' in str(row[0]).lower():
                            continue
                        if len(row) < 7:
                            continue

                        dates    = [d.strip() for d in str(row[0]).split('\n')
                                    if self._DATE_PAT.match(d.strip())]
                        balances = [b.strip() for b in str(row[6] or '').split('\n')
                                    if b.strip()]

                        if not dates or len(dates) != len(balances):
                            continue

                        narr_lines = [n.strip() for n in str(row[1] or '').split('\n')
                                      if n.strip()]
                        chunk = max(1, len(narr_lines) // len(dates))

                        for i, (d, b) in enumerate(zip(dates, balances)):
                            all_dates.append(d)
                            all_balances.append(self._clean_amt(b))
                            start = i * chunk
                            narr  = ' '.join(narr_lines[start:start + chunk])
                            all_narrations.append(narr)

        except Exception as e:
            import traceback
            self._log(f"Fatal: {e}")
            traceback.print_exc()
            return []

        # Opening balance: pehli transaction ke balance se amount ghata ke nikalo
        opening_bal = self._extract_opening_balance(pdf_path)
        self._log(f"Opening balance extracted: {opening_bal}")
        if opening_bal is None and all_balances:
            # Statement summary se: 498.82 type pattern
            opening_bal = None  # fallback neeche handle hoga
        
        transactions = []
        for i, (date, bal) in enumerate(zip(all_dates, all_balances)):
            if i == 0:
                if opening_bal is not None:
                    prev_bal = opening_bal
                else:
                    # Pehli balance se amount subtract/add karke nikalo
                    prev_bal = all_balances[0]  # worst case same balance
            else:
                prev_bal = all_balances[i - 1]
            diff     = round(bal - prev_bal, 2)
            txn_type = 'CR' if diff > 0 else 'DR'
            amount   = round(abs(diff), 2)
            if amount == 0:
                amount = bal  # fallback
            narr     = all_narrations[i] if i < len(all_narrations) else ''
            transactions.append({
                'date':            date,
                'desc':            narr,
                'amount':          amount,
                'type':            txn_type,
                'balance':         bal,
                'opening_balance': opening_bal if i == 0 else None,
            })

        # normalizer ko override karne se rokna hai
        for t in transactions:
            t['_type_locked'] = True

        self._log(f"Transactions parsed: {len(transactions)}")
        return normalize(transactions, opening_balance=opening_bal)

    @staticmethod
    def _clean_amt(s) -> float:
        if not s:
            return 0.0
        s = re.sub(r'[₹,\s]', '', str(s).strip())
        try:
            return float(s)
        except Exception:
            return 0.0

    def _extract_opening_balance(self, pdf_path: str) -> float:
        """HDFC statement summary se opening balance nikalo."""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text() or ''
                    if 'opening' in text.lower():
                        print(f"[hdfc-debug] Found 'opening' on page, text snippet: {repr(text[text.lower().find('opening'):text.lower().find('opening')+200])}")
                    # HDFC format: "Opening Balance Dr Count Cr Count Debits Credits Closing Bal"
                    # Next line: "498.82 132 80 470,188.15 488,797.44 19,108.11"
                    m = re.search(
                        r'opening\s*balance.*?[\n\r]+([\d,]+\.\d{2})',
                        text, re.IGNORECASE | re.DOTALL
                    )
                    if m:
                        val = self._clean_amt(m.group(1))
                        if val > 0:
                            self._log(f"OB found: {val}")
                            return val
        except Exception as e:
            self._log(f"OB extract error: {e}")
        return None
    