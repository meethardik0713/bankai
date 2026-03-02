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

    _DETECT_KEYWORDS = ['hdfc bank', 'hdfcbank', 'hdfc bank ltd', 'hdfc bank limited']
    _DATE_PAT        = re.compile(r'^\d{2}/\d{2}/\d{2}$')

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

        transactions = []
        for i, (date, bal) in enumerate(zip(all_dates, all_balances)):
            prev_bal = all_balances[i - 1] if i > 0 else 0.0
            diff     = round(bal - prev_bal, 2)
            txn_type = 'CR' if diff >= 0 else 'DR'
            amount   = abs(diff)
            narr     = all_narrations[i] if i < len(all_narrations) else ''
            transactions.append({
                'date':    date,
                'desc':    narr,
                'amount':  amount,
                'type':    txn_type,
                'balance': bal,
            })

        self._log(f"Transactions parsed: {len(transactions)}")
        return normalize(transactions)

    @staticmethod
    def _clean_amt(s) -> float:
        if not s:
            return 0.0
        s = re.sub(r'[₹,\s]', '', str(s).strip())
        try:
            return float(s)
        except Exception:
            return 0.0
        