"""
parsers/saraswat.py
────────────────────
Saraswat Co-operative Bank — signed single-amount-column format.

Format quirk: the "Amount" column carries its OWN sign (negative = debit,
positive = credit) and has NO 'DR'/'CR' suffix. The statement is also
printed newest-transaction-first (descending date order).

Because the sign is unambiguous, this parser reads CR/DR directly from the
sign instead of relying on core.normalizer._fix_types() (which infers type
from balance deltas and assumes ascending date order — wrong assumption for
this bank, and unnecessary here since we already know the answer).

Every transaction is tagged '_type_locked': True, which core.normalizer
already respects (see _resolve_opening_balance / _fix_types) — so this file
requires ZERO changes to the shared normalizer, and cannot affect any other
bank's parsing.
"""

import re
import pdfplumber

from parsers.base    import BaseParser
from core.normalizer import normalize

_RE_TXN_LINE = re.compile(
    r'^(\d{2}/\d{2}/\d{4})\s+(?:(\d{4,8})\s+)?(-?[\d,]+\.\d{1,2})\s+(-?[\d,]+\.\d{1,2})\s*$'
)

_SKIP_LINE_PATTERNS = [
    re.compile(r'date and time', re.I),
    re.compile(r'page\s+\d+\s+of\s+\d+', re.I),
    re.compile(r'^transactions list', re.I),
    re.compile(r'^date\s+instrument\s+id', re.I),
    re.compile(r'^remarks$', re.I),
]


class SaraswatParser(BaseParser):
    """
    Detection is structural, not text-phrase based.

    This export template (core-banking/net-banking PDF dump) always prints
    a fixed, nested sequence of section + field labels on page 1:

        Account Details
          General Details
            Number: → Nickname: → IBAN: → Status: → Type: → Name:
            → Currency: → Open Date: → Branch:
          Balance Details
            Available Balance: → Total Balance: → Ledger Balance:
            → Effective Available Balance: → Unclear Balance:
        Transactions List

    Matching this full nested ORDER (not just presence of one phrase) is far
    safer than a single bank-name/IFSC match:
      - A bare bank-name match risks false positives if that name coincidentally
        appears in a transaction narration on a DIFFERENT bank's statement.
      - An IFSC-prefix match (e.g. 'srcb') is actively unsafe here, since that
        code also shows up in NEFT/RTGS reference numbers on OTHER banks'
        statements whenever someone pays money INTO a Saraswat account.
      - This exact label sequence belongs to the export template itself and
        won't appear by chance in an unrelated statement's text.

    Deliberately EXCLUDED from the sequence: 'Drawing Power', 'Sanction
    Limit', 'Debit/Credit Accrued Interest', 'Pending Debit Card
    Authorizations' — these are Overdraft-account-specific fields that
    wouldn't appear on a Saraswat Savings/Current account statement from
    the same export tool, and would make detection brittle across account
    types.
    """

    _ORDERED_MARKERS = [
        'account details',
        'general details',
        'number:',
        'nickname:',
        'iban:',
        'name:',
        'status:',
        'type:',
        'currency:',
        'open date:',
        'branch:',
        'balance details',
        'available balance:',
        'total balance:',
        'ledger balance:',
        'effective available balance:',
        'unclear balance:',
        'transactions list',
    ]

    def detect_from_text(self, text_low: str) -> bool:
        pos = -1
        for marker in self._ORDERED_MARKERS:
            idx = text_low.find(marker, pos + 1)
            if idx == -1:
                return False
            pos = idx
        return True

    def detect(self, pdf_path: str) -> bool:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                first_text = (pdf.pages[0].extract_text() or '').lower()
            return self.detect_from_text(first_text)
        except Exception:
            return False

    def parse(self, pdf_path: str) -> list:
        raw_rows = self._extract_rows(pdf_path)
        self._log(f"Raw signed rows: {len(raw_rows)}")
        if not raw_rows:
            return []

        # Statement is newest-first: the LAST row in the list is the oldest
        # transaction. Opening balance = that row's balance MINUS its own
        # signed amount (undo its effect to recover the pre-transaction
        # balance).
        last = raw_rows[-1]
        opening_balance = round(last['_signed_balance'] - last['_signed_amount'], 2)

        txns = []
        for r in raw_rows:
            signed_amt = r['_signed_amount']
            txns.append({
                'date':         r['date'],
                'desc':         r['desc'],
                'amount':       round(abs(signed_amt), 2),
                'balance':      round(r['_signed_balance'], 2),
                'type':         'CR' if signed_amt > 0 else 'DR',
                'reference':    r.get('reference', ''),
                # Authoritative — sign already tells us CR/DR with zero
                # ambiguity.
                '_type_locked': True,
            })

        # This bank's PDF prints newest-transaction-first. Every shared
        # downstream module in this codebase (core.normalizer._fix_types,
        # core.post_validator.validate_and_fix) assumes ascending
        # (oldest-first) order — reversing HERE, once, inside this parser,
        # means those shared files need zero changes and keep working
        # exactly as they already do for every other bank.
        txns.reverse()

        return normalize(txns, opening_balance=opening_balance)

    # ══════════════════════════════════════════════════════
    #  EXTRACTION
    # ══════════════════════════════════════════════════════

    def _extract_rows(self, pdf_path: str) -> list:
        rows = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text() or ''
                    rows.extend(self._parse_page_text(text))
        except Exception as e:
            self._log(f"Cannot open/read: {e}")
        return rows

    def _parse_page_text(self, text: str) -> list:
        rows    = []
        pending = None

        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if self._should_skip_line(line):
                continue

            m = _RE_TXN_LINE.match(line)
            if m:
                if pending:
                    rows.append(self._finalize(pending))
                date_str, ref, amt_str, bal_str = m.groups()
                pending = {
                    'date':       date_str,
                    'reference':  ref or '',
                    'amt_str':    amt_str,
                    'bal_str':    bal_str,
                    'desc_lines': [],
                }
            elif pending:
                pending['desc_lines'].append(line)

        if pending:
            rows.append(self._finalize(pending))

        return rows

    @staticmethod
    def _should_skip_line(line: str) -> bool:
        return any(p.search(line) for p in _SKIP_LINE_PATTERNS)

    @staticmethod
    def _finalize(pending: dict) -> dict:
        signed_amt = SaraswatParser._to_signed_float(pending['amt_str'])
        signed_bal = SaraswatParser._to_signed_float(pending['bal_str'])
        desc = ' '.join(pending['desc_lines']).strip()[:200]
        return {
            'date':            pending['date'],
            'desc':            desc,
            'reference':       pending['reference'],
            '_signed_amount':  signed_amt,
            '_signed_balance': signed_bal,
        }

    @staticmethod
    def _to_signed_float(s: str) -> float:
        s = s.replace(',', '').strip()
        try:
            return float(s)
        except ValueError:
            return 0.0
        