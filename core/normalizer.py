"""
core/normalizer.py
──────────────────
Post-parse normalization: date formatting, CR/DR fix via balance continuity,
deduplication, and category tagging.

Used by ALL bank parsers after raw extraction.
"""

import re
from datetime import datetime

# ═══════════════════════════════════════════════════════════
#  PRE-COMPILED PATTERNS
# ═══════════════════════════════════════════════════════════

_DATE_PATTERNS = [
    (re.compile(r'\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\b'),
     ['%d %b %Y', '%d %B %Y']),
    (re.compile(r'\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2})\b'),
     ['%d %b %y', '%d %B %y']),
    (re.compile(r'\b(\d{2}[A-Za-z]{3}\d{4})\b'),
     ['%d%b%Y']),
    (re.compile(r'\b(\d{2}[A-Za-z]{3}\d{2})\b'),
     ['%d%b%y']),
    (re.compile(r'\b(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{4})\b'),
     ['%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%m/%d/%Y']),
    (re.compile(r'\b(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2})\b'),
     ['%d/%m/%y', '%d-%m-%y', '%d.%m.%y']),
    (re.compile(r'\b(\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2})\b'),
     ['%Y-%m-%d', '%Y/%m/%d']),
]

_RE_WHITESPACE = re.compile(r'\s+')

_JUNK_PHRASES = [
    'rbi mandate', 'important information', 'account no.',
    'kotak mahindra bank', 'statement generated',
]

# ═══════════════════════════════════════════════════════════
#  CATEGORY MAP
# ═══════════════════════════════════════════════════════════

CATEGORY_MAP = {
    'UPI':           ['upi/', 'upi-', 'phonepe', 'gpay', 'google pay',
                      'paytm', 'amazonpay', 'bhim'],
    'NEFT/RTGS':     ['neft', 'rtgs', 'neftinw'],
    'IMPS':          ['imps'],
    'ATM/Cash':      ['atm', 'cash withdrawal', 'cash wdl', 'cwdr',
                      'cash deposit'],
    'Salary':        ['salary', 'payroll', 'sal cr', 'wages'],
    'EMI/Loan':      ['pocketly', 'speel finance', 'stucred', 'mpokket',
                      'branch internat', 'truecredit', 'lazypay',
                      'snapmint', 'emi', 'loan'],
    'POS':           ['pos ', 'point of sale', 'pci/'],
    'Interest':      ['interest', 'int.pd', 'int pd', 'int cr',
                      'int.pd:', 'sbint'],
    'Charges':       ['charges', 'fee', 'commission', 'gst',
                      'service charge', 'sms alert', 'annual fee', 'chrg:'],
    'Transfer':      ['transfer', 'trf ', 'fund transfer',
                      'mb:sent', 'mb:received'],
    'Cheque':        ['cheque', 'chq', 'clearing', 'cts'],
    'Food':          ['swiggy', 'zomato', 'blinkit', 'zepto', 'dominos',
                      'mcdonalds', 'pizza', 'swad sadan', 'shreejee',
                      'bikaner', 'gianis', 'dosa'],
    'Shopping':      ['amazon', 'flipkart', 'myntra', 'meesho', 'ekart',
                      'westside', 'snitch', 'zudio'],
    'Entertainment': ['netflix', 'spotify', 'zee5', 'jiohotstar',
                      'google play', 'steam', 'valve', 'bookmyshow'],
    'Travel':        ['aeronfly', 'irctc', 'makemytrip', 'redbus'],
}


# ═══════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════

def normalize(txns: list, opening_balance: float = None) -> list:
    """
    Main normalization pipeline:
    1. Infer/confirm opening balance
    2. Fix CR/DR via sequential balance diff
    3. Deduplicate
    4. Attach opening balance to first txn
    5. Clean desc + categorize
    """
    if not txns:
        return []

    opening_balance = _resolve_opening_balance(txns, opening_balance)
    _fix_types(txns)
    result = _dedup_and_clean(txns)

    if result and opening_balance is not None:
        result[0]['opening_balance'] = round(opening_balance, 2)
    elif result:
        result[0].setdefault('opening_balance', None)

    return result


def normalize_date(date_str: str) -> str:
    """Convert any date string to YYYY-MM-DD. Returns original if unparseable."""
    if not date_str:
        return ''
    for pattern, fmts in _DATE_PATTERNS:
        m = pattern.search(str(date_str))
        if not m:
            continue
        raw = m.group(1) if m.lastindex else m.group()
        for fmt in fmts:
            try:
                dt = datetime.strptime(raw.strip(), fmt)
                if 2000 <= dt.year <= 2035:
                    return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
    return date_str


def categorize(desc: str) -> str:
    """Return category string for a transaction description."""
    lower = desc.lower()
    for cat, kws in CATEGORY_MAP.items():
        for kw in kws:
            if kw in lower:
                return cat
    return 'Other'


# ═══════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════

def _resolve_opening_balance(txns: list, opening_balance: float) -> float:
    first = txns[0]
    b0    = first.get('balance')
    amt0  = first.get('amount', 0)

    if opening_balance is None:
        if b0 is not None and amt0 and not first.get('_type_locked'):
            implied_cr = round(b0 - amt0, 2)
            implied_dr = round(b0 + amt0, 2)
            if abs(b0 - amt0) <= max(1.0, amt0 * 0.01):
                opening_balance = 0.0
                first['type']   = 'CR'
                print(f"[normalize] First txn looks like opening deposit → OB=0.00")
            elif implied_cr >= 0:
                opening_balance = implied_cr
                first['type']   = 'CR'
                print(f"[normalize] Inferred OB (CR path): ₹{opening_balance:,.2f}")
            elif implied_dr >= 0:
                opening_balance = implied_dr
                first['type']   = 'DR'
                print(f"[normalize] Inferred OB (DR path): ₹{opening_balance:,.2f}")
    else:
        if b0 is not None and amt0 and not first.get('_type_locked'):
            diff = round(b0 - opening_balance, 2)
            tol  = max(1.0, round(amt0 * 0.01, 2))
            if abs(diff - amt0) <= tol:
                first['type'] = 'CR'
                print(f"[normalize] OB from PDF → first txn CR")
            elif abs(diff + amt0) <= tol:
                first['type'] = 'DR'
                print(f"[normalize] OB from PDF → first txn DR")

    return opening_balance


def _fix_types(txns: list):
    """Fix CR/DR for all transactions using sequential balance diff."""
    for i in range(1, len(txns)):
        curr   = txns[i]
        prev   = txns[i - 1]
        b_curr = curr.get('balance')
        b_prev = prev.get('balance')
        amt    = curr.get('amount', 0)

        if b_curr is not None and b_prev is not None and amt and not curr.get('_type_locked'):
            diff = round(b_curr - b_prev, 2)
            tol  = max(1.0, round(amt * 0.01, 2))
            if abs(diff - amt) <= tol:
                curr['type'] = 'CR'
            elif abs(diff + amt) <= tol:
                curr['type'] = 'DR'


def _dedup_and_clean(txns: list) -> list:
    """Deduplicate transactions and clean descriptions."""
    seen, result = set(), []
    for t in txns:
        norm_date = normalize_date(t['date'])
        key = (
            norm_date,
            t['amount'],
            t['type'],
            (t.get('desc') or '').strip(),
            t.get('balance'),
        )
        if key in seen:
            continue
        seen.add(key)

        t['date'] = norm_date or t['date']

        # Clean desc — strip junk phrases
        raw_desc = _RE_WHITESPACE.sub(' ', t.get('desc') or '').strip()
        for junk in _JUNK_PHRASES:
            ji = raw_desc.lower().find(junk)
            if ji > 10:
                raw_desc = raw_desc[:ji].strip()
        t['desc']     = raw_desc[:200].strip()
        t['category'] = categorize(t['desc'])

        result.append(t)
    return result

