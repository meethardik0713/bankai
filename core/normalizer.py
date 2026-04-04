"""
core/normalizer.py
──────────────────
Post-parse normalization: date formatting, CR/DR fix via balance continuity,
deduplication, and category tagging.

Used by ALL bank parsers after raw extraction.

v2.1 — Fixed:
- PCI/ card transactions properly categorized (Claude.ai, Canva, Railway etc.)
- MB: mobile banking transactions properly categorized
- Loan disbursal sources flagged as 'Loan Disbursal' not income
- Family transfer sources flagged as 'Family Transfer'
- Expanded category map with more Indian merchants
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
#  LOAN DISBURSAL SOURCES
#  Credits from these = loan received, NOT income
# ═══════════════════════════════════════════════════════════

LOAN_DISBURSAL_SOURCES = [
    'pocketly', 'speel finance', 'speel fin', 'stucred', 'stucredpayouts',
    'mpokket', 'mpokket financial', 'mpokket financi',
    'branch internat', 'branch int', 'branch payment',
    'truecredit', 'true credit', 'true credits',
    'lazypay', 'snapmint', 'kreon finnancia', 'kreon fin',
    'instantpay indi', 'instantpay india',
    'cashfree paymen', 'cashfree payment',
    'navi limited', 'slice small fin',
    'easebuzz privat', 'easebuzz',
    'brokentusk tech', 'setu brokentusk',
    'apibanking',
]

# ═══════════════════════════════════════════════════════════
#  FAMILY TRANSFER SOURCES
#  Credits from these = personal family transfer, NOT income
# ═══════════════════════════════════════════════════════════

FAMILY_TRANSFER_NAMES = [
    'rekha sharma', 'rekha/', '/rekha',
    'phani raj sharm', 'phani raj',
    'naina sharma',
    'hardik sharma',  # self transfers
]

# ═══════════════════════════════════════════════════════════
#  PCI MERCHANT → CATEGORY MAP
#  PCI/ prefix = card/international transaction
# ═══════════════════════════════════════════════════════════

PCI_MERCHANT_CATEGORY = {
    # Subscriptions
    'claude.ai': 'Subscription',
    'anthropic': 'Subscription',
    'scribd': 'Subscription',
    'higgsfield': 'Subscription',
    'canva': 'Subscription',
    'uwear.ai': 'Shopping',
    # Travel
    'railway': 'Travel',
    'irctc': 'Travel',
    # International payments
    'myfatoorah': 'International Payment',
    'fundingpi': 'International Payment',
    # Shopping / Gaming
    'google*play': 'Entertainment',
    'google play': 'Entertainment',
    'mountain v': 'Entertainment',  # Google Play Mountain View
    'valve': 'Entertainment',
    'steam': 'Entertainment',
    # Food
    'pizza': 'Food',
    'dominos': 'Food',
    'peppers pizza': 'Food',
}

# ═══════════════════════════════════════════════════════════
#  CATEGORY MAP  (full transaction descriptions)
# ═══════════════════════════════════════════════════════════

CATEGORY_MAP = {
    # Transport types first — broad buckets
    'NEFT/RTGS':          ['neftinw', 'neft ', 'rtgs'],
    'IMPS':               ['imps'],
    'ATM/Cash':           ['atm', 'cash withdrawal', 'cash wdl', 'cwdr',
                           'cash deposit', 'cdm'],

    # Specific income/transfer types
    'Salary':             ['salary', 'payroll', 'sal cr', 'wages',
                           'mb:received from shourya',
                           'mb:received from tramo',
                           '/salary'],
    'Loan Disbursal':     ['pocketly', 'speel finance', 'speel fin',
                           'stucred', 'stucredpayouts',
                           'mpokket', 'mpokket financial',
                           'branch internat', 'branch int',
                           'truecredit', 'true credit',
                           'lazypay', 'snapmint',
                           'kreon finnancia', 'instantpay indi',
                           'cashfree paymen', 'navi limited',
                           'slice small fin', 'easebuzz privat'],
    'EMI/Loan Repayment': ['emi', 'loan repay', 'nach'],
    'Family Transfer':    ['rekha sharma', 'rekha/', 'phani raj sharm',
                           'mb:sent to rekha', 'naina sharma'],
    'Freelance Income':   ['tramo technolab', 'tramo tech'],
    'Marketplace Income': ['meesho', 'shiprocket', 'meeshofas',
                           'myntra des', 'ekart'],

    # Expense categories
    'Food':               ['swiggy', 'zomato', 'blinkit', 'zepto',
                           'dominos', 'mcdonalds', 'pizza', 'swad sadan',
                           'shreejee', 'bikaner', 'gianis', 'dosa',
                           'annas dosa', 'peppers pizza', 'banaras wala',
                           'bharat juice', 'cafe ', 'dhaba', 'food',
                           'restaurant', 'hotel', 'snack'],
    'Shopping':           ['amazon', 'flipkart', 'myntra', 'ekart',
                           'westside', 'snitch', 'zudio', 'uwear',
                           'lenskart', 'safe gold', 'gold'],
    'Entertainment':      ['netflix', 'spotify', 'zee5', 'jiohotstar',
                           'google play', 'steam', 'valve', 'bookmyshow',
                           'bigtree', 'astrotalk', 'astrosage', 'higgsfield',
                           'scribd', 'claude.ai', 'anthropic'],
    'Subscription':       ['canva', 'godaddy', 'claude.ai subscription',
                           'anthropic', 'scribd', 'airtel digital',
                           'jio', 'airtel', 'dth'],
    'Travel':             ['aeronfly', 'irctc', 'makemytrip', 'redbus',
                           'railway', 'pci/0908/railway', 'busybees logist',
                           'rapido', 'ola ', 'uber'],
    'Health':             ['apollo pharmacy', 'one stop pharma',
                           'tata one mg', 'tablet medical', 'hospital',
                           'medical', 'pharmacy', 'health'],
    'Education':          ['amity universit', 'bennett', 'tutedude',
                           'work8ive', 'coursera', 'udemy'],
    'Utilities':          ['electricity', 'broadband', 'airtel',
                           'jio recharge', 'recharge', 'bijli'],
    'Investment':         ['zerodha', 'groww', 'safe gold', 'ppf',
                           'mutual fund', 'nse', 'bse'],
    'Transfer':           ['mb:sent', 'mb:received', 'transfer',
                           'trf ', 'fund transfer'],
    'Interest':           ['interest', 'int.pd', 'int pd', 'int cr',
                           'int.pd:', 'sbint'],
    'Charges':            ['charges', 'fee', 'commission',
                           'service charge', 'sms alert', 'annual fee',
                           'chrg:', 'tbms', 'dcc fee'],
    'Cheque':             ['cheque', 'chq', 'clearing', 'cts'],
    'Government':         ['uidai', 'govt', 'income tax', 'tds'],

    # UPI is the catch-all for UPI transactions not matched above
    'UPI':                ['upi/', 'upi-', 'phonepe', 'gpay',
                           'google pay', 'paytm', 'amazonpay', 'bhim'],
    'POS':                ['pos ', 'point of sale'],
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
    """
    Return category string for a transaction description.
    Priority order:
    1. PCI/ prefix → use merchant map
    2. MB: prefix → salary / family transfer
    3. Loan disbursal source check
    4. Family transfer check
    5. Full category map scan
    """
    lower = desc.lower()

    # ── PCI/ card transactions ──────────────────────────────
    if lower.startswith('pci/'):
        for merchant, cat in PCI_MERCHANT_CATEGORY.items():
            if merchant in lower:
                return cat
        return 'POS'  # generic card transaction

    # ── MB: mobile banking ──────────────────────────────────
    if lower.startswith('mb:'):
        if 'salary' in lower:
            return 'Salary'
        if 'sent to' in lower:
            return 'Family Transfer'
        if 'received from' in lower:
            return 'MB Transfer'
        return 'Transfer'

    # ── Loan disbursal sources ──────────────────────────────
    for src in LOAN_DISBURSAL_SOURCES:
        if src in lower:
            return 'Loan Disbursal'

    # ── Family transfer ─────────────────────────────────────
    for name in FAMILY_TRANSFER_NAMES:
        if name in lower:
            return 'Family Transfer'

    # ── Marketplace income check (before generic IMPS bucket) ──
    marketplace_kws = ['meesho', 'meeshofas', 'shiprocket', 'myntra des',
                       'reliance r', 'ekart']
    if any(k in lower for k in marketplace_kws):
        return 'Marketplace Income'

    # ── Full category map ───────────────────────────────────
    for cat, kws in CATEGORY_MAP.items():
        for kw in kws:
            if kw in lower:
                return cat

    return 'Other'


def is_loan_disbursal(desc: str) -> bool:
    """Returns True if this credit is a loan disbursal, not real income."""
    lower = desc.lower()
    for src in LOAN_DISBURSAL_SOURCES:
        if src in lower:
            return True
    return False


def is_family_transfer(desc: str) -> bool:
    """Returns True if this credit is a family/personal transfer."""
    lower = desc.lower()
    for name in FAMILY_TRANSFER_NAMES:
        if name in lower:
            return True
    return False


def is_self_transfer(desc: str) -> bool:
    """Returns True if this is a self-transfer between own accounts."""
    lower = desc.lower()
    self_patterns = ['hardik sharma', 'self transfer', 'own account',
                     'hardik101306', 'hardik/']
    return any(p in lower for p in self_patterns)


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
