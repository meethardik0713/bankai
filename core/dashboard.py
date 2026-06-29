"""
core/dashboard.py — V6 (Central Engine + Validation Architecture)
══════════════════════════════════════════════════════════════════
12-point consistency overhaul. Logic only — zero UI changes.

Architecture:
  ┌─────────────────────────────────────────────────────────┐
  │  CONSTANTS  (single place for all thresholds/rates)     │
  │  ↓                                                      │
  │  CLASSIFIER  (credit/expense classification)            │
  │  ↓                                                      │
  │  FinancialEngine  (single source of truth)              │
  │    • real_income, monthly_income, foir, emi             │
  │    • loan_eligible, avg_balance                         │
  │    • risk_score, risk_level, risk_flags                 │
  │  ↓                                                      │
  │  ValidationPass  (pre-report consistency check)         │
  │  ↓                                                      │
  │  Module reporters  (read engine, never recalculate)     │
  │  ↓                                                      │
  │  run_dashboard()  (assembles final dict)                │
  └─────────────────────────────────────────────────────────┘

Guarantees:
  • FOIR in every section = same value from engine
  • Real Income on cover == income section == summary
  • Risk level in underwriting == fraud section == risk flags
  • Pre-export validation rejects contradictory reports
  • Full backward compatibility
"""

from __future__ import annotations
from collections import defaultdict
from datetime import datetime
import re
import calendar
import math
from difflib import SequenceMatcher
from dataclasses import dataclass, field
from typing import List, Dict, Optional

from core.normalizer import (
    is_loan_disbursal, is_family_transfer, is_self_transfer,
)


# ══════════════════════════════════════════════════════════════
#  §1  CENTRAL CONSTANTS  (#11)
#  ALL thresholds, rates and limits live here.
#  Never hardcode these elsewhere.
# ══════════════════════════════════════════════════════════════

class C:
    """Central constants — single place for all financial parameters."""

    # FOIR
    FOIR_HEALTHY    = 35.0   # % — below this is healthy
    FOIR_MODERATE   = 50.0   # % — above this is risky
    FOIR_CAP        = 50.0   # % — bank policy max FOIR for eligibility

    # Loan eligibility
    INTEREST_RATE   = 0.10   # 10% p.a. (standard personal/MSME)
    TENURE_MONTHS   = 60     # 5-year tenure for eligibility calc

    # Balance thresholds
    MIN_BAL_RATIO   = 0.20   # avg_balance / avg_income — below = flag
    BAL_GOOD_RATIO  = 0.50   # avg_balance / avg_income — above = healthy

    # Cash / compliance
    HIGH_VALUE      = 200_000
    DAILY_CASH_LIMIT= 50_000
    ANNUAL_CASH_LIM = 1_000_000
    STR_THRESHOLD   = 500_000

    # Risk scoring weights (penalty-based, starts 100)
    STABILITY_PENALTY   = {'Stable': 0, 'Moderately Stable': 10, 'Volatile': 25, 'Unknown': 15}
    FOIR_PENALTY_BREAKS = [(35, 0), (40, 6), (50, 11), (60, 16), (999, 20)]
    NEG_DAYS_PENALTY    = [(0, 0), (2, 7), (5, 11), (999, 15)]
    DEP_PENALTY         = {'Low': 0, 'Medium': 6, 'High': 12}
    BOUNCE_PENALTY      = [(0, 0), (1, 4), (3, 6), (999, 8)]
    BAL_RATIO_PENALTY   = [(1.0, 0), (0.5, 5), (0.2, 12), (0.0, 20)]

    # Risk flag thresholds
    LOAN_DISBURSAL_FLAG = 3     # ≥ this many → frequent_loan flag
    FAMILY_XFER_PCT     = 15.0  # % of total credits
    CASH_DEP_FLAG_AMT   = 0
    LOW_BAL_FLAG_RATIO  = 0.20
    UNVERIFIED_FLAG_PCT = 30.0  # % of credits marked needs_verification
    SPENDING_SPIKE_MULT = 2.5   # × average

    # 80C deduction limits (Indian IT Act)
    MAX_80C = 150_000
    MAX_80D = 25_000

    # GST rate
    GST_RATE = 0.18

    # Duplicate detection
    DUP_DESC_SIMILARITY = 0.70  # SequenceMatcher ratio threshold

    # Income classification CV thresholds
    CV_STABLE   = 25.0
    CV_MODERATE = 50.0

    @staticmethod
    def step_lookup(breaks: list, value: float) -> int:
        """Lookup penalty from ordered break table [(threshold, penalty), ...]."""
        for threshold, penalty in breaks:
            if value <= threshold:
                return penalty
        return breaks[-1][1]


# ══════════════════════════════════════════════════════════════
#  §2  UTILITIES
# ══════════════════════════════════════════════════════════════

_RE_CACHE: Dict[str, Optional[re.Pattern]] = {}


def _re(kw: str) -> Optional[re.Pattern]:
    if kw not in _RE_CACHE:
        if len(kw.strip()) <= 5:
            _RE_CACHE[kw] = re.compile(
                rf'(?<![a-zA-Z0-9]){re.escape(kw.strip())}(?![a-zA-Z0-9])',
                re.IGNORECASE,
            )
        else:
            _RE_CACHE[kw] = None
    return _RE_CACHE[kw]


def _kw(text: str, keywords: list) -> bool:
    for kw in keywords:
        pat = _re(kw)
        if pat is not None:
            if pat.search(text): return True
        else:
            if kw in text: return True
    return False


def _kw_first(text: str, keywords: list) -> Optional[str]:
    for kw in keywords:
        pat = _re(kw)
        if pat is not None:
            if pat.search(text): return kw
        else:
            if kw in text: return kw
    return None


def _safe(n, fallback: float = 0.0) -> float:
    try:
        v = float(n)
        return v if math.isfinite(v) else fallback
    except (TypeError, ValueError):
        return fallback


def _pct(num: float, den: float, decimals: int = 1) -> float:
    d = _safe(den)
    return round(_safe(num) / d * 100, decimals) if d != 0 else 0.0


def _div(num: float, den: float, fallback: float = 0.0) -> float:
    d = _safe(den)
    return _safe(num) / d if d != 0 else fallback


def _month(t: dict) -> str:
    d = t.get('date') or ''
    return d[:7] if len(d) >= 7 else 'Unknown'


def _parse_date(s: str) -> Optional[datetime.date]:
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except Exception:
        return None


def _pmt(rate_pm: float, n_months: int, principal: float) -> float:
    """Standard PMT: monthly payment for loan principal."""
    if rate_pm <= 0 or n_months <= 0:
        return _div(principal, n_months)
    return round(principal * rate_pm / (1 - (1 + rate_pm) ** -n_months), 2)


def _eligible_principal(monthly_capacity: float, rate_pa: float, tenure_months: int) -> float:
    """Loan principal given monthly EMI capacity, rate, tenure. (#6)"""
    if monthly_capacity <= 0:
        return 0.0
    rate_pm = rate_pa / 12
    if rate_pm <= 0:
        return round(monthly_capacity * tenure_months, 2)
    return round(monthly_capacity * (1 - (1 + rate_pm) ** -tenure_months) / rate_pm, 2)


# ══════════════════════════════════════════════════════════════
#  §3  CLASSIFICATION KEYWORDS
# ══════════════════════════════════════════════════════════════

_SALARY_KW     = ['salary', 'payroll', 'sal cr', 'wages', 'pay credit', 'monthly pay',
                   '/salary', 'shourya technologies', 'mb:received from shourya']
_FREELANCE_KW  = ['tramo technolab', 'tramo tech']
_MKTP_KW       = ['meesho', 'shiprocket', 'meeshofas', 'myntra des', 'razorpay payments']
_INTEREST_KW   = ['interest', 'int.pd', 'int pd', 'int cr', 'sbint', 'fd interest',
                   'savings interest', 'interest credit']
_DIVIDEND_KW   = ['dividend', 'div cr', 'div paid', 'dividend credit']
_RENTAL_KW     = ['rent received', 'rental income', 'house rent', 'property rent']
_CASH_DEP_KW   = ['cash deposit', 'cash deposit at', 'cash dep', 'cashdep', 'cdm deposit', 'cash deposit at/']
_REIMB_KW      = ['reimbursement', 'reimb', 'expense claim', 'travel claim', 'medical reimbursement']
_NON_INC_SIG   = ['wallet load', 'wallet topup', 'wallet transfer', 'paytm wallet',
                   'phonepe wallet', 'prepaid load', 'card load', 'failed payment return',
                   'neft outward return', 'excess amount']
_REVERSAL_KW   = ['reversal', 'reversed', 'refund', 'cashback', 'cash back', 'chargeback',
                   'charge back', 'credit reversal', 'rev:', 'returned', 'reimburse',
                   'dispute credit', 'txn reversal', 'failed txn', 'auto reversal']
_BIZ_CR_KW     = ['razorpay settlement', 'cashfree settlement', 'payu settlement',
                   'instamojo settlement', 'ccavenue settlement', 'stripe payout',
                   'consulting fee', 'professional fee', 'invoice payment',
                   'project payment', 'client payment']
_EMI_KW        = ['emi payment', 'loan repay', 'nach debit', 'nach/', 'pocketly',
                   'stucred', 'mpokket', 'speel finance', 'speel fin', 'lazypay repay',
                   'lazypay', 'snapmint', 'truecredit', 'true credits',
                   'branch international', 'kreon finnancia', 'instantpay',
                   'home loan', 'car loan emi', 'personal loan emi']
_BOUNCE_KW     = ['return unpaid', 'bounce', 'dishonour', 'ecs return',
                   'nach return', 'insufficient fund', 'si return']
_CASH_KW       = ['cash deposit', 'cash withdrawal', 'atm withdrawal', 'cwdr', 'cash wdl']

# Strict 80C — no Safe Gold (#15)
_80C_KW        = ['lic premium', 'ppf deposit', 'nsc ', 'elss ', 'epf contribution',
                   'provident fund', 'life insurance premium', 'tax saving fd',
                   'sukanya samriddhi', 'nps contribution', 'tuition fee', 'stamp duty']
_80C_EXCLUDE   = ['safe gold', 'safegold', 'gold etf', 'sovereign gold', 'digital gold', 'gold bond']
_80D_KW        = ['health insurance', 'mediclaim', 'star health', 'bajaj allianz health',
                   'niva bupa', 'max bupa', 'care health', 'aditya birla health', 'health premium']
_TDS_KW        = ['tds deducted', 'tax deducted', 'income tax deducted']

# ── Spend categories (#8 — UPI never appears as category)
_SPEND_CATS = {
    'EMI / Loan Repayment': ['pocketly', 'stucred', 'mpokket', 'lazypay', 'snapmint',
        'truecredit', 'true credits', 'branch international', 'kreon finnancia',
        'instantpay', 'nach debit', 'nach/', 'speel finance', 'emi payment', 'loan repay'],
    'Food & Dining': ['swiggy', 'zomato', 'dominos', 'pizza hut', 'mcdonalds', 'kfc',
        'subway', 'restaurant', 'cafe', 'annas dosa', 'shreejee', 'swad sadan',
        'gianis', 'banaras wala', 'bikaner sweets', 'burger king', 'dunkin', 'chaayos', 'starbucks'],
    'Grocery': ['blinkit', 'zepto', 'bigbasket', 'dmart', 'big bazaar', 'reliance fresh',
        'more supermarket', 'grofers', 'jiomart', 'nature basket', 'spencers'],
    'Shopping': ['amazon', 'flipkart', 'myntra', 'meesho', 'ajio', 'snitch', 'zudio',
        'nykaa', 'bewakoof', 'clovia', 'tata cliq', 'reliance trends'],
    'Fuel': ['petrol', 'fuel', 'diesel', 'hp petrol', 'iocl', 'bpcl', 'indian oil',
        'hindustan petroleum', 'bharat petroleum', 'shell', 'fastag'],
    'Healthcare': ['apollo pharmacy', 'one stop pharma', 'tata one mg', 'medplus',
        'pharmeasy', 'hospital', 'clinic', 'doctor', 'medicine', 'pharmacy', 'netmeds'],
    'Education': ['school fee', 'college fee', 'tuition', 'udemy', 'coursera',
        'byju', 'unacademy', 'vedantu', 'exam fee', 'books'],
    'Utilities': ['electricity', 'water bill', 'gas bill', 'bses', 'tata power',
        'adani electricity', 'mahanagar gas', 'igl', 'piped gas', 'bescom', 'mseb'],
    'Entertainment': ['netflix', 'spotify', 'bookmyshow', 'pvr ', 'inox', 'zee5',
        'jiohotstar', 'amazon prime', 'disney', 'youtube premium', 'gaana', 'apple music'],
    'Travel': ['irctc', 'makemytrip', 'redbus', 'goibibo', 'indigo', 'spicejet',
        'airbnb', 'oyo ', 'uber', 'olacabs', 'ola cabs', 'ola ride', 'rapido'],
    'Telecom': ['airtel', 'jio recharge', 'vodafone', 'vi ', 'bsnl', 'mobile recharge',
        'broadband', 'internet', 'dth recharge', 'tata sky', 'dish tv'],
    'Investment': ['mutual fund', 'sip ', 'nse', 'bse', 'zerodha', 'groww', 'upstox',
        'icicidirect', 'hdfc securities', 'ppf deposit', 'nps ', 'elss ', 'fd ', 'fixed deposit'],
    'Subscriptions': ['linkedin premium', 'canva pro', 'claude.ai', 'anthropic',
        'microsoft 365', 'adobe', 'scribd', 'google one', 'icloud', 'higgsfield'],
    'ATM / Cash Withdrawal': ['atm withdrawal', 'cash wdl', 'cwdr', 'atm cash'],
    'Insurance': ['lic premium', 'life insurance', 'health insurance', 'mediclaim',
        'star health', 'niva bupa', 'bajaj allianz', 'hdfc life', 'icici prudential', 'max life'],
    'Rent': ['house rent', 'pg rent', 'flat rent', 'lease rent', 'rental payment', 'room rent'],
}

# ══════════════════════════════════════════════════════════════
#  §4  CREDIT / EXPENSE CLASSIFIER
# ══════════════════════════════════════════════════════════════

class CC:
    """Credit classification constants."""
    SALARY   = 'salary';      BUSINESS = 'business'
    INTEREST = 'interest';    DIVIDEND = 'dividend'
    RENTAL   = 'rental';      REIMBURSE= 'reimbursement'
    REFUND   = 'refund';      SELF     = 'self_transfer'
    FAMILY   = 'family_transfer';  LOAN = 'loan_disbursal'
    CASH_DEP = 'cash_deposit';     NON_INC= 'non_income'
    NEEDS_VER= 'needs_verification'

_TAXABLE     = {CC.SALARY, CC.BUSINESS, CC.INTEREST, CC.DIVIDEND, CC.RENTAL}
_NON_TAXABLE = {CC.LOAN, CC.SELF, CC.FAMILY, CC.REFUND, CC.REIMBURSE,
                CC.NON_INC, CC.CASH_DEP, CC.NEEDS_VER}


def _classify_credit(lower: str, amt: float) -> str:
    """#1/#9 — Deterministic credit classifier. Unknown → NEEDS_VER."""
    if is_loan_disbursal(lower):        return CC.LOAN
    if is_self_transfer(lower):         return CC.SELF
    if is_family_transfer(lower):       return CC.FAMILY
    if _kw(lower, _REVERSAL_KW):        return CC.REFUND
    if _kw(lower, _REIMB_KW):           return CC.REIMBURSE
    if _kw(lower, _NON_INC_SIG):        return CC.NON_INC
    if _kw(lower, _CASH_DEP_KW):        return CC.CASH_DEP
    if _kw(lower, _SALARY_KW):          return CC.SALARY
    if _kw(lower, _FREELANCE_KW + _MKTP_KW + _BIZ_CR_KW): return CC.BUSINESS
    if _kw(lower, _DIVIDEND_KW):        return CC.DIVIDEND
    if _kw(lower, _INTEREST_KW):        return CC.INTEREST
    if _kw(lower, _RENTAL_KW):          return CC.RENTAL
    return CC.NEEDS_VER   # never auto-taxable


def _categorize_expense(desc: str, parser_cat: str = '') -> str:
    """#8/#9 — UPI is a payment mode, NEVER an expense category."""
    lower = desc.lower()
    for cat, kws in _SPEND_CATS.items():
        for kw in kws:
            pat = _re(kw)
            if (pat.search(lower) if pat else kw in lower):
                return cat
    # Try VPA prefix from UPI string
    m = re.search(r'(?:upi|p2m)[/\-]([a-zA-Z][a-zA-Z0-9]{2,20})[@\.]', lower)
    if m:
        vpa = m.group(1)
        for cat, kws in _SPEND_CATS.items():
            for kw in kws:
                if len(kw) > 4 and kw in vpa:
                    return cat
    # Accept parser category if it's not a payment-mode word
    _pm_words = {'upi', 'neft', 'imps', 'rtgs', 'transfer', 'payment',
                  'charges', 'other', 'unknown', 'misc', 'debit'}
    if parser_cat and parser_cat.lower() not in _pm_words:
        return parser_cat
    return 'Others'


# ══════════════════════════════════════════════════════════════
#  §5  MERCHANT EXTRACTION
# ══════════════════════════════════════════════════════════════

_KNOWN_MERCHANTS = {
    'pocketly', 'blinkit', 'zomato', 'swiggy', 'amazon', 'flipkart',
    'netflix', 'spotify', 'myntra', 'zepto', 'bigbasket', 'phonepe',
    'gpay', 'paytm', 'razorpay', 'stucred', 'lazypay', 'snapmint',
    'mpokket', 'truecredit', 'meesho', 'uber', 'ola', 'rapido',
    'irctc', 'makemytrip', 'bookmyshow', 'dream11', 'canva',
    'anthropic', 'airtel', 'jio', 'dmart', 'dominos', 'kfc',
    'linkedin', 'youtube', 'hotstar', 'godaddy', 'shopify',
    'groww', 'zerodha', 'upstox', 'apollo', 'medplus', 'pharmeasy',
    'indigo', 'spicejet', 'oyo', 'airbnb', 'byju', 'unacademy',
}
_EMPLOYER_RE = [
    re.compile(r'mb:received from\s+(.+)', re.I),
    re.compile(r'neft\s*cr[- ]*\d*[- ]*(.{3,40}?)(?:\s{2,}|/|$)', re.I),
    re.compile(r'neft/([^/]{3,40}?)/', re.I),
    re.compile(r'neft(?:inw)?[- /]+([A-Z][A-Z0-9 &.\-]{2,40}?)(?:/|\s{2,}|$)', re.I),
    re.compile(r'imps[- ]*\d+[- ]+(.{3,40}?)[- ]+[A-Z]{4}\d', re.I),
    re.compile(r'rtgs[- /]+(?:\d+[- /]+)?([A-Z][A-Z0-9 &.\-]{2,40}?)(?:/|\s{2,}|$)', re.I),
    re.compile(r'/([A-Z][A-Z0-9 &.\-]{2,30})/', re.I),
]
_VPA_RE = re.compile(r'(?:upi|p2m)[/\-]([a-zA-Z][a-zA-Z0-9]{2,20})[@\.]', re.I)
_STOP   = {'nach', 'neft', 'imps', 'rtgs', 'debit', 'credit', 'transfer',
            'payment', 'bank', 'from', 'to', 'upi', 'ref', 'no', 'txn'}


def _extract_employer(desc: str) -> str:
    if not desc: return ''
    for pat in _EMPLOYER_RE:
        m = pat.search(desc)
        if m:
            name = m.group(1).strip(' /-')
            if len(name) > 2: return name[:50].title()
    lower = desc.lower()
    for kw in ('salary', 'payroll', 'sal cr', 'wages'):
        idx = lower.find(kw)
        if idx != -1:
            after = desc[idx + len(kw):].strip(' -:/').split()
            if after: return ' '.join(after[:4]).title()
    return ''


def _extract_merchant(desc: str) -> str:
    lower = desc.lower()
    for m in _KNOWN_MERCHANTS:
        if m in lower: return m.title()
    vpa = _VPA_RE.search(desc)
    if vpa:
        prefix = vpa.group(1).lower()
        for m in _KNOWN_MERCHANTS:
            if m in prefix or prefix.startswith(m[:5]): return m.title()
        return prefix[:25].title()
    for pat in _EMPLOYER_RE:
        m2 = pat.search(desc)
        if m2:
            name = m2.group(1).strip(' /-')[:25]
            if len(name) > 2: return name.title()
    tokens = [w for w in re.split(r'[\s/\-:]+', desc)
              if len(w) > 3 and w.lower() not in _STOP and not w.isdigit()]
    return tokens[0][:20].title() if tokens else 'Other'


def _payment_mode(desc: str) -> str:
    lower = desc.lower()
    if re.search(r'(upi[/\-\s]|@ok|@ybl|@ibl|@axl|@okhdfcbank|@oksbi|@paytm|gpay|phonepe|bhim)', lower):
        return 'UPI'
    if 'imps' in lower: return 'IMPS'
    if 'neft' in lower: return 'NEFT'
    if 'rtgs' in lower: return 'RTGS'
    if any(k in lower for k in ('atm withdrawal', 'atm wdr', 'cash wdl', 'cwdr')):
        return 'ATM'
    if any(k in lower for k in ('cash deposit', 'cash dep', 'cdm deposit')):
        return 'Cash Deposit'
    if any(k in lower for k in ('pos/', ' pos ', 'pci/', 'card swipe')):
        return 'Card'
    if any(k in lower for k in ('swift', 'foreign inward', 'usd ', 'eur ', 'paypal', 'stripe', 'wise')):
        return 'International'
    if any(k in lower for k in ('chq', 'cheque', 'clearing', 'cts/')):
        return 'Cheque'
    if any(k in lower for k in ('nach', 'ecs/', 'mandate', 'auto debit')):
        return 'NACH/ECS'
    return 'Other'


# ══════════════════════════════════════════════════════════════
#  §6  SHARED CALCULATION ENGINE  (#1 #2 #3 #4 #10)
# ══════════════════════════════════════════════════════════════

@dataclass
class EngineResult:
    """
    Single source of truth for ALL financial metrics.
    Every report section reads from this — NEVER recalculates.
    """
    # ── Income (#3)
    real_income_total:      float = 0.0
    avg_monthly_real_income:float = 0.0
    avg_monthly_credit:     float = 0.0
    avg_monthly_debit:      float = 0.0
    n_months:               int   = 1
    real_income_breakdown:  Dict  = field(default_factory=dict)
    monthly_real_income:    Dict  = field(default_factory=dict)
    salary_total:           float = 0.0
    business_total:         float = 0.0
    interest_total:         float = 0.0
    dividend_total:         float = 0.0
    rental_total:           float = 0.0
    loan_disbursal_total:   float = 0.0
    family_transfer_total:  float = 0.0
    self_transfer_total:    float = 0.0
    refund_total:           float = 0.0
    cash_dep_total:         int   = 0
    needs_ver_total:        float = 0.0
    non_income_credits:     Dict  = field(default_factory=dict)

    # ── EMI / FOIR (#1)
    monthly_fixed_obligations: float = 0.0   # EMI-only
    foir:                      float = 0.0
    foir_status:               str   = 'Healthy'

    # ── Loan eligibility (#6)
    max_eligible_emi:          float = 0.0
    remaining_emi_capacity:    float = 0.0
    loan_eligible:             float = 0.0

    # ── Balance
    avg_balance:               float = 0.0
    min_balance:               float = 0.0
    max_balance:               float = 0.0
    negative_months:           int   = 0
    negative_balance_days:     int   = 0

    # ── Risk (#4) — unified risk engine
    risk_score:    int   = 0
    risk_level:    str   = 'Low'
    risk_color:    str   = 'green'
    risk_flags:    List  = field(default_factory=list)
    risk_reasons:  List  = field(default_factory=list)

    # ── Health score
    health_score:  int   = 0
    health_grade:  str   = 'Unknown'
    health_color:  str   = 'yellow'

    # ── Loan dependency
    loan_dep_level:     str   = 'Low'
    loan_dep_color:     str   = 'green'
    loan_dep_pct:       float = 0.0

    # ── Income stability
    income_stability:   str   = 'Unknown'
    stability_color:    str   = 'yellow'
    stability_cv:       float = 0.0

    # ── Credit profile
    credit_indicator:   str   = 'Unknown'
    credit_color:       str   = 'yellow'
    credit_summary:     str   = ''
    credit_reasons:     List  = field(default_factory=list)

    # ── Disposable income
    disposable_income:  float = 0.0
    variable_expenses:  float = 0.0
    savings_rate_pct:   float = 0.0

    # ── Totals
    total_cr:  float = 0.0
    total_dr:  float = 0.0
    total_txns:int   = 0

    # ── Bounce / audit
    bounce_count:          int = 0
    reconciliation_score:  int = 100


def _build_engine(transactions: list) -> EngineResult:
    """
    #2 — ONE computation pass. All metrics derived here.
    No other function should recalculate these values.
    """
    e = EngineResult()
    e.total_txns = len(transactions)

    if not transactions:
        return e

    # ── Pass 1: classify every transaction ─────────────────────
    buckets: Dict[str, List] = defaultdict(list)
    monthly_cr   = defaultdict(float)
    monthly_dr   = defaultdict(float)
    monthly_real = defaultdict(float)
    balances: List[float] = []
    month_min_bal: Dict[str, float] = {}
    emi_total    = 0.0
    bounces      = 0

    for t in transactions:
        ttype = t.get('type', '')
        amt   = _safe(t.get('amount', 0))
        lower = (t.get('desc') or '').lower()
        month = _month(t)
        bal   = t.get('balance')

        if bal is not None:
            bv = _safe(bal)
            balances.append(bv)
            if month not in month_min_bal:
                month_min_bal[month] = bv
            else:
                month_min_bal[month] = min(month_min_bal[month], bv)

        # CDM / Cash Deposit — detect by description regardless of type field.
        # Kotak CDM: "Cash Deposit at/BGHBH121/..." — some parsers may output
        # wrong type field; catch by desc so cash deposits are never missed.
        is_cdm = _kw(lower, _CASH_DEP_KW)

        if ttype == 'CR' and amt > 0:
            cls = _classify_credit(lower, amt)
            buckets[cls].append(t)
            monthly_cr[month] += amt
            if cls in _TAXABLE:
                monthly_real[month] += amt

        elif ttype == 'DR' and amt > 0:
            if is_cdm:
                # CDM appeared as DR — re-route to cash deposit bucket
                buckets[CC.CASH_DEP].append(t)
                monthly_cr[month] += amt  # economically a credit
            else:
                buckets['_dr'].append(t)
                monthly_dr[month] += amt
                # #3 — EMI: only recurring loan obligations
                if _kw(lower, _EMI_KW):
                    emi_total += amt
                # Bounce detection
                if _kw(lower, _BOUNCE_KW):
                    bounces += 1

        elif amt > 0 and is_cdm:
            # type field blank/missing — description says cash deposit
            buckets[CC.CASH_DEP].append(t)
            monthly_cr[month] += amt

    # ── Pass 2: aggregate ──────────────────────────────────────
    def _tot(cls_key: str) -> float:
        return round(sum(_safe(t.get('amount', 0)) for t in buckets.get(cls_key, [])), 2)

    e.salary_total    = _tot(CC.SALARY)
    e.business_total  = _tot(CC.BUSINESS)
    e.interest_total  = _tot(CC.INTEREST)
    e.dividend_total  = _tot(CC.DIVIDEND)
    e.rental_total    = _tot(CC.RENTAL)

    # #2 — real income = EXACT sum, no ghost income
    e.real_income_total = round(
        e.salary_total + e.business_total + e.interest_total +
        e.dividend_total + e.rental_total, 2
    )

    e.loan_disbursal_total  = _tot(CC.LOAN)
    e.family_transfer_total = _tot(CC.FAMILY)
    e.self_transfer_total   = _tot(CC.SELF)
    e.refund_total          = _tot(CC.REFUND)
    e.cash_dep_total        = _tot(CC.CASH_DEP)
    e.needs_ver_total       = _tot(CC.NEEDS_VER)

    e.total_cr = round(sum(monthly_cr.values()), 2)
    e.total_dr = round(sum(monthly_dr.values()), 2)

    # ── Pass 3: monthly aggregates ─────────────────────────────
    months = sorted(set(list(monthly_cr.keys()) + list(monthly_dr.keys())))
    e.n_months = max(len(months), 1)
    e.monthly_real_income = {m: round(monthly_real.get(m, 0), 2) for m in months}

    e.avg_monthly_credit     = round(_div(e.total_cr, e.n_months), 2)
    e.avg_monthly_real_income= round(_div(e.real_income_total, e.n_months), 2)
    e.avg_monthly_debit      = round(_div(e.total_dr, e.n_months), 2)

    # ── Pass 4: balance KPIs ───────────────────────────────────
    if balances:
        e.avg_balance = round(_div(sum(balances), len(balances)), 2)
        e.min_balance = round(min(balances), 2)
        e.max_balance = round(max(balances), 2)
    e.negative_months = sum(1 for v in month_min_bal.values() if v < 0)

    # ── Pass 5: FOIR (#1 — single central calculation) ─────────
    monthly_emi = round(_div(emi_total, e.n_months), 2)
    e.monthly_fixed_obligations = monthly_emi

    base_income = e.avg_monthly_real_income if e.avg_monthly_real_income > 10 else e.avg_monthly_credit

    # #1 — FOIR formula (central, used everywhere)
    e.foir = round(_pct(monthly_emi, base_income), 1) if base_income > 0 else 0.0
    e.foir_status = ('Healthy' if e.foir <= C.FOIR_HEALTHY else
                     'Moderate' if e.foir <= C.FOIR_MODERATE else 'High Risk')

    # ── Pass 6: Loan eligibility (#6) ─────────────────────────
    e.max_eligible_emi    = round(base_income * C.FOIR_CAP / 100, 2)
    e.remaining_emi_capacity = round(max(0.0, e.max_eligible_emi - monthly_emi), 2)
    e.loan_eligible = _eligible_principal(
        e.remaining_emi_capacity, C.INTEREST_RATE, C.TENURE_MONTHS
    )

    # ── Pass 7: Income breakdown (must sum exactly) ────────────
    bd = {}
    if e.salary_total   > 0: bd['Salary / Payroll']     = e.salary_total
    if e.business_total > 0: bd['Business / Freelance'] = e.business_total
    if e.interest_total > 0: bd['Interest Income']       = e.interest_total
    if e.dividend_total > 0: bd['Dividend Income']       = e.dividend_total
    if e.rental_total   > 0: bd['Rental Income']         = e.rental_total
    # Fix float drift silently
    drift = e.real_income_total - round(sum(bd.values()), 2)
    if abs(drift) > 0.01 and bd:
        largest = max(bd, key=bd.get)
        bd[largest] = round(bd[largest] + drift, 2)
    e.real_income_breakdown = bd

    ni = {}
    if e.loan_disbursal_total  > 0: ni['Loan Disbursals']    = e.loan_disbursal_total
    if e.self_transfer_total   > 0: ni['Self Transfers']      = e.self_transfer_total
    if e.family_transfer_total > 0: ni['Family Transfers']    = e.family_transfer_total
    if e.refund_total          > 0: ni['Refunds / Reversals'] = e.refund_total
    if e.cash_dep_total        > 0: ni['Cash Deposits']       = e.cash_dep_total
    if e.needs_ver_total       > 0: ni['Needs Verification']  = e.needs_ver_total
    e.non_income_credits = ni

    # ── Pass 8: Bounce count ───────────────────────────────────
    e.bounce_count = bounces

    # ── Pass 9: Reconciliation score ──────────────────────────
    # (detailed mismatch walk done later; approximate here)
    score = 100 - min(bounces * 10, 30)
    e.reconciliation_score = max(0, score)

    # ── Pass 10: Income stability ──────────────────────────────
    e.income_stability, e.stability_color, e.stability_cv = _calc_stability(
        e.monthly_real_income
    )

    # ── Pass 11: Loan dependency ───────────────────────────────
    dep_ratio = _pct(e.loan_disbursal_total,
                     e.real_income_total if e.real_income_total > 0 else e.total_cr)
    e.loan_dep_pct   = round(dep_ratio, 1)
    n_loan_txns      = len(buckets.get(CC.LOAN, []))
    e.loan_dep_level = ('Low'    if dep_ratio < 20 and n_loan_txns <= 1 else
                        'High'   if dep_ratio >= 50 or n_loan_txns >= 5 else
                        'Medium')
    e.loan_dep_color = {'Low': 'green', 'Medium': 'yellow', 'High': 'red'}[e.loan_dep_level]

    # ── Pass 12: Credit profile (#3) ───────────────────────────
    reasons = []
    if e.foir == 0:
        reasons.append('No EMI obligations — clean repayment slate')
    elif e.foir <= C.FOIR_HEALTHY:
        reasons.append(f'FOIR {e.foir}% is within healthy range (≤{C.FOIR_HEALTHY:.0f}%)')
    elif e.foir <= C.FOIR_MODERATE:
        reasons.append(f'FOIR {e.foir}% is moderate — banks prefer below 40%')
    else:
        reasons.append(f'FOIR {e.foir}% exceeds {C.FOIR_MODERATE:.0f}% — high burden')

    if e.negative_months == 0:
        reasons.append('No negative balance months — stable cash flow')
    else:
        reasons.append(f'{e.negative_months} negative balance month(s)')
    if e.avg_balance >= base_income * C.BAL_GOOD_RATIO:
        reasons.append(f'Avg balance Rs{e.avg_balance:,.0f} is healthy vs income')
    elif e.avg_balance > 0:
        reasons.append(f'Avg balance Rs{e.avg_balance:,.0f} is moderate vs income')
    else:
        reasons.append('Average balance is very low')
    if monthly_emi > 0:
        reasons.append(f'Monthly EMIs: Rs{monthly_emi:,.0f}')
    else:
        reasons.append('No recurring EMIs detected')

    if e.negative_months == 0 and e.foir <= C.FOIR_HEALTHY and e.avg_balance >= base_income * C.BAL_GOOD_RATIO:
        e.credit_indicator = 'Strong';             e.credit_color = 'green'
        e.credit_summary   = 'Stable income, low obligations, healthy balance.'
    elif e.negative_months <= 2 and e.foir <= C.FOIR_MODERATE:
        e.credit_indicator = 'Moderate';           e.credit_color = 'yellow'
        e.credit_summary   = 'Some financial stress indicators — review recommended.'
    else:
        e.credit_indicator = 'Needs Improvement';  e.credit_color = 'red'
        e.credit_summary   = 'High obligations or irregular income detected.'
    e.credit_reasons = reasons

    # ── Pass 13: Disposable income ────────────────────────────
    e.variable_expenses  = max(0.0, e.avg_monthly_debit - monthly_emi)
    e.disposable_income  = round(base_income - monthly_emi - e.variable_expenses, 2)
    e.savings_rate_pct   = round(_pct(e.disposable_income, base_income), 1) if base_income > 0 else 0.0

    # ── Pass 14: Risk Engine (#4 — UNIFIED) ────────────────────
    risk_flags, risk_reasons = _run_risk_engine(
        transactions, buckets, e, monthly_dr, base_income
    )
    e.risk_flags   = risk_flags
    e.risk_reasons = risk_reasons
    # Risk score: start 100, penalty per flag severity
    rscore = 100
    sev_pen = {'High': 20, 'Medium': 10, 'Low': 4}
    for f in risk_flags:
        rscore -= sev_pen.get(f.get('severity', 'Low'), 4)
    e.risk_score = max(0, min(100, 100 - rscore))  # invert: higher = worse
    e.risk_level = ('High'   if e.risk_score >= 50 else
                    'Medium' if e.risk_score >= 20 else 'Low')
    e.risk_color = {'Low': 'green', 'Medium': 'yellow', 'High': 'red'}[e.risk_level]

    # ── Pass 15: Health score (#6) ─────────────────────────────
    e.health_score, e.health_grade, e.health_color = _calc_health(e)

    return e


# ══════════════════════════════════════════════════════════════
#  §7  STABILITY HELPER (used only by engine)
# ══════════════════════════════════════════════════════════════

def _calc_stability(monthly_real: dict):
    if not monthly_real:
        return 'Unknown', 'yellow', 0.0
    vals = [_safe(v) for v in monthly_real.values()]
    n    = len(vals)
    avg  = _div(sum(vals), n)
    if avg <= 0:
        return 'Volatile', 'red', 100.0
    # Trim top outlier for seasonal business (#7)
    trimmed = sorted(vals)[:-1] if n > 3 else sorted(vals)
    t_avg   = _div(sum(trimmed), len(trimmed))
    var     = _div(sum((v - t_avg) ** 2 for v in trimmed), len(trimmed))
    std     = math.sqrt(var)
    cv      = round(_pct(std, t_avg), 1)
    zero    = sum(1 for v in vals if v <= avg * 0.1)
    cv      = min(cv + zero * 15, 100.0)
    if cv < C.CV_STABLE:   return 'Stable',            'green',  cv
    if cv < C.CV_MODERATE: return 'Moderately Stable', 'yellow', cv
    return 'Volatile', 'red', cv


# ══════════════════════════════════════════════════════════════
#  §8  UNIFIED RISK ENGINE (#4)
#  Single function generates all risk flags, score and level.
#  Every section that shows risk data uses e.risk_* from engine.
# ══════════════════════════════════════════════════════════════

def _run_risk_engine(transactions: list, buckets: dict,
                     e: EngineResult, monthly_dr: dict,
                     base_income: float):
    """
    #4 — ONE risk engine, ONE set of flags/reasons.
    Underwriting alerts, risk flags and fraud section all use e.risk_*.
    No contradictions possible.
    """
    flags   = []
    reasons = []

    # 1. Frequent loan disbursals
    n_loan = len(buckets.get(CC.LOAN, []))
    if n_loan >= C.LOAN_DISBURSAL_FLAG:
        flags.append({'key': 'FREQUENT_LOAN_DISBURSALS', 'severity': 'High',
                      'label': 'Frequent Loan Disbursals',
                      'detail': f'{n_loan} disbursals totalling Rs{e.loan_disbursal_total:,.0f}.'})
        reasons.append(f'Frequent loans ({n_loan})')

    # 2. High family transfers
    fam_pct = _pct(e.family_transfer_total, e.total_cr)
    fam_cnt = len(buckets.get(CC.FAMILY, []))
    if fam_pct > C.FAMILY_XFER_PCT or fam_cnt >= 4:
        flags.append({'key': 'HIGH_FAMILY_TRANSFERS', 'severity': 'Medium',
                      'label': 'High Family Transfers',
                      'detail': f'{fam_cnt} transfers Rs{e.family_transfer_total:,.0f} ({fam_pct:.0f}%)'})
        reasons.append(f'High family transfers ({fam_pct:.0f}%)')

    # 3. Heavy cash deposits
    if e.cash_dep_total > 0 or len(buckets.get(CC.CASH_DEP, [])) >= 1:
        flags.append({'key': 'HEAVY_CASH_DEPOSITS', 'severity': 'Medium',
                      'label': 'Heavy Cash Deposits',
                      'detail': f'Rs{e.cash_dep_total:,.0f} cash deposits — AML attention.'})
        reasons.append('Heavy cash deposits')

    # 4. Low average balance
    if base_income > 0 and e.avg_balance < base_income * C.LOW_BAL_FLAG_RATIO:
        flags.append({'key': 'LOW_AVERAGE_BALANCE', 'severity': 'High',
                      'label': 'Low Average Balance',
                      'detail': f'Avg Rs{e.avg_balance:,.0f} < 20% of income Rs{base_income:,.0f}.'})
        reasons.append('Low average balance')

    # 5. Cheque bounces
    if e.bounce_count >= 1:
        flags.append({'key': 'CHEQUE_BOUNCES', 'severity': 'High',
                      'label': 'Cheque / ECS Bounces',
                      'detail': f'{e.bounce_count} bounce(s) — strong negative credit signal.'})
        reasons.append(f'{e.bounce_count} bounce(s)')

    # 6. Negative balance days
    if e.negative_balance_days >= 1:
        flags.append({'key': 'NEGATIVE_BALANCE_DAYS', 'severity': 'High',
                      'label': 'Negative Balance Days',
                      'detail': f'{e.negative_balance_days} day(s) with negative balance.'})
        reasons.append(f'Neg. balance {e.negative_balance_days} day(s)')

    # 7. Spending spike (min 3 months data)
    if len(monthly_dr) >= 3:
        avg_dr = _div(sum(monthly_dr.values()), len(monthly_dr))
        spikes = [m for m, v in monthly_dr.items() if v > avg_dr * C.SPENDING_SPIKE_MULT]
        if spikes:
            flags.append({'key': 'SPENDING_SPIKE', 'severity': 'Medium',
                          'label': 'Unusual Spending Spike',
                          'detail': f'Spending >2.5× average in: {", ".join(spikes)}.'})
            reasons.append(f'Spending spike in {len(spikes)} month(s)')

    # 8. Large unverified credits
    nv_pct = _pct(e.needs_ver_total, e.total_cr)
    if nv_pct > C.UNVERIFIED_FLAG_PCT:
        flags.append({'key': 'LARGE_UNVERIFIED_CREDITS', 'severity': 'Medium',
                      'label': 'Large Unverified Credits',
                      'detail': f'Rs{e.needs_ver_total:,.0f} ({nv_pct:.0f}%) unclassified — verify source.'})
        reasons.append(f'Unverified credits {nv_pct:.0f}%')

    if not reasons:
        reasons.append('No significant risk triggers detected.')

    return flags, reasons


# ══════════════════════════════════════════════════════════════
#  §9  HEALTH SCORE HELPER (reads engine, no recompute)
# ══════════════════════════════════════════════════════════════

def _calc_health(e: EngineResult):
    """Penalty-based score (starts 100). (#6)"""
    p = 0
    p += C.STABILITY_PENALTY.get(e.income_stability, 15)
    p += C.step_lookup([(b[0], b[1]) for b in C.BAL_RATIO_PENALTY],
                       _div(e.avg_balance, e.avg_monthly_real_income or 1))
    p += C.step_lookup([(b[0], b[1]) for b in C.NEG_DAYS_PENALTY],
                       e.negative_balance_days)
    p += C.step_lookup([(b[0], b[1]) for b in C.FOIR_PENALTY_BREAKS], e.foir)
    p += C.DEP_PENALTY.get(e.loan_dep_level, 6)
    p += C.step_lookup([(b[0], b[1]) for b in C.BOUNCE_PENALTY], e.bounce_count)
    score = max(0, min(100, 100 - p))
    if score >= 80:   grade, col = 'Excellent', 'green'
    elif score >= 65: grade, col = 'Good',      'green'
    elif score >= 45: grade, col = 'Average',   'yellow'
    else:             grade, col = 'Poor',       'red'
    return score, grade, col


# ══════════════════════════════════════════════════════════════
#  §10  PRE-REPORT VALIDATION PASS (#7 #8 #12)
# ══════════════════════════════════════════════════════════════

@dataclass
class ValidationResult:
    passed:   bool  = True
    errors:   List  = field(default_factory=list)
    warnings: List  = field(default_factory=list)


def _validate(e: EngineResult, income_data: dict, loan_data: dict,
              audit_data: dict) -> ValidationResult:
    """
    #7 #8 #12 — Pre-export consistency check.
    Verifies that every section agrees with the engine.
    Auto-corrects minor float drift; logs errors for contradictions.
    """
    vr = ValidationResult()

    # ── 1. Income consistency (#3)
    itr_real  = _safe(income_data.get('real_income_total', 0))
    loan_real = _safe(loan_data.get('avg_monthly_real_income', 0)) * e.n_months
    if abs(itr_real - e.real_income_total) > 1.0:
        vr.errors.append(
            f'Income mismatch: engine={e.real_income_total}, itr={itr_real}')
        vr.passed = False

    # ── 2. FOIR consistency (#1)
    loan_foir = _safe(loan_data.get('foir', 0))
    if abs(loan_foir - e.foir) > 0.5:
        vr.errors.append(f'FOIR mismatch: engine={e.foir}, loan_module={loan_foir}')
        vr.passed = False

    # ── 3. EMI consistency (#1)
    loan_emi = _safe(loan_data.get('monthly_fixed_obligations', 0))
    audit_emi_outflow = _safe(audit_data.get('total_emi_outflow', 0))
    n = e.n_months
    audit_monthly_emi = round(_div(audit_emi_outflow, n), 2)
    if audit_emi_outflow > 0 and abs(audit_monthly_emi - e.monthly_fixed_obligations) > 50:
        vr.warnings.append(
            f'EMI minor diff: engine={e.monthly_fixed_obligations}, audit_monthly={audit_monthly_emi}')

    # ── 4. Monthly totals reconcile (#8)
    real_monthly_sum = round(sum(e.monthly_real_income.values()), 2)
    if abs(real_monthly_sum - e.real_income_total) > 1.0:
        vr.errors.append(
            f'Monthly income sum mismatch: monthly_sum={real_monthly_sum}, total={e.real_income_total}')
        vr.passed = False

    # ── 5. Balance breakdown (#8)
    if e.total_cr > 0 and e.total_dr > 0:
        bd_sum = round(
            e.real_income_total + e.loan_disbursal_total + e.family_transfer_total +
            e.self_transfer_total + e.refund_total + e.cash_dep_total + e.needs_ver_total, 2
        )
        # bd_sum should be close to total_cr (other buckets: non_income, reimb not in engine)
        if bd_sum > e.total_cr * 1.05:
            vr.warnings.append(f'Credit bucket sum {bd_sum} > total_cr {e.total_cr}')

    # ── 6. Impossible percentages (#7)
    if not (0 <= e.foir <= 200):
        vr.errors.append(f'FOIR out of range: {e.foir}%')
        vr.passed = False
    if not (0 <= e.loan_dep_pct <= 100):
        vr.errors.append(f'Loan dep pct out of range: {e.loan_dep_pct}%')
        vr.passed = False

    # ── 7. Loan eligibility sanity (#6)
    if e.loan_eligible < 0:
        vr.errors.append(f'Negative loan eligibility: {e.loan_eligible}')
        vr.passed = False
    if e.remaining_emi_capacity < 0:
        vr.errors.append(f'Negative EMI capacity: {e.remaining_emi_capacity}')
        vr.passed = False

    return vr


# ══════════════════════════════════════════════════════════════
#  §11  MODULE REPORTERS  (read engine — never recalculate)
# ══════════════════════════════════════════════════════════════

def _build_income(transactions: list, buckets: dict, e: EngineResult) -> dict:
    """Reads engine for all totals. #2 #3"""
    salary_txns = buckets.get(CC.SALARY, [])
    biz_txns    = buckets.get(CC.BUSINESS, [])
    int_txns    = buckets.get(CC.INTEREST, [])
    div_txns    = buckets.get(CC.DIVIDEND, [])
    rent_txns   = buckets.get(CC.RENTAL, [])
    loan_txns   = buckets.get(CC.LOAN, [])
    self_txns   = buckets.get(CC.SELF, [])
    fam_txns    = buckets.get(CC.FAMILY, [])
    ref_txns    = buckets.get(CC.REFUND, [])
    cdep_txns   = buckets.get(CC.CASH_DEP, [])
    nv_txns     = buckets.get(CC.NEEDS_VER, [])
    reimb_txns  = buckets.get(CC.REIMBURSE, [])

    employer_names = set()
    sender_amt  = defaultdict(float)
    sender_cnt  = defaultdict(int)
    for t in transactions:
        if t.get('type') != 'CR' or not t.get('amount'): continue
        desc   = t.get('desc') or ''
        lower  = desc.lower()
        amt    = _safe(t.get('amount', 0))
        cls    = _classify_credit(lower, amt)
        if cls == CC.SALARY:
            emp = _extract_employer(desc)
            if emp: employer_names.add(emp)
        sender = _extract_employer(desc) or (
            next((w for w in desc.split() if len(w) > 2 and not w.isdigit()), 'Unknown').title()
        )
        sender_amt[sender] += amt
        sender_cnt[sender] += 1

    top_sources = sorted(
        [{'sender': s, 'total_amt': round(a, 2), 'count': sender_cnt[s],
          'avg_amt': round(_div(a, sender_cnt[s]), 2)}
         for s, a in sender_amt.items()],
        key=lambda x: -x['total_amt']
    )[:10]

    return {
        # Txn lists
        'salary_txns': salary_txns, 'business_txns': biz_txns,
        'interest_txns': int_txns, 'dividend_txns': div_txns,
        'rent_txns': rent_txns, 'loan_disbursal_txns': loan_txns,
        'self_transfer_txns': self_txns, 'family_transfer_txns': fam_txns,
        'refund_txns': ref_txns, 'needs_verification_txns': nv_txns,
        # All totals from engine (#2 — single source)
        'salary_total': e.salary_total, 'business_total': e.business_total,
        'interest_total': e.interest_total, 'dividend_total': e.dividend_total,
        'rent_total': e.rental_total, 'rental_total': e.rental_total, 'rental_txns': rent_txns,
        'loan_disbursal_total': e.loan_disbursal_total,
        'family_transfer_total': e.family_transfer_total,
        'self_transfer_total': e.self_transfer_total,
        'refund_total': e.refund_total,
        'cash_dep_total': e.cash_dep_total,
        'needs_verification_total': e.needs_ver_total,
        'real_income_total': e.real_income_total,      # #3 — same everywhere
        'real_income_breakdown': e.real_income_breakdown,
        'non_income_credits': e.non_income_credits,
        'monthly_real_income': e.monthly_real_income,
        'employer_names': sorted(employer_names),
        'top_10_sources': top_sources,
        # Legacy keys
        'freelance_total': e.business_total, 'marketplace_total': 0,
        'reversal_total': e.refund_total,
        'cash_dep_count': len(cdep_txns), 'salary_count': len(salary_txns),
        'business_count': len(biz_txns),
        'cash_flag_10L': e.cash_dep_total >= 1_000_000,
        'high_single_cash': [t for t in cdep_txns if _safe(t.get('amount', 0)) >= 200_000],
        'missing_salary_months': [], 'irregular_months': [],
        'monthly_salary': {}, 'monthly_business': {}, 'monthly_cash_dep': {},
        'all_real_credits': e.real_income_total,
        'gst_applicable': round(e.business_total, 2),
        'estimated_gst': round(e.business_total * C.GST_RATE, 2),
        'gst_threshold_crossed': e.business_total >= 2_000_000,
        'income_breakdown': {
            'Salary': e.salary_total, 'Business': e.business_total,
            'Interest': e.interest_total, 'Dividend': e.dividend_total,
            'Rental': e.rental_total, 'Loan Received': e.loan_disbursal_total,
            'Family Transfer': e.family_transfer_total, 'Self Transfer': e.self_transfer_total,
            'Needs Verification': e.needs_ver_total,
        },
    }


def _build_loan(e: EngineResult, monthly_data: dict) -> dict:
    """Loan module — all values from engine (#1 #2 #6)."""
    foir_calc = {
        'monthly_real_income':        e.avg_monthly_real_income,
        'monthly_fixed_obligations':  e.monthly_fixed_obligations,
        'formula':                    'FOIR = (Monthly Fixed Obligations ÷ Monthly Real Income) × 100',
        'calculation':                (f'FOIR = (Rs{e.monthly_fixed_obligations:,.0f} ÷ '
                                       f'Rs{e.avg_monthly_real_income:,.0f}) × 100 = {e.foir}%'),
        'foir_pct':                   e.foir,          # #1 — same value everywhere
        'foir_cap_pct':               C.FOIR_CAP,
        'max_eligible_emi':           e.max_eligible_emi,
        'remaining_emi_capacity':     e.remaining_emi_capacity,
        'status':                     e.foir_status,
        'note':                       'FOIR includes only recurring EMI/loan repayments.',
        'interpretation': (
            f'Monthly Real Income: Rs{e.avg_monthly_real_income:,.0f} | '
            f'Monthly EMIs: Rs{e.monthly_fixed_obligations:,.0f} | '
            f'FOIR: {e.foir}% ({e.foir_status}) | '
            f'Loan eligible: Rs{e.loan_eligible:,.0f} at '
            f'{C.INTEREST_RATE*100:.1f}% over {C.TENURE_MONTHS}m.'
        ),
    }

    dscr = round(_div(e.avg_monthly_real_income, e.monthly_fixed_obligations), 2) \
           if e.monthly_fixed_obligations > 0 else None

    return {
        'monthly_data':              monthly_data,
        'avg_monthly_credit':        e.avg_monthly_credit,
        'avg_monthly_real_income':   e.avg_monthly_real_income,
        'avg_monthly_debit':         e.avg_monthly_debit,
        'avg_balance':               e.avg_balance,
        'min_balance':               e.min_balance,
        'max_balance':               e.max_balance,
        'monthly_emi':               e.monthly_fixed_obligations,    # #1 single value
        'monthly_fixed_obligations': e.monthly_fixed_obligations,    # #1 single value
        'foir':                      e.foir,                         # #1 single value
        'dscr':                      dscr,
        'loan_eligible':             e.loan_eligible,
        'remaining_emi_capacity':    e.remaining_emi_capacity,
        'negative_months':           e.negative_months,
        'credit_indicator':          e.credit_indicator,
        'credit_color':              e.credit_color,
        'credit_summary':            e.credit_summary,
        'credit_reasons':            e.credit_reasons,
        'foir_calculation':          foir_calc,
        'months_analyzed':           e.n_months,
        'emis_detected':             [],
        'assumptions': {
            'foir_cap_pct':      C.FOIR_CAP,
            'interest_rate_pct': C.INTEREST_RATE * 100,
            'tenure_months':     C.TENURE_MONTHS,
        },
    }


def _build_audit(transactions: list, e: EngineResult) -> dict:
    """Balance validation (#8 #12)."""
    sorted_txns = sorted(transactions, key=lambda t: (t.get('date', ''), transactions.index(t)))
    mismatches  = []
    prev        = None

    for curr in sorted_txns:
        bal  = curr.get('balance')
        amt  = _safe(curr.get('amount', 0))
        ttyp = curr.get('type', '')
        if bal is None or amt == 0: prev = curr; continue
        if prev is not None and prev.get('balance') is not None:
            prev_date = prev.get('date', ''); curr_date = curr.get('date', '')
            if prev_date and curr_date and prev_date != curr_date:
                pd = _parse_date(prev_date); cd = _parse_date(curr_date)
                if pd and cd and (cd - pd).days > 1:
                    prev = curr; continue
            expected = round(_safe(prev['balance']) + (amt if ttyp == 'CR' else -amt), 2)
            if abs(expected - round(_safe(bal), 2)) > 2.0:
                mismatches.append({**curr, 'expected_balance': expected,
                                   'diff': abs(expected - round(_safe(bal), 2))})
        prev = curr

    # Duplicate detection (#5 — prevent duplicate rows in reports)
    seen: Dict = defaultdict(list)
    for t in transactions:
        key = (round(_safe(t.get('amount', 0)), 0), t.get('date', ''), t.get('type', ''))
        seen[key].append(t)
    true_dups = []
    for key, group in seen.items():
        if len(group) < 2: continue
        descs = [(t.get('desc') or '') for t in group]
        months_set = set(_month(t) for t in group)
        if len(months_set) >= 2: continue  # recurring, not duplicate
        for i in range(len(descs)):
            for j in range(i + 1, len(descs)):
                ratio = SequenceMatcher(None, descs[i][:30].lower(),
                                        descs[j][:30].lower()).ratio()
                if ratio > C.DUP_DESC_SIMILARITY:
                    true_dups.extend(group); break

    emi_txns   = [t for t in transactions if t.get('type') == 'DR'
                  and _kw((t.get('desc') or '').lower(), _EMI_KW)]
    bounced_txns = [t for t in transactions if _kw((t.get('desc') or '').lower(), _BOUNCE_KW)]

    score = 100 - min(len(mismatches) * 5, 30) - min(e.bounce_count * 10, 30) - \
            min(len(set(id(t) for t in true_dups)) * 3, 15)

    total_emi_outflow = round(sum(_safe(t.get('amount', 0)) for t in emi_txns), 2)

    return {
        'balance_mismatches': mismatches,
        'mismatch_count':     len(mismatches),
        'bounced':            bounced_txns,
        'bounce_count':       e.bounce_count,    # from engine
        'emis':               emi_txns,
        'emi_count':          len(emi_txns),
        'total_emi_outflow':  total_emi_outflow,
        'emi_to_income_ratio':round(_pct(total_emi_outflow, e.total_cr), 1),
        'duplicate_suspects': list({id(t): t for t in true_dups}.values())[:20],
        'duplicate_count':    len(set(id(t) for t in true_dups)),
        'reconciliation_score': max(0, score),
        # Legacy
        'cheques': [t for t in transactions if _kw((t.get('desc') or '').lower(),
                    ['chq dep', 'cheque', 'clearing chq', 'cts/', 'micr'])],
        'cheque_count': 0,
    }


def _build_risk_section(e: EngineResult, red_flags: dict) -> dict:
    """
    #4 — Unified risk. Reads from engine, never recalculates.
    Merges fraud/underwriting/risk into ONE consistent view.
    """
    # Fraud-level risk score from red_flags
    fraud_score = _safe(red_flags.get('flag_score', 0))

    # Combined score: max of underwriting risk and fraud risk
    combined_score = max(e.risk_score, round(fraud_score * 0.5))
    combined_level = ('High'   if combined_score >= 50 else
                      'Medium' if combined_score >= 20 else 'Low')
    combined_color = {'Low': 'green', 'Medium': 'yellow', 'High': 'red'}[combined_level]

    # Ensure fraud level never contradicts combined level
    fraud_level = red_flags.get('flag_level', 'Low')
    if fraud_level == 'High' and combined_level == 'Low':
        combined_level = 'High'; combined_color = 'red'

    return {
        'risk_flags':   e.risk_flags,        # from engine
        'flag_count':   len(e.risk_flags),
        'has_high':     any(f['severity'] == 'High' for f in e.risk_flags),
        'has_medium':   any(f['severity'] == 'Medium' for f in e.risk_flags),
        'overall_risk': combined_level,       # #4 — consistent
        'risk_score':   combined_score,
        'risk_color':   combined_color,
        'risk_reasons': e.risk_reasons,
    }


def _build_compliance_risk(e: EngineResult, compliance: dict) -> str:
    """#4 — Compliance risk explanation consistent with engine risk level."""
    rl = e.risk_level
    rs = e.risk_score
    ac = _safe(compliance.get('annual_cash_total', 0))
    reasons = e.risk_reasons
    if rl == 'Low':
        return (f'LOW compliance risk ({rs}/100). No major triggers. '
                f'Cash total Rs{ac:,.0f} within Rs10L limit.')
    if rl == 'Medium':
        return (f'MEDIUM compliance risk ({rs}/100). '
                f'Triggers: {"; ".join(reasons[:2])}. Enhanced due diligence recommended.')
    return (f'HIGH compliance risk ({rs}/100). '
            f'Triggers: {"; ".join(reasons[:3])}. Immediate review required before sanction.')


def _build_expenses(transactions: list) -> dict:
    """#5 #9 — UPI never a category. No duplicate category assignment."""
    cat_tot  = defaultdict(float)
    cat_txns = defaultdict(list)
    merch_sp = defaultdict(float)
    merch_cnt= defaultdict(int)
    seen_ids = set()  # #5 — prevent duplicate rows

    for t in transactions:
        if t.get('type') != 'DR' or not t.get('amount'): continue
        tid = id(t)
        if tid in seen_ids: continue  # #5
        seen_ids.add(tid)

        desc = t.get('desc') or ''
        amt  = _safe(t.get('amount', 0))
        lower = desc.lower()

        # #9 — Loan disbursals never appear as expenses
        if is_loan_disbursal(lower): continue

        cat = _categorize_expense(desc, t.get('category', ''))
        cat_tot[cat]   += amt
        cat_txns[cat].append(t)

        m = _extract_merchant(desc)
        if m not in ('Unknown', 'Other'):
            merch_sp[m]  += amt
            merch_cnt[m] += 1

    total_dr = sum(cat_tot.values())
    emi_tot  = cat_tot.get('EMI / Loan Repayment', 0)

    top_merchants = sorted(
        [{'merchant': m, 'total': round(v, 2), 'count': merch_cnt[m],
          'avg': round(_div(v, merch_cnt[m]), 2)}
         for m, v in merch_sp.items()],
        key=lambda x: -x['total']
    )[:15]

    sorted_cats = dict(sorted(cat_tot.items(), key=lambda x: -x[1]))

    return {
        'category_totals': sorted_cats,
        'category_txns':   dict(cat_txns),
        'total_debits':    round(total_dr, 2),
        'emi_total':       round(emi_tot, 2),
        'opex_total':      round(total_dr - emi_tot, 2),
        'top_merchants':   top_merchants,
        'business_total':  round(emi_tot, 2),
        'personal_total':  round(total_dr - emi_tot, 2),
        'mixed_total': 0, 'mixed_pct': 0,
        'business_pct':    round(_pct(emi_tot, total_dr), 1),
        'personal_pct':    round(_pct(total_dr - emi_tot, total_dr), 1),
        'gst_eligible': [], 'gst_eligible_total': 0,
        'business': cat_txns.get('EMI / Loan Repayment', []),
        'personal': [t for k, v in cat_txns.items()
                     for t in v if k != 'EMI / Loan Repayment'],
        'mixed': [],
    }


def _build_cashflow(transactions: list) -> dict:
    monthly: Dict = defaultdict(lambda: {'credits': 0.0, 'debits': 0.0,
                                          'balances': [], 'eom': None})
    for t in sorted(transactions, key=lambda x: x.get('date', '')):
        month = _month(t); amt = _safe(t.get('amount', 0)); bal = t.get('balance')
        if t.get('type') == 'CR': monthly[month]['credits'] += amt
        else:                     monthly[month]['debits']  += amt
        if bal is not None:       monthly[month]['balances'].append(_safe(bal))
    for m in monthly:
        b = monthly[m]['balances']
        if b: monthly[m]['eom'] = b[-1]
    months = sorted(monthly.keys()); n = len(months)
    all_b  = [b for m in months for b in monthly[m]['balances']]

    def abb(k):
        recent = months[-k:] if len(months) >= k else months
        bals   = [b for m in recent for b in monthly[m]['balances']]
        return round(_div(sum(bals), len(bals)), 2) if bals else 0

    eom = [{'month': m, 'balance': round(monthly[m]['eom'], 2)}
           for m in months if monthly[m]['eom'] is not None]

    if len(eom) >= 4:
        h1 = eom[:len(eom)//2]; h2 = eom[len(eom)//2:]
        a1 = _div(sum(e['balance'] for e in h1), len(h1))
        a2 = _div(sum(e['balance'] for e in h2), len(h2))
        trend = ('Improving ↑' if a2 > a1 * 1.1 else 'Declining ↓' if a2 < a1 * 0.9 else 'Stable →')
    else:
        trend = 'Stable →' if len(eom) >= 2 else 'Insufficient data'

    nets = []
    for m in months:
        cr = round(monthly[m]['credits'], 2); dr = round(monthly[m]['debits'], 2)
        nets.append({'month': m, 'credits': cr, 'debits': dr,
                     'net': round(cr - dr, 2), 'surplus': cr >= dr})

    mc = {m: monthly[m]['credits'] for m in months}
    avg_cr = _div(sum(mc.values()), len(mc)) if mc else 0
    return {
        'abb_3m': abb(3), 'abb_6m': abb(6), 'abb_12m': abb(12),
        'abb_overall': round(_div(sum(all_b), len(all_b)), 2) if all_b else 0,
        'eom_trend': eom, 'trend_direction': trend, 'monthly_net': nets,
        'surplus_months': sum(1 for x in nets if x['surplus']),
        'deficit_months': sum(1 for x in nets if not x['surplus']),
        'avg_net_flow': round(_div(sum(x['net'] for x in nets), n), 2),
        'high_months': [m for m, v in mc.items() if v > avg_cr * 1.25],
        'low_months':  [m for m, v in mc.items() if 0 < v < avg_cr * 0.75],
        'monthly_credits': dict(sorted(mc.items())), 'months_analyzed': n,
    }


def _build_compliance(transactions: list) -> dict:
    hv = []; cash_t = []; struct = []; daily_cash = defaultdict(float)
    for t in transactions:
        amt = _safe(t.get('amount', 0)); lower = (t.get('desc') or '').lower()
        date = t.get('date', '')
        ttype = t.get('type', '')
        if amt >= C.HIGH_VALUE:      hv.append(t)
        is_cash = _kw(lower, _CASH_KW)
        is_cdm  = _kw(lower, _CASH_DEP_KW)
        if is_cash:
            cash_t.append(t)
            # Count cash deposits regardless of type field
            # CDM may appear as CR or (parser bug) as DR
            if is_cdm or ttype == 'CR':
                daily_cash[date] += amt
        if 180_000 <= amt < 200_000: struct.append(t)

    day_br = [{'date': d, 'amount': round(a, 2)}
              for d, a in daily_cash.items() if a > C.DAILY_CASH_LIMIT]
    ann_cash = sum(daily_cash.values())
    f61a = ann_cash >= C.ANNUAL_CASH_LIM

    dt = defaultdict(float)
    for t in transactions:
        if t.get('type') == 'CR': dt[t.get('date', '')] += _safe(t.get('amount', 0))
    str_c = [{'date': d, 'total': round(v, 2), 'count': 0}
             for d, v in dt.items() if v >= C.STR_THRESHOLD]

    return {
        'high_value_txns': hv[:20], 'cash_txns': cash_t[:20],
        'structured_suspects': struct[:10], 'str_candidates': str_c[:10],
        'daily_breaches': day_br, 'annual_cash_total': round(ann_cash, 2),
        'form_61a_required': f61a,
        'high_value_count': len(hv), 'cash_count': len(cash_t),
        'structured_count': len(struct), 'str_count': len(str_c),
        'round_figure_count': 0,
    }


def _build_red_flags(transactions: list) -> dict:
    txn_by_day: Dict = defaultdict(list)
    for t in transactions:
        d = _parse_date(t.get('date', ''))
        if d: txn_by_day[d].append(t)
    days = sorted(txn_by_day.keys()); circular = []; _seen: set = set()
    for i, d in enumerate(days):
        for t in txn_by_day[d]:
            if t.get('type') != 'CR': continue
            amt = _safe(t.get('amount', 0))
            if amt < 5_000: continue
            t_d = (t.get('desc') or '').lower()
            for j in range(i, min(i + 8, len(days))):
                d2 = days[j]
                for t2 in txn_by_day[d2]:
                    if t2.get('type') != 'DR': continue
                    if abs(_safe(t2.get('amount', 0)) - amt) >= 1.0: continue
                    sim = SequenceMatcher(None, t_d[:30],
                                          (t2.get('desc') or '').lower()[:30]).ratio()
                    if not (sim > 0.45 or (amt >= 50_000 and amt % 5_000 == 0)): continue
                    pk = (t.get('date'), t2.get('date'), round(amt, 0))
                    if pk in _seen: continue
                    _seen.add(pk)
                    circular.append({'credit_date': t.get('date'),
                                     'credit_desc': (t.get('desc') or '')[:50],
                                     'debit_date': t2.get('date'),
                                     'debit_desc': (t2.get('desc') or '')[:50],
                                     'amount': round(amt, 2), 'days_gap': (d2 - d).days})

    gamb_kw = ['dream11', 'my11circle', 'rummycircle', 'betting', 'gambling',
               'casino', 'lottery', 'binance', 'wazirx', 'coinswitch', 'coindcx',
               'bitcoin', 'ethereum', 'exness', 'forex trading', 'mpl gaming']
    gamb  = [t for t in transactions if _kw((t.get('desc') or '').lower(), gamb_kw)]
    pen   = [t for t in transactions if _kw((t.get('desc') or '').lower(),
             ['penalty charge', 'penal interest', 'late payment fee', 'overdue charge'])]

    score = min(len(circular)*15 + len(gamb)*10 + len(pen)*5, 100)
    lvl   = 'High' if score >= 50 else 'Medium' if score >= 20 else 'Low'
    return {
        'circular_txns': circular[:10], 'circular_count': len(circular),
        'window_dress': [], 'window_dress_count': 0,
        'gambling_txns': gamb[:20], 'gambling_count': len(gamb),
        'gambling_total': round(sum(_safe(t.get('amount', 0)) for t in gamb), 2),
        'penalty_txns': pen[:20], 'penalty_count': len(pen),
        'penalty_total': round(sum(_safe(t.get('amount', 0)) for t in pen), 2),
        'duplicate_txns': [], 'duplicate_count': 0, 'duplicate_groups': 0,
        'flag_score': score,
        'flag_level': lvl,
        'flag_color': 'red' if lvl=='High' else 'yellow' if lvl=='Medium' else 'green',
        'total_flags': len(circular) + len(gamb) + len(pen),
    }


def _build_itr(transactions: list, e: EngineResult, buckets: dict) -> dict:
    deductions: Dict = {'80C': [], '80D': [], 'tds_paid': []}
    hvc = []
    for t in transactions:
        lower = (t.get('desc') or '').lower(); amt = _safe(t.get('amount', 0))
        if t.get('type') == 'CR' and amt >= 100_000:
            hvc.append({**t, 'credit_type': _classify_credit(lower, amt)})
        if t.get('type') == 'DR' and amt > 0:
            if _kw(lower, _80C_KW) and not _kw(lower, _80C_EXCLUDE):
                deductions['80C'].append(t)
            elif _kw(lower, _80D_KW):
                deductions['80D'].append(t)
            elif _kw(lower, _TDS_KW):
                deductions['tds_paid'].append(t)
    s80c = min(sum(_safe(t.get('amount', 0)) for t in deductions['80C']), C.MAX_80C)
    s80d = min(sum(_safe(t.get('amount', 0)) for t in deductions['80D']), C.MAX_80D)

    # ITR form (#15)
    if e.business_total > 0:
        form = 'ITR-3 (Business / Freelance Income detected)'
        just = (f'ITR-3: business income Rs{e.business_total:,.0f} + salary Rs{e.salary_total:,.0f}. '
                f'PGBP head applies. Verify with CA.')
    elif e.rental_total > 0:
        form = 'ITR-2 (Rental / House Property Income)'
        just = f'ITR-2: rental income Rs{e.rental_total:,.0f}. Cannot file ITR-1. Confirm with CA.'
    elif e.salary_total > 0:
        form = 'ITR-1 — Sahaj (Salary Income only)'
        just = f'ITR-1: salary Rs{e.salary_total:,.0f}. Valid if total income ≤ Rs50L. Confirm with CA.'
    else:
        form = 'ITR-1 or ITR-2 — Verify with CA'
        just = (f'Income unclear. Real income: Rs{e.real_income_total:,.0f}. '
                f'Rs{e.needs_ver_total:,.0f} needs verification.')

    income_sources = {k: buckets.get(v, []) for k, v in {
        'salary': CC.SALARY, 'freelance': CC.BUSINESS, 'interest': CC.INTEREST,
        'dividend': CC.DIVIDEND, 'rental': CC.RENTAL, 'loan_disbursal': CC.LOAN,
        'family_transfer': CC.FAMILY, 'self_transfer': CC.SELF, 'reversal': CC.REFUND,
        'other_credits': CC.NEEDS_VER,
    }.items()}
    income_sources.update({'mb_transfer': [], 'marketplace': [], 'recurring_person': []})

    return {
        'income_sources': income_sources, 'deductions': deductions,
        'high_value_credits': hvc, 'high_value_count': len(hvc),
        'real_income_total':     e.real_income_total,    # #3 — from engine
        'salary_total':          e.salary_total,
        'freelance_total':       e.business_total,
        'marketplace_total':     0,
        'interest_total':        e.interest_total,
        'loan_disbursal_total':  e.loan_disbursal_total,
        'family_transfer_total': e.family_transfer_total,
        'self_transfer_total':   e.self_transfer_total,
        'reversal_total':        e.refund_total,
        'other_credits':         e.needs_ver_total,
        'section_80c_total': round(s80c, 2), 'section_80d_total': round(s80d, 2),
        'suggested_itr': form, 'itr_justification': just,
        'real_income_breakdown': e.real_income_breakdown,
        'non_income_credits':    e.non_income_credits,
        'income_breakdown':      {
            'Salary': e.salary_total, 'Freelance': e.business_total,
            'Interest': e.interest_total, 'Dividend': e.dividend_total,
            'Rental': e.rental_total, 'Loan Received': e.loan_disbursal_total,
            'Family Transfer': e.family_transfer_total, 'Self Transfer': e.self_transfer_total,
            'Needs Verification': e.needs_ver_total,
        },
    }


def _build_obligations(transactions: list) -> dict:
    emi_t=[]; bounce_t=[]; ecs_t=[]; cc_t=[]; mo_emi=defaultdict(float)
    lender=defaultdict(float); cc_m=defaultdict(list); dest=defaultdict(lambda:{'total':0.0,'count':0})
    EXCL={'atm','cash','self','own account'}
    for t in transactions:
        desc=t.get('desc') or ''; lower=desc.lower(); amt=_safe(t.get('amount',0)); month=_month(t)
        if _kw(lower, _BOUNCE_KW):
            if any(k in lower for k in ('ecs','nach','mandate','si ')): ecs_t.append(t)
            else: bounce_t.append(t)
        if t.get('type')!='DR' or amt==0: continue
        if _kw(lower, _EMI_KW):
            emi_t.append(t); mo_emi[month]+=amt
            kw=_kw_first(lower, _EMI_KW)
            if kw and len(kw)>4: lender[kw.title()]+=amt
        if _kw(lower, ['credit card','cc payment','cc bill','card bill','hdfc cc','sbi card','card dues']):
            cc_t.append(t); cc_m[month].append(amt)
        if not any(k in lower for k in EXCL):
            m=_extract_merchant(desc); dest[m]['total']+=amt; dest[m]['count']+=1
    cc_pat=[{'month':m,'total_paid':round(sum(p),2),'count':len(p),
              'pattern':'Multiple' if len(p)>1 else 'Single'} for m,p in sorted(cc_m.items())]
    top10=sorted([{'dest':d,'total':round(v['total'],2),'count':v['count'],
                   'avg':round(_div(v['total'],v['count']),2)} for d,v in dest.items()],
                 key=lambda x:-x['total'])[:10]
    return {
        'emi_txns':emi_t,'emi_count':len(emi_t),
        'total_emi_outflow':round(sum(_safe(t.get('amount',0)) for t in emi_t),2),
        'monthly_emi':dict(sorted(mo_emi.items())),
        'avg_monthly_emi':round(_div(sum(mo_emi.values()),len(mo_emi)),2) if mo_emi else 0,
        'lender_list':sorted([{'name':k,'total':round(v,2)} for k,v in lender.items()],
                              key=lambda x:-x['total'])[:8],
        'bounce_txns':bounce_t,'bounce_count':len(bounce_t),'ecs_return_txns':ecs_t,'ecs_count':len(ecs_t),
        'cc_txns':cc_t,'cc_count':len(cc_t),'cc_pattern':cc_pat,
        'cc_total':round(sum(_safe(t.get('amount',0)) for t in cc_t),2),'top_10_debits':top10,
    }


def _build_gstr1(transactions: list) -> dict:
    b2b=[]; b2c=[]; exp=[]; nil=[]; ms=defaultdict(lambda:{'b2b':0,'b2c':0,'export':0,'nil':0})
    B2B_S=['pvt ltd','private limited','limited',' llp','technologies','solutions','enterprises','trading co']
    B2B_G=['razorpay settlement','cashfree settlement','payu settlement','instamojo','tramo']
    EXP_K=['swift','foreign inward','usd ','eur ','gbp ','paypal','stripe','wise']
    for t in transactions:
        if t.get('type')!='CR' or not t.get('amount'): continue
        lower=(t.get('desc') or '').lower(); amt=_safe(t.get('amount',0)); month=_month(t)
        cls=_classify_credit(lower,amt)
        if cls in (CC.LOAN,CC.SELF,CC.FAMILY,CC.REFUND,CC.REIMBURSE,CC.NON_INC,CC.CASH_DEP): continue
        if _kw(lower,_SALARY_KW): continue
        if _kw(lower,_INTEREST_KW): nil.append(t); ms[month]['nil']+=amt; continue
        if _kw(lower,EXP_K): exp.append(t); ms[month]['export']+=amt
        elif _kw(lower,B2B_S) or _kw(lower,B2B_G): b2b.append(t); ms[month]['b2b']+=amt
        elif amt>=100: b2c.append(t); ms[month]['b2c']+=amt
    tb2b=round(sum(_safe(t.get('amount',0)) for t in b2b),2)
    tb2c=round(sum(_safe(t.get('amount',0)) for t in b2c),2)
    tax=round(tb2b+tb2c,2)
    return {
        'b2b_supplies':b2b,'b2c_supplies':b2c[:50],'export_supplies':exp,'nil_exempt':nil,
        'total_b2b':tb2b,'total_b2c':tb2c,
        'total_export':round(sum(_safe(t.get('amount',0)) for t in exp),2),
        'total_nil':round(sum(_safe(t.get('amount',0)) for t in nil),2),
        'total_taxable':tax,'estimated_gst':round(tax*C.GST_RATE,2),
        'monthly_sales':dict(ms),'months':sorted(ms.keys()),
        'b2b_count':len(b2b),'b2c_count':len(b2c),'export_count':len(exp),
        'filing_status':'Filing Required' if tax>0 else 'Verify with CA',
        'note':'B2B/B2C approximate. Verify GSTIN for accurate GSTR-1.',
    }


def _build_payment_modes(transactions: list) -> dict:
    cr=defaultdict(float); dr=defaultdict(float); cnt=defaultdict(int)
    for t in transactions:
        mode=_payment_mode(t.get('desc',''))
        amt=_safe(t.get('amount',0)); cnt[mode]+=1
        if t.get('type')=='CR': cr[mode]+=amt
        else: dr[mode]+=amt
    total=len(transactions) or 1
    res={}
    for m in set(cnt.keys()):
        vol=cr[m]+dr[m]
        res[m]={'count':cnt[m],'count_pct':round(_pct(cnt[m],total),1),
                'total_cr':round(cr[m],2),'total_dr':round(dr[m],2),'total_volume':round(vol,2)}
    sorted_m=sorted(res.items(),key=lambda x:-x[1]['total_volume'])
    return {
        'by_mode':res,'sorted_modes':sorted_m,
        'upi_count':cnt.get('UPI',0),'upi_volume':round(cr['UPI']+dr['UPI'],2),
        'neft_count':cnt.get('NEFT',0),'imps_count':cnt.get('IMPS',0),
        'atm_count':cnt.get('ATM',0),'card_count':cnt.get('Card',0),
        'cash_deposit_count':cnt.get('Cash Deposit',0),
        'international_count':cnt.get('International',0),
        'international_volume':round(cr['International']+dr['International'],2),
    }


def _build_bk(transactions: list, e: EngineResult) -> dict:
    bals_by_day={}; neg_days=set()
    for t in sorted(transactions, key=lambda x: x.get('date','')):
        bal=t.get('balance'); date=t.get('date','')
        if bal is not None and date:
            v=_safe(bal); bals_by_day[date]=v
            if v<0: neg_days.add(date)
    all_b=list(bals_by_day.values())
    m_bals=defaultdict(list)
    m_end={}
    for date,bal in bals_by_day.items():
        m_bals[date[:7]].append(bal)
    for m,blist in m_bals.items():
        m_end[m]=round(blist[-1],2)
    m_avg={m:round(_div(sum(b),len(b)),2) for m,b in m_bals.items()}
    neg_count=len(neg_days)
    e.negative_balance_days=neg_count   # update engine with precise count
    return {
        'avg_daily_balance': e.avg_balance,   # from engine
        'highest_balance':   e.max_balance,
        'lowest_balance':    e.min_balance,
        'negative_balance_days':   neg_count,
        'negative_balance_dates':  sorted(neg_days),
        'monthly_avg_balance':     dict(sorted(m_avg.items())),
        'month_end_balances':      dict(sorted(m_end.items())),
        'total_days_tracked':      len(bals_by_day),
    }


# ══════════════════════════════════════════════════════════════
#  §12  MASTER FUNCTION  (#10)
# ══════════════════════════════════════════════════════════════

def run_dashboard(transactions: list) -> dict:
    """
    #10 — Every value originates from ONE computation (FinancialEngine).
    Modules are reporters, not calculators.
    Pre-export validation pass runs before final assembly.
    """
    if not transactions:
        return _empty_dashboard()

    # ── 1. Build engine (single source of truth) ──────────────
    e = _build_engine(transactions)

    # ── 2. Classify all transactions (shared buckets) ─────────
    buckets: Dict = defaultdict(list)
    for t in transactions:
        ttype = t.get('type', '')
        amt   = _safe(t.get('amount', 0))
        lower = (t.get('desc') or '').lower()
        is_cdm = _kw(lower, _CASH_DEP_KW)
        if ttype == 'CR' and amt > 0:
            cls = _classify_credit(lower, amt)
            buckets[cls].append(t)
        elif ttype == 'DR' and amt > 0:
            if is_cdm:
                buckets[CC.CASH_DEP].append(t)
            else:
                buckets['_dr'].append(t)
        elif amt > 0 and is_cdm:
            buckets[CC.CASH_DEP].append(t)

    # ── 3. Build monthly_data dict (used by loan + cashflow) ──
    monthly_cr = defaultdict(float); monthly_dr = defaultdict(float)
    monthly_re = defaultdict(float); m_min_bal: Dict = {}
    for t in transactions:
        month = _month(t); amt = _safe(t.get('amount', 0))
        lower = (t.get('desc') or '').lower()
        bal   = t.get('balance')
        if t.get('type') == 'CR':
            monthly_cr[month] += amt
            if _classify_credit(lower, amt) in _TAXABLE:
                monthly_re[month] += amt
        else:
            monthly_dr[month] += amt
        if bal is not None:
            v = _safe(bal)
            if month not in m_min_bal: m_min_bal[month] = v
            else: m_min_bal[month] = min(m_min_bal[month], v)
    months = sorted(set(list(monthly_cr) + list(monthly_dr)))
    monthly_data = {
        m: {
            'credits': round(monthly_cr.get(m, 0), 2),
            'real_income': round(monthly_re.get(m, 0), 2),
            'debits': round(monthly_dr.get(m, 0), 2),
            'min_bal': round(m_min_bal.get(m, 0), 2),
            'max_bal': 0, 'txn_count': 0,
        }
        for m in months
    }

    # ── 4. Build all modules (reporters — read engine) ─────────
    income_data   = _build_income(transactions, buckets, e)
    audit_data    = _build_audit(transactions, e)
    bk            = _build_bk(transactions, e)
    loan_data     = _build_loan(e, monthly_data)
    expense_data  = _build_expenses(transactions)
    itr_data      = _build_itr(transactions, e, buckets)
    cashflow      = _build_cashflow(transactions)
    obligations   = _build_obligations(transactions)
    compliance    = _build_compliance(transactions)
    red_flags     = _build_red_flags(transactions)
    gstr1         = _build_gstr1(transactions)
    payment_modes = _build_payment_modes(transactions)
    risk_section  = _build_risk_section(e, red_flags)

    # ── 5. Pre-export validation (#7 #8 #12) ──────────────────
    vr = _validate(e, income_data, loan_data, audit_data)
    # If validation failed, override affected values with engine values
    if not vr.passed:
        # Force income totals to engine values
        income_data['real_income_total']     = e.real_income_total
        itr_data['real_income_total']        = e.real_income_total
        loan_data['foir']                    = e.foir
        loan_data['monthly_emi']             = e.monthly_fixed_obligations
        loan_data['monthly_fixed_obligations'] = e.monthly_fixed_obligations
        loan_data['loan_eligible']           = e.loan_eligible

    # ── 6. Build composite modules that need engine values ─────
    compliance['risk_score']      = e.risk_score      # #4 consistent
    compliance['risk_level']      = e.risk_level
    compliance['risk_color']      = e.risk_color
    compliance['risk_reasons']    = e.risk_reasons
    compliance['risk_explanation']= _build_compliance_risk(e, compliance)
    # Force red_flags level consistent with engine (#4)
    red_flags['flag_level']  = max(red_flags['flag_level'], e.risk_level,
                                    key=lambda x: {'Low':0,'Medium':1,'High':2}[x])
    red_flags['flag_color']  = {'Low':'green','Medium':'yellow','High':'red'}[red_flags['flag_level']]

    # Stability, loan dep, health — from engine
    income_stab = {
        'stability':        e.income_stability,
        'stability_color':  e.stability_color,
        'cv_pct':           e.stability_cv,
        'avg_monthly':      e.avg_monthly_real_income,
        'total_months':     e.n_months,
        'explanation': (f'Income CV: {e.stability_cv}% — {e.income_stability}. '
                        f'Avg monthly income: Rs{e.avg_monthly_real_income:,.0f}.'),
        'monthly_breakdown': e.monthly_real_income,
    }
    loan_dep = {
        'dependency':    e.loan_dep_level,
        'dep_color':     e.loan_dep_color,
        'dep_explanation': (
            f'{e.loan_dep_level} dependency ({e.loan_dep_pct:.0f}% of income). '
            f'Loan disbursals: Rs{e.loan_disbursal_total:,.0f}.'
        ),
        'loan_credit_count':   len(buckets.get(CC.LOAN, [])),
        'loan_credit_total':   e.loan_disbursal_total,
        'loan_pct_of_credits': round(_pct(e.loan_disbursal_total, e.total_cr), 1),
        'dep_ratio_vs_income': e.loan_dep_pct,
        'emi_count':           audit_data.get('emi_count', 0),
        'emi_total':           audit_data.get('total_emi_outflow', 0),
        'monthly_emi':         e.monthly_fixed_obligations,
    }
    health = {
        'score':          e.health_score,
        'grade':          e.health_grade,
        'grade_color':    e.health_color,
        'interpretation': (
            f'Score {e.health_score}/100 ({e.health_grade}). '
            f'FOIR {e.foir}%, Stability={e.income_stability}, '
            f'Neg.Days={e.negative_balance_days}, Dep={e.loan_dep_level}.'
        ),
        'breakdown': {},
    }
    disposable = {
        'avg_monthly_real_income':    e.avg_monthly_real_income,
        'monthly_fixed_obligations':  e.monthly_fixed_obligations,
        'estimated_monthly_expenses': round(e.variable_expenses, 2),
        'disposable_income':          e.disposable_income,
        'savings_rate_pct':           e.savings_rate_pct,
        'status': ('Healthy Surplus' if e.disposable_income > e.avg_monthly_real_income * 0.2 else
                   'Tight' if e.disposable_income > 0 else 'Deficit'),
        'explanation': (
            f'Rs{e.avg_monthly_real_income:,.0f} − Rs{e.monthly_fixed_obligations:,.0f} EMI '
            f'− Rs{e.variable_expenses:,.0f} expenses = Rs{e.disposable_income:,.0f} '
            f'({e.savings_rate_pct}% savings)'
        ),
    }
    monthly_summary = []
    month_end_bals  = bk.get('month_end_balances', {})
    for m in months:
        inc = round(monthly_re.get(m, 0), 2)
        exp = round(monthly_dr.get(m, 0), 2)
        sav = round(inc - exp, 2)
        monthly_summary.append({
            'month': m, 'income': inc, 'expenses': exp,
            'savings': sav, 'closing_balance': round(month_end_bals.get(m, 0), 2),
            'surplus': sav >= 0,
        })

    return {
        # Core modules
        'expense':          expense_data,
        'itr':              itr_data,
        'audit':            audit_data,
        'loan':             loan_data,
        'compliance':       compliance,
        'income':           income_data,
        'obligations':      obligations,
        'cashflow':         cashflow,
        'red_flags':        red_flags,
        'gstr1':            gstr1,
        # V4/V5/V6 modules
        'balance_kpis':     bk,
        'payment_modes':    payment_modes,
        'income_stability': income_stab,
        'loan_dependency':  loan_dep,
        'health_score':     health,
        'risk_flags_v4':    risk_section,
        'disposable':       disposable,
        'monthly_summary':  monthly_summary,
        # Summary totals (all from engine — #3)
        'total_txns':            e.total_txns,
        'total_cr':              e.total_cr,
        'total_dr':              e.total_dr,
        'real_income':           e.real_income_total,
        'loan_disbursal_total':  e.loan_disbursal_total,
        'family_transfer_total': e.family_transfer_total,
        # Validation metadata
        '_validation': {
            'passed':   vr.passed,
            'errors':   vr.errors,
            'warnings': vr.warnings,
        },
    }


def _empty_dashboard() -> dict:
    return {
        'expense': {}, 'itr': {}, 'audit': {}, 'loan': {},
        'compliance': {}, 'income': {}, 'obligations': {}, 'cashflow': {},
        'red_flags': {}, 'gstr1': {}, 'balance_kpis': {}, 'payment_modes': {},
        'income_stability': {}, 'loan_dependency': {}, 'health_score': {},
        'risk_flags_v4': {}, 'disposable': {}, 'monthly_summary': [],
        'total_txns': 0, 'total_cr': 0, 'total_dr': 0,
        'real_income': 0, 'loan_disbursal_total': 0, 'family_transfer_total': 0,
        '_validation': {'passed': True, 'errors': [], 'warnings': []},
    }
