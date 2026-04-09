"""
core/dashboard.py
──────────────────
5 financial analysis modules for AarogyamFin Dashboard:
1. Expense Categorization
2. ITR / Tax Filing
3. Audit & Reconciliation
4. Loan & Credit Assessment
5. Compliance Reporting (PMLA / RBI)

v2.1 — Fixed:
- Loan disbursals separated from real income
- Family transfers separated from income
- Self transfers excluded from income
- PCI/ transactions properly categorized
- MB: salary properly detected
- Real income = credits - loan disbursals - family transfers - self transfers
- Freelance income (TRAMO, EKO) properly identified
- Expense categories expanded for Indian merchants
- FOIR calculated on real income, not inflated credits
"""

from collections import defaultdict
from datetime import datetime
import re

# Import helpers from normalizer
from core.normalizer import (
    is_loan_disbursal, is_family_transfer, is_self_transfer,
    LOAN_DISBURSAL_SOURCES, FAMILY_TRANSFER_NAMES
)


# ═══════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════

# Credits from these = real income sources
SALARY_KEYWORDS = [
    'salary', 'payroll', 'sal cr', 'wages', 'pay credit', 'monthly pay',
    'mb:received from shourya', '/salary', 'shourya technologies',
]

FREELANCE_KEYWORDS = [
    'tramo technolab', 'tramo tech',
]

MARKETPLACE_KEYWORDS = [
    'meesho', 'shiprocket', 'meeshofas', 'myntra des',
    'reliance r',  # Reliance payouts
    'razorpay payments',  # online business payments
]

INTEREST_KEYWORDS = [
    'interest', 'int.pd', 'int pd', 'int cr', 'sbint',
    'fd interest', 'savings interest',
]

RENTAL_KEYWORDS = [
    'rent received', 'rental income', 'house rent', 'property rent',
]

# These are recurring credits from individuals (may be income or loans)
RECURRING_PERSON_KEYWORDS = [
    'roushan kumar', 'roushan kuamr',
    'anshu kumari',
    'rakesh kumar',
]

# Debit expense categories
BUSINESS_KEYWORDS = [
    'gst', 'invoice', 'vendor', 'supplier', 'b2b', 'office',
    'tds', 'professional', 'consulting',
    'advertising', 'marketing', 'domain',
    'hosting', 'ca ', 'audit', 'legal',
    'aws', 'azure', 'google cloud',
    'linkedin', 'facebook ads', 'meta ads',
    'canva', 'godaddy', 'shopify',
]

PERSONAL_KEYWORDS = [
    'swiggy', 'zomato', 'netflix', 'spotify', 'zepto',
    'blinkit', 'ola ', 'uber', 'rapido',
    'bookmyshow', 'pvr', 'inox',
    'atm', 'cash', 'petrol', 'fuel',
    'apollo pharmacy', 'one stop pharma', 'tata one mg',
    'gym', 'salon', 'spa',
    'irctc', 'makemytrip', 'oyo', 'airbnb',
    'dth', 'recharge', 'dominos', 'pizza',
    'shreejee', 'swad sadan', 'annas dosa',
    'banaras wala', 'gianis', 'bikaner sweets',
    'amazon', 'flipkart', 'myntra', 'snitch', 'zudio',
    'airtel', 'jio', 'google play',
    'claude.ai', 'anthropic', 'scribd', 'higgsfield',
    'aeronfly', 'railway', 'makemytrip',
    'netflix', 'zee5', 'jiohotstar',
    'safe gold',
]

GST_ELIGIBLE_KEYWORDS = [
    'vendor', 'supplier', 'b2b', 'invoice', 'gst', 'purchase',
    'raw material', 'office rent', 'professional', 'consulting',
    'advertising', 'marketing', 'software', 'hosting',
    'aws', 'azure', 'logistics', 'courier', 'printing', 'stationery',
    'canva', 'godaddy', 'shopify', 'domain',
    'meta ads', 'facebook ads', 'google ads',
    'linkedin', 'microsoft', 'adobe',
]

# Tax deduction keywords
SECTION_80C = [
    'lic', 'ppf', 'nsc', 'elss', 'epf', 'provident fund',
    'life insurance', 'mutual fund', 'tax saving fd', 'safe gold',
]
SECTION_80D = [
    'health insurance', 'mediclaim', 'star health',
    'bajaj allianz health', 'niva bupa',
]
TDS_KEYWORDS = ['tds', 'tax deducted', 'income tax']

# EMI / loan repayment keywords (DEBITS)
EMI_KEYWORDS = [
    'emi', 'loan repay', 'nach',
    'pocketly', 'stucred', 'mpokket', 'mpokket financi',
    'speel finance', 'speel fin',
    'lazypay repayme', 'lazypay',
    'snapmint credit', 'snapmint',
    'truecredit', 'true credits',
    'branch internat', 'branch/',
    'kreon finnancia',
    'instantpay indi',
]

# Cheque / bounce keywords
CHEQUE_KEYWORDS = ['chq', 'cheque', 'clearing', 'cts', 'micr']
BOUNCE_KEYWORDS = ['return', 'bounce', 'dishonour', 'failed', 'reject', 'insufficient']

# Compliance
CASH_KEYWORDS = ['cash deposit', 'cash withdrawal', 'atm', 'cwdr', 'cash wdl', 'cdm']
HIGH_VALUE_THRESHOLD = 200000
CASH_DEPOSIT_DAILY_LIMIT = 50000
ANNUAL_CASH_LIMIT = 1000000


# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════

def _classify_credit(desc: str, amt: float) -> str:
    """
    Classify a credit transaction into:
    salary / freelance / marketplace / interest / rental /
    loan_disbursal / family_transfer / self_transfer / other_credit
    """
    lower = desc.lower()

    # Loan disbursals — MUST check first
    if is_loan_disbursal(lower):
        return 'loan_disbursal'

    # Self transfers
    if is_self_transfer(lower):
        return 'self_transfer'

    # Family transfers
    if is_family_transfer(lower):
        return 'family_transfer'

    # Salary
    if any(k in lower for k in SALARY_KEYWORDS):
        return 'salary'

    # Freelance
    if any(k in lower for k in FREELANCE_KEYWORDS):
        return 'freelance'

    # Marketplace income
    if any(k in lower for k in MARKETPLACE_KEYWORDS):
        return 'marketplace'

    # Interest
    if any(k in lower for k in INTEREST_KEYWORDS):
        return 'interest'

    # Rental
    if any(k in lower for k in RENTAL_KEYWORDS):
        return 'rental'

    # Recurring person credits — could be informal income
    if any(k in lower for k in RECURRING_PERSON_KEYWORDS):
        return 'recurring_person'

    # MB: received (not salary, not family)
    if lower.startswith('mb:received'):
        return 'mb_transfer'

    return 'other_credits'


# ═══════════════════════════════════════════════════════════
#  1. EXPENSE CATEGORIZATION
# ═══════════════════════════════════════════════════════════

def analyze_expenses(transactions: list) -> dict:
    business, personal, mixed = [], [], []
    gst_eligible = []
    category_totals = defaultdict(float)
    monthly_spend = defaultdict(lambda: {'business': 0, 'personal': 0})

    for t in transactions:
        if t.get('type') != 'DR' or not t.get('amount'):
            continue

        desc  = (t.get('desc') or '').lower()
        amt   = t['amount']
        date  = t.get('date', '')
        month = date[:7] if date else 'Unknown'
        cat   = t.get('category', 'Other')
        category_totals[cat] += amt

        # Skip loan repayments from expense classification
        if any(k in desc for k in EMI_KEYWORDS):
            category_totals['EMI/Loan Repayment'] += amt
            continue

        is_biz = any(k in desc for k in BUSINESS_KEYWORDS)
        is_per = any(k in desc for k in PERSONAL_KEYWORDS)
        is_gst = any(k in desc for k in GST_ELIGIBLE_KEYWORDS)

        if cat in ('UPI', 'Transfer', 'Charges', 'IMPS', 'NEFT/RTGS', 'ATM/Cash',
                   'Subscription', 'Entertainment', 'Food', 'Shopping', 'Travel', 'Health'):
            is_per = True

        # PCI/ transactions — card spends, mostly personal
        if desc.startswith('pci/'):
            is_per = True
            # Canva, Anthropic = could be business
            if any(k in desc for k in ['canva', 'anthropic', 'claude', 'godaddy']):
                is_biz = True
                is_gst = True

        txn = {**t, 'gst_eligible': is_gst}

        if is_biz and not is_per:
            business.append(txn)
            monthly_spend[month]['business'] += amt
        elif is_per and not is_biz:
            personal.append(txn)
            monthly_spend[month]['personal'] += amt
        else:
            mixed.append(txn)
            monthly_spend[month]['personal'] += amt

        if is_gst:
            gst_eligible.append(txn)

    total_dr = sum(t['amount'] for t in transactions if t.get('type') == 'DR' and t.get('amount'))

    return {
        'business':          business,
        'personal':          personal,
        'mixed':             mixed,
        'gst_eligible':      gst_eligible,
        'business_total':    round(sum(t['amount'] for t in business), 2),
        'personal_total':    round(sum(t['amount'] for t in personal), 2),
        'mixed_total':       round(sum(t['amount'] for t in mixed), 2),
        'gst_eligible_total':round(sum(t['amount'] for t in gst_eligible), 2),
        'category_totals':   dict(sorted(category_totals.items(), key=lambda x: -x[1])),
        'monthly_spend':     dict(monthly_spend),
        'total_debits':      round(total_dr, 2),
        'business_pct':      round(sum(t['amount'] for t in business) / total_dr * 100, 1) if total_dr else 0,
        'personal_pct':      round(sum(t['amount'] for t in personal) / total_dr * 100, 1) if total_dr else 0,
    }


# ═══════════════════════════════════════════════════════════
#  2. ITR / TAX FILING
# ═══════════════════════════════════════════════════════════

def analyze_itr(transactions: list) -> dict:
    income_sources = {
        'salary':           [],
        'freelance':        [],
        'marketplace':      [],
        'interest':         [],
        'rental':           [],
        'recurring_person': [],
        'mb_transfer':      [],
        'other_credits':    [],
        # Non-income (separated out)
        'loan_disbursal':   [],
        'family_transfer':  [],
        'self_transfer':    [],
    }

    deductions = {
        '80C':      [],
        '80D':      [],
        'tds_paid': [],
    }

    high_value_credits = []
    monthly_income     = defaultdict(float)

    for t in transactions:
        desc  = (t.get('desc') or '').lower()
        amt   = t.get('amount', 0)
        date  = t.get('date', '')
        month = date[:7] if date else 'Unknown'

        if t.get('type') == 'CR' and amt:
            credit_type = _classify_credit(desc, amt)
            income_sources[credit_type].append(t)

            # Only count real income in monthly_income
            if credit_type in ('salary', 'freelance', 'marketplace',
                               'interest', 'rental', 'recurring_person',
                               'mb_transfer', 'other_credits'):
                monthly_income[month] += amt

            if amt >= 100000:
                high_value_credits.append({**t, 'credit_type': credit_type})

        elif t.get('type') == 'DR' and amt:
            if any(k in desc for k in SECTION_80C):
                deductions['80C'].append(t)
            elif any(k in desc for k in SECTION_80D):
                deductions['80D'].append(t)
            elif any(k in desc for k in TDS_KEYWORDS):
                deductions['tds_paid'].append(t)

    # Totals
    salary_total      = sum(t['amount'] for t in income_sources['salary'])
    freelance_total   = sum(t['amount'] for t in income_sources['freelance'])
    marketplace_total = sum(t['amount'] for t in income_sources['marketplace'])
    interest_total    = sum(t['amount'] for t in income_sources['interest'])
    rental_total      = sum(t['amount'] for t in income_sources['rental'])
    recurring_total   = sum(t['amount'] for t in income_sources['recurring_person'])
    other_total       = sum(t['amount'] for t in income_sources['other_credits'])
    mb_total          = sum(t['amount'] for t in income_sources['mb_transfer'])

    loan_disbursal_total  = sum(t['amount'] for t in income_sources['loan_disbursal'])
    family_transfer_total = sum(t['amount'] for t in income_sources['family_transfer'])
    self_transfer_total   = sum(t['amount'] for t in income_sources['self_transfer'])

    real_income_total = (salary_total + freelance_total + marketplace_total +
                         interest_total + rental_total + recurring_total +
                         other_total + mb_total)

    total_credits = sum(t['amount'] for t in transactions
                        if t.get('type') == 'CR' and t.get('amount'))

    section_80c_total = sum(t['amount'] for t in deductions['80C'])
    section_80d_total = sum(t['amount'] for t in deductions['80D'])

    # ITR form suggestion
    if freelance_total > 0 or marketplace_total > 0:
        suggested_itr = 'ITR-3 (Business/Freelance Income detected)'
    elif rental_total > 0:
        suggested_itr = 'ITR-2 (Rental Income detected)'
    elif salary_total > 0:
        suggested_itr = 'ITR-1 (Salary Income — verify with CA)'
    else:
        suggested_itr = 'ITR-1 or ITR-2 (Verify with CA — no clear salary found)'

    return {
        'income_sources':        income_sources,
        'deductions':            deductions,
        'high_value_credits':    high_value_credits,
        'monthly_income':        dict(monthly_income),

        # Real income (excluding loans/family)
        'real_income_total':     round(real_income_total, 2),
        'loan_disbursal_total':  round(loan_disbursal_total, 2),
        'family_transfer_total': round(family_transfer_total, 2),
        'self_transfer_total':   round(self_transfer_total, 2),

        # Raw total (all credits, for reference)
        'total_credits':         round(total_credits, 2),

        'salary_total':          round(salary_total, 2),
        'freelance_total':       round(freelance_total, 2),
        'marketplace_total':     round(marketplace_total, 2),
        'interest_total':        round(interest_total, 2),
        'section_80c_total':     round(min(section_80c_total, 150000), 2),
        'section_80d_total':     round(min(section_80d_total, 25000), 2),
        'suggested_itr':         suggested_itr,
        'high_value_count':      len(high_value_credits),

        'income_breakdown': {
            'Salary':          round(salary_total, 2),
            'Freelance':       round(freelance_total, 2),
            'Marketplace':     round(marketplace_total, 2),
            'Interest':        round(interest_total, 2),
            'Rental':          round(rental_total, 2),
            'Recurring/Other': round(recurring_total + other_total + mb_total, 2),
            # Non-income (shown separately for transparency)
            'Loan Received':   round(loan_disbursal_total, 2),
            'Family Transfer': round(family_transfer_total, 2),
            'Self Transfer':   round(self_transfer_total, 2),
        }
    }


# ═══════════════════════════════════════════════════════════
#  3. AUDIT & RECONCILIATION
# ═══════════════════════════════════════════════════════════

def analyze_reconciliation(transactions: list) -> dict:
    cheques           = []
    emis              = []
    bounced           = []
    balance_mismatches= []
    duplicate_suspects= []
    seen              = defaultdict(list)

    for t in transactions:
        desc = (t.get('desc') or '').lower()
        amt  = t.get('amount', 0)
        date = t.get('date', '')

        if any(k in desc for k in CHEQUE_KEYWORDS):
            cheques.append(t)

        if any(k in desc for k in EMI_KEYWORDS):
            emis.append(t)

        if any(k in desc for k in BOUNCE_KEYWORDS):
            bounced.append(t)

        key = (date, round(amt, 0), t.get('type'))
        seen[key].append(t)

    for key, txns in seen.items():
        if len(txns) > 1:
            duplicate_suspects.extend(txns)

    for i in range(1, len(transactions)):
        prev = transactions[i - 1]
        curr = transactions[i]
        if prev.get('balance') and curr.get('balance') and curr.get('amount'):
            expected = round(
                prev['balance'] + curr['amount'] if curr.get('type') == 'CR'
                else prev['balance'] - curr['amount'], 2
            )
            actual = round(curr['balance'], 2)
            if abs(expected - actual) > 1.0:
                balance_mismatches.append({
                    **curr,
                    'expected_balance': expected,
                    'diff': round(abs(expected - actual), 2)
                })

    total_emi = sum(t['amount'] for t in emis if t.get('amount') and t.get('type') == 'DR')
    total_cr  = sum(t['amount'] for t in transactions if t.get('type') == 'CR' and t.get('amount'))

    return {
        'cheques':            cheques,
        'emis':               emis,
        'bounced':            bounced,
        'balance_mismatches': balance_mismatches,
        'duplicate_suspects': list({id(t): t for t in duplicate_suspects}.values()),
        'cheque_count':       len(cheques),
        'emi_count':          len(emis),
        'total_emi_outflow':  round(total_emi, 2),
        'emi_to_income_ratio':round(total_emi / total_cr * 100, 1) if total_cr else 0,
        'bounce_count':       len(bounced),
        'mismatch_count':     len(balance_mismatches),
        'duplicate_count':    len(set(id(t) for t in duplicate_suspects)),
        'reconciliation_score': max(0, 100 - len(balance_mismatches) * 5
                                    - len(bounced) * 10
                                    - len(duplicate_suspects) * 3),
    }


# ═══════════════════════════════════════════════════════════
#  4. LOAN & CREDIT ASSESSMENT
# ═══════════════════════════════════════════════════════════

def analyze_loan_eligibility(transactions: list) -> dict:
    if not transactions:
        return {}

    monthly_data  = defaultdict(lambda: {
        'credits': 0, 'real_income': 0, 'debits': 0,
        'min_bal': float('inf'), 'max_bal': 0, 'txn_count': 0
    })
    balances      = [t['balance'] for t in transactions if t.get('balance')]
    emis_detected = []

    for t in transactions:
        date  = t.get('date', '')
        month = date[:7] if date else 'Unknown'
        amt   = t.get('amount', 0) or 0
        desc  = (t.get('desc') or '').lower()

        if t.get('type') == 'CR':
            monthly_data[month]['credits'] += amt
            # Real income excludes loan disbursals, family transfers, self transfers
            if not is_loan_disbursal(desc) and not is_family_transfer(desc) and not is_self_transfer(desc):
                monthly_data[month]['real_income'] += amt
        else:
            monthly_data[month]['debits'] += amt

        if t.get('balance'):
            monthly_data[month]['min_bal'] = min(monthly_data[month]['min_bal'], t['balance'])
            monthly_data[month]['max_bal'] = max(monthly_data[month]['max_bal'], t['balance'])

        monthly_data[month]['txn_count'] += 1

        # EMI = debit side loan repayments
        if t.get('type') == 'DR' and any(k in desc for k in EMI_KEYWORDS):
            emis_detected.append(t)

    months = sorted(monthly_data.keys())
    n      = len(months)

    avg_monthly_credit     = sum(monthly_data[m]['credits'] for m in months) / n if n else 0
    avg_monthly_real_income= sum(monthly_data[m]['real_income'] for m in months) / n if n else 0
    avg_monthly_debit      = sum(monthly_data[m]['debits'] for m in months) / n if n else 0
    avg_balance            = sum(balances) / len(balances) if balances else 0
    min_balance            = min(balances) if balances else 0
    max_balance            = max(balances) if balances else 0

    total_emi   = sum(t['amount'] for t in emis_detected if t.get('amount'))
    monthly_emi = total_emi / n if n else 0

    # Use REAL income for FOIR, not inflated credits
    base_income = avg_monthly_real_income if avg_monthly_real_income > 0 else avg_monthly_credit
    dscr        = round(base_income / monthly_emi, 2) if monthly_emi > 0 else None
    foir        = round((monthly_emi / base_income) * 100, 1) if base_income and monthly_emi else 0

    negative_months      = sum(1 for m in months if monthly_data[m]['min_bal'] < 0)
    max_eligible_emi     = round(base_income * 0.50, 2)
    remaining_emi_capacity = round(max(0, max_eligible_emi - monthly_emi), 2)

    monthly_rate = 0.10 / 12
    if remaining_emi_capacity > 0:
        loan_eligible = round(
            remaining_emi_capacity * ((1 - (1 + monthly_rate) ** -60) / monthly_rate), 2
        )
    else:
        loan_eligible = 0

    if negative_months == 0 and foir < 40 and avg_balance > base_income * 0.5:
        credit_indicator = 'Strong'
        credit_color     = 'green'
    elif negative_months <= 2 and foir < 60:
        credit_indicator = 'Moderate'
        credit_color     = 'yellow'
    else:
        credit_indicator = 'Needs Improvement'
        credit_color     = 'red'

    return {
        'monthly_data':          {m: {**v, 'min_bal': v['min_bal'] if v['min_bal'] != float('inf') else 0}
                                   for m, v in monthly_data.items()},
        'avg_monthly_credit':    round(avg_monthly_credit, 2),
        'avg_monthly_real_income': round(avg_monthly_real_income, 2),
        'avg_monthly_debit':     round(avg_monthly_debit, 2),
        'avg_balance':           round(avg_balance, 2),
        'min_balance':           round(min_balance, 2),
        'max_balance':           round(max_balance, 2),
        'monthly_emi':           round(monthly_emi, 2),
        'foir':                  foir,
        'dscr':                  dscr,
        'loan_eligible':         loan_eligible,
        'remaining_emi_capacity':remaining_emi_capacity,
        'negative_months':       negative_months,
        'credit_indicator':      credit_indicator,
        'credit_color':          credit_color,
        'months_analyzed':       n,
        'emis_detected':         emis_detected[:10],
    }


# ═══════════════════════════════════════════════════════════
#  5. COMPLIANCE REPORTING (PMLA / RBI)
# ═══════════════════════════════════════════════════════════

def analyze_compliance(transactions: list) -> dict:
    high_value_txns    = []
    cash_txns          = []
    round_figure_txns  = []
    daily_cash         = defaultdict(float)
    monthly_cash       = defaultdict(float)
    structured_suspects= []

    for t in transactions:
        amt   = t.get('amount', 0) or 0
        desc  = (t.get('desc') or '').lower()
        date  = t.get('date', '')
        month = date[:7] if date else 'Unknown'

        if amt >= HIGH_VALUE_THRESHOLD:
            high_value_txns.append(t)

        if any(k in desc for k in CASH_KEYWORDS):
            cash_txns.append(t)
            if t.get('type') == 'CR':  # Only cash deposits count for limits
                daily_cash[date]   += amt
                monthly_cash[month]+= amt

        if amt >= 10000 and amt % 10000 == 0:
            round_figure_txns.append(t)

    daily_breaches = [
        {'date': d, 'amount': round(a, 2)}
        for d, a in daily_cash.items()
        if a > CASH_DEPOSIT_DAILY_LIMIT
    ]

    annual_cash_total  = sum(daily_cash.values())
    form_61a_required  = annual_cash_total >= ANNUAL_CASH_LIMIT

    for t in transactions:
        amt = t.get('amount', 0) or 0
        if 180000 <= amt < 200000:
            structured_suspects.append(t)

    str_candidates  = []
    daily_totals    = defaultdict(list)
    for t in transactions:
        daily_totals[t.get('date', '')].append(t)

    for date, txns in daily_totals.items():
        day_total = sum(t.get('amount', 0) for t in txns if t.get('type') == 'CR')
        if day_total >= 500000:
            str_candidates.append({'date': date, 'total': round(day_total, 2), 'count': len(txns)})

    total_cr = sum(t['amount'] for t in transactions if t.get('type') == 'CR' and t.get('amount'))
    total_dr = sum(t['amount'] for t in transactions if t.get('type') == 'DR' and t.get('amount'))

    risk_score = 0
    if high_value_txns:    risk_score += len(high_value_txns) * 5
    if daily_breaches:     risk_score += len(daily_breaches) * 10
    if structured_suspects:risk_score += len(structured_suspects) * 15
    if str_candidates:     risk_score += len(str_candidates) * 20
    risk_score = min(risk_score, 100)

    risk_level = 'Low'   if risk_score < 20 else \
                 'Medium' if risk_score < 50 else 'High'
    risk_color = 'green'  if risk_score < 20 else \
                 'yellow'  if risk_score < 50 else 'red'

    return {
        'high_value_txns':     high_value_txns[:20],
        'cash_txns':           cash_txns[:20],
        'round_figure_txns':   round_figure_txns[:20],
        'structured_suspects': structured_suspects[:10],
        'str_candidates':      str_candidates[:10],
        'daily_breaches':      daily_breaches,
        'annual_cash_total':   round(annual_cash_total, 2),
        'form_61a_required':   form_61a_required,
        'high_value_count':    len(high_value_txns),
        'cash_count':          len(cash_txns),
        'round_figure_count':  len(round_figure_txns),
        'structured_count':    len(structured_suspects),
        'str_count':           len(str_candidates),
        'risk_score':          risk_score,
        'risk_level':          risk_level,
        'risk_color':          risk_color,
        'total_credits':       round(total_cr, 2),
        'total_debits':        round(total_dr, 2),
    }


# ═══════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════

def run_dashboard(transactions: list) -> dict:
    """Run all 5 analyses and return combined dashboard data."""
    itr_data = analyze_itr(transactions)

    total_cr = round(sum(t['amount'] for t in transactions
                         if t.get('type') == 'CR' and t.get('amount')), 2)
    total_dr = round(sum(t['amount'] for t in transactions
                         if t.get('type') == 'DR' and t.get('amount')), 2)

    return {
        'expense':              analyze_expenses(transactions),
        'itr':                  itr_data,
        'audit':                analyze_reconciliation(transactions),
        'loan':                 analyze_loan_eligibility(transactions),
        'compliance':           analyze_compliance(transactions),
        'total_txns':           len(transactions),
        'total_cr':             total_cr,
        'total_dr':             total_dr,
        # Expose real income at top level for dashboard header
        'real_income':          itr_data.get('real_income_total', 0),
        'loan_disbursal_total': itr_data.get('loan_disbursal_total', 0),
        'family_transfer_total':itr_data.get('family_transfer_total', 0),
    }
