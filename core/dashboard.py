"""
core/dashboard.py
──────────────────
Financial analysis modules for AarogyamFin Dashboard.
Features 1-27 (checklist complete):

INCOME ANALYSIS (1-6) — analyze_income()
EXPENSE & OBLIGATIONS (7-12) — analyze_obligations()
BALANCE & CASH FLOW (13-16) — analyze_balance_cashflow()
RED FLAGS & FRAUD (17-21) — analyze_red_flags()
COMPLIANCE (22-24) — analyze_compliance()
OUTPUT (25-27) — analyze_reconciliation(), Excel export, run_dashboard()
"""

from collections import defaultdict
from datetime import datetime
import re

from core.normalizer import (
    is_loan_disbursal, is_family_transfer, is_self_transfer,
    LOAN_DISBURSAL_SOURCES, FAMILY_TRANSFER_NAMES
)


# ═══════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════

SALARY_KEYWORDS = [
    'salary', 'payroll', 'sal cr', 'wages', 'pay credit', 'monthly pay',
    'mb:received from shourya', '/salary', 'shourya technologies',
]
FREELANCE_KEYWORDS = ['tramo technolab', 'tramo tech']
MARKETPLACE_KEYWORDS = [
    'meesho', 'shiprocket', 'meeshofas', 'myntra des',
    'reliance r', 'razorpay payments',
]
INTEREST_KEYWORDS = [
    'interest', 'int.pd', 'int pd', 'int cr', 'sbint',
    'fd interest', 'savings interest',
]
RENTAL_KEYWORDS = ['rent received', 'rental income', 'house rent', 'property rent']
RECURRING_PERSON_KEYWORDS = [
    'roushan kumar', 'roushan kuamr', 'anshu kumari', 'rakesh kumar',
]
BUSINESS_KEYWORDS = [
    'gst', 'invoice', 'vendor', 'supplier', 'b2b', 'office',
    'tds', 'professional', 'consulting', 'advertising', 'marketing', 'domain',
    'hosting', 'ca ', 'audit', 'legal', 'aws', 'azure', 'google cloud',
    'linkedin', 'facebook ads', 'meta ads', 'canva', 'godaddy', 'shopify',
]
PERSONAL_KEYWORDS = [
    'swiggy', 'zomato', 'netflix', 'spotify', 'zepto', 'blinkit',
    'ola ', 'uber', 'rapido', 'bookmyshow', 'pvr', 'inox',
    'atm', 'cash', 'petrol', 'fuel', 'apollo pharmacy', 'one stop pharma',
    'tata one mg', 'gym', 'salon', 'spa', 'irctc', 'makemytrip', 'oyo', 'airbnb',
    'dth', 'recharge', 'dominos', 'pizza', 'shreejee', 'swad sadan', 'annas dosa',
    'banaras wala', 'gianis', 'bikaner sweets', 'amazon', 'flipkart', 'myntra',
    'snitch', 'zudio', 'airtel', 'jio', 'google play', 'claude.ai', 'anthropic',
    'scribd', 'higgsfield', 'aeronfly', 'railway', 'zee5', 'jiohotstar', 'safe gold',
]
GST_ELIGIBLE_KEYWORDS = [
    'vendor', 'supplier', 'b2b', 'invoice', 'gst', 'purchase',
    'raw material', 'office rent', 'professional', 'consulting',
    'advertising', 'marketing', 'software', 'hosting',
    'aws', 'azure', 'logistics', 'courier', 'printing', 'stationery',
    'canva', 'godaddy', 'shopify', 'domain',
    'meta ads', 'facebook ads', 'google ads', 'linkedin', 'microsoft', 'adobe',
]
SECTION_80C = [
    'lic', 'ppf', 'nsc', 'elss', 'epf', 'provident fund',
    'life insurance', 'mutual fund', 'tax saving fd', 'safe gold',
]
SECTION_80D = ['health insurance', 'mediclaim', 'star health', 'bajaj allianz health', 'niva bupa']
TDS_KEYWORDS = ['tds', 'tax deducted', 'income tax']
EMI_KEYWORDS = [
    'emi', 'loan repay', 'nach', 'pocketly', 'stucred', 'mpokket', 'mpokket financi',
    'speel finance', 'speel fin', 'lazypay repayme', 'lazypay',
    'snapmint credit', 'snapmint', 'truecredit', 'true credits',
    'branch internat', 'branch/', 'kreon finnancia', 'instantpay indi',
]
CHEQUE_KEYWORDS = ['chq', 'cheque', 'clearing', 'cts', 'micr']
BOUNCE_KEYWORDS = ['return', 'bounce', 'dishonour', 'failed', 'reject', 'insufficient']
CASH_KEYWORDS   = ['cash deposit', 'cash withdrawal', 'atm', 'cwdr', 'cash wdl', 'cdm']
HIGH_VALUE_THRESHOLD    = 200000
CASH_DEPOSIT_DAILY_LIMIT= 50000
ANNUAL_CASH_LIMIT       = 1000000

# Income analysis constants
_DIVIDEND_KW = ['dividend', 'div cr', 'div paid', 'div ', 'divident']
_CASH_DEP_KW = ['cash deposit', 'cash dep', 'cashdep', 'cdm', 'cash cr']
_BUSINESS_CREDIT_KW = [
    'invoice', 'payment received', 'consulting', 'professional fee',
    'project', 'client', 'service charge', 'razorpay', 'cashfree',
    'instamojo', 'payu', 'ccavenue',
]
_RE_EMPLOYER_NEFT  = re.compile(
    r'(?:neft(?:inw|cr)?[- /]*|imps[- /]*)([A-Z][A-Z0-9 &.\-]{2,40}?)(?:/|\s{2,}|$)',
    re.IGNORECASE
)
_RE_EMPLOYER_MB    = re.compile(r'mb:received from\s+(.+)', re.IGNORECASE)
_RE_EMPLOYER_SLASH = re.compile(r'/([A-Z][A-Z0-9 &.\-]{2,30})/', re.IGNORECASE)

# Credit card keywords
CC_KEYWORDS = [
    'credit card', 'cc payment', 'cc bill', 'creditcard',
    'hdfc cc', 'icici cc', 'sbi card', 'axis cc', 'kotak cc',
    'amex', 'american express', 'visa card', 'mastercard',
    'citi card', 'indusind cc', 'yes bank cc',
    'bill payment', 'card outstanding', 'card dues',
    'minimum due', 'min due', 'total due',
]

# Red flags keywords
GAMBLING_KW = [
    'dream11', 'mpl ', 'my11circle', 'fantasy', 'rummy',
    'bet', 'gambling', 'casino', 'lotto', 'stake',
    'binance', 'wazirx', 'coinswitch', 'coinbase', 'zebpay',
    'bitbns', 'coindcx', 'crypto', 'bitcoin', 'ethereum',
    'exness', 'forex', 'funding pips', 'ftmo',
]
PENALTY_KW = [
    'penalty', 'fine', 'court', 'legal fee', 'legal charge',
    'late fee', 'penal', 'overdue charge', 'notice fee',
]
WINDOW_DRESS_DAYS = 3  # days before/after month end to check


# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════

def _classify_credit(desc: str, amt: float) -> str:
    lower = desc.lower()
    if is_loan_disbursal(lower):    return 'loan_disbursal'
    if is_self_transfer(lower):     return 'self_transfer'
    if is_family_transfer(lower):   return 'family_transfer'
    if any(k in lower for k in SALARY_KEYWORDS):    return 'salary'
    if any(k in lower for k in FREELANCE_KEYWORDS): return 'freelance'
    if any(k in lower for k in MARKETPLACE_KEYWORDS): return 'marketplace'
    if any(k in lower for k in INTEREST_KEYWORDS):  return 'interest'
    if any(k in lower for k in RENTAL_KEYWORDS):    return 'rental'
    if any(k in lower for k in RECURRING_PERSON_KEYWORDS): return 'recurring_person'
    if lower.startswith('mb:received'): return 'mb_transfer'
    return 'other_credits'


def _extract_employer_name(desc: str) -> str:
    if not desc: return ''
    m = _RE_EMPLOYER_MB.search(desc)
    if m: return m.group(1).strip()[:50].title()
    m = _RE_EMPLOYER_NEFT.search(desc)
    if m:
        name = m.group(1).strip(' /-')
        if len(name) > 2: return name[:50].title()
    m = _RE_EMPLOYER_SLASH.search(desc)
    if m:
        name = m.group(1).strip()
        if len(name) > 2: return name[:50].title()
    lower = desc.lower()
    for kw in ['salary', 'payroll', 'sal cr', 'wages']:
        idx = lower.find(kw)
        if idx != -1:
            after = desc[idx + len(kw):].strip(' -:/').split()
            if after: return ' '.join(after[:4]).title()
    return ''


def _parse_date(date_str: str):
    """Parse YYYY-MM-DD string to date object."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════
#  INCOME ANALYSIS (Features 1-6)
# ═══════════════════════════════════════════════════════════

def analyze_income(transactions: list) -> dict:
    salary_txns = []; business_txns = []; rent_txns = []
    dividend_txns = []; interest_txns = []; cash_dep_txns = []
    monthly_salary = defaultdict(float); monthly_business = defaultdict(float)
    monthly_cash_dep = defaultdict(float); monthly_income = defaultdict(float)
    sender_amount = defaultdict(float); sender_count = defaultdict(int)
    employer_names = set()

    for t in transactions:
        if t.get('type') != 'CR' or not t.get('amount'): continue
        desc = (t.get('desc') or ''); lower = desc.lower()
        amt = t['amount']; date = t.get('date', '')
        month = date[:7] if date else 'Unknown'

        if is_loan_disbursal(lower) or is_family_transfer(lower) or is_self_transfer(lower):
            continue

        sender = _extract_employer_name(desc)
        if not sender:
            tokens = [w for w in desc.split() if len(w) > 2 and not w.isdigit()]
            sender = tokens[0].title() if tokens else 'Unknown'
        sender_amount[sender] += amt; sender_count[sender] += 1

        if any(k in lower for k in _CASH_DEP_KW):
            cash_dep_txns.append(t); monthly_cash_dep[month] += amt; continue

        if any(k in lower for k in SALARY_KEYWORDS):
            emp = _extract_employer_name(desc)
            if emp: employer_names.add(emp)
            salary_txns.append({**t, 'employer': emp})
            monthly_salary[month] += amt; monthly_income[month] += amt; continue

        if any(k in lower for k in _DIVIDEND_KW):
            dividend_txns.append(t); monthly_income[month] += amt; continue

        if any(k in lower for k in RENTAL_KEYWORDS):
            rent_txns.append(t); monthly_income[month] += amt; continue

        if any(k in lower for k in INTEREST_KEYWORDS):
            interest_txns.append(t); monthly_income[month] += amt; continue

        if (any(k in lower for k in FREELANCE_KEYWORDS) or
            any(k in lower for k in MARKETPLACE_KEYWORDS) or
            any(k in lower for k in _BUSINESS_CREDIT_KW)):
            business_txns.append(t)
            monthly_business[month] += amt; monthly_income[month] += amt

    avg_biz = sum(monthly_business.values()) / len(monthly_business) if monthly_business else 0
    irregular_months = []
    for m, val in sorted(monthly_business.items()):
        if avg_biz > 0:
            dev = abs(val - avg_biz) / avg_biz * 100
            if dev > 50:
                irregular_months.append({
                    'month': m, 'amount': round(val, 2), 'avg': round(avg_biz, 2),
                    'deviation': round(dev, 1), 'flag': 'Spike' if val > avg_biz else 'Drop',
                })

    all_months = sorted(monthly_income.keys())
    missing_salary_months = []
    if salary_txns and len(all_months) > 1:
        for m in all_months:
            if monthly_salary.get(m, 0) == 0: missing_salary_months.append(m)

    total_cash_dep   = sum(t['amount'] for t in cash_dep_txns)
    gst_applicable   = sum(t['amount'] for t in business_txns)
    salary_total     = round(sum(t['amount'] for t in salary_txns), 2)
    business_total   = round(sum(t['amount'] for t in business_txns), 2)
    rent_total       = round(sum(t['amount'] for t in rent_txns), 2)
    dividend_total   = round(sum(t['amount'] for t in dividend_txns), 2)
    interest_total   = round(sum(t['amount'] for t in interest_txns), 2)

    top_10_sources = sorted(
        [{'sender': s, 'total_amt': round(a, 2), 'count': sender_count[s],
          'avg_amt': round(a / sender_count[s], 2)} for s, a in sender_amount.items()],
        key=lambda x: x['total_amt'], reverse=True
    )[:10]

    return {
        'salary_txns': salary_txns, 'salary_total': salary_total,
        'employer_names': sorted(employer_names),
        'monthly_salary': dict(sorted(monthly_salary.items())),
        'missing_salary_months': missing_salary_months, 'salary_count': len(salary_txns),
        'business_txns': business_txns, 'business_total': business_total,
        'monthly_business': dict(sorted(monthly_business.items())),
        'irregular_months': irregular_months, 'business_count': len(business_txns),
        'rent_txns': rent_txns, 'rent_total': rent_total,
        'dividend_txns': dividend_txns, 'dividend_total': dividend_total,
        'interest_txns': interest_txns, 'interest_total': interest_total,
        'cash_dep_txns': cash_dep_txns, 'cash_dep_total': round(total_cash_dep, 2),
        'monthly_cash_dep': dict(sorted(monthly_cash_dep.items())),
        'cash_flag_10L': total_cash_dep >= 1_000_000,
        'high_single_cash': [t for t in cash_dep_txns if t['amount'] >= 200_000],
        'cash_dep_count': len(cash_dep_txns),
        'all_real_credits': round(salary_total + business_total + rent_total + dividend_total + interest_total, 2),
        'gst_applicable': round(gst_applicable, 2),
        'estimated_gst': round(gst_applicable * 0.18, 2),
        'gst_threshold_crossed': gst_applicable >= 2_000_000,
        'top_10_sources': top_10_sources,
        'real_income_total': round(salary_total + business_total + rent_total + dividend_total + interest_total, 2),
        'monthly_income': dict(sorted(monthly_income.items())),
        'income_breakdown': {
            'Salary': salary_total, 'Business': business_total,
            'Rent': rent_total, 'Dividend': dividend_total, 'Interest': interest_total,
        },
    }


# ═══════════════════════════════════════════════════════════
#  EXPENSE & OBLIGATIONS (Features 7-12)
# ═══════════════════════════════════════════════════════════

def analyze_obligations(transactions: list) -> dict:
    emi_txns = []; bounce_txns = []; ecs_return_txns = []; cc_txns = []
    monthly_emi = defaultdict(float); lender_totals = defaultdict(float)
    cc_monthly = defaultdict(list)
    debit_dest = defaultdict(lambda: {'total': 0, 'count': 0})

    EXCLUDE_DEST_KW = ['atm', 'cash', 'self', 'own account', 'neft outward', 'imps outward']

    for t in transactions:
        desc = (t.get('desc') or ''); lower = desc.lower()
        amt = t.get('amount', 0) or 0
        date = t.get('date', ''); month = date[:7] if date else 'Unknown'

        # Feature 9: Bounce / ECS
        if any(k in lower for k in BOUNCE_KEYWORDS):
            if 'ecs' in lower or 'nach' in lower or 'mandate' in lower:
                ecs_return_txns.append(t)
            else:
                bounce_txns.append(t)

        if t.get('type') != 'DR' or not amt: continue

        # Feature 7: EMI
        if any(k in lower for k in EMI_KEYWORDS):
            emi_txns.append(t); monthly_emi[month] += amt
            for kw in EMI_KEYWORDS:
                if kw in lower and len(kw) > 4:
                    lender_totals[kw.title()] += amt; break

        # Feature 10: Credit card
        if any(k in lower for k in CC_KEYWORDS):
            cc_txns.append(t); cc_monthly[month].append(amt)

        # Feature 12: Top debit destinations
        if not any(k in lower for k in EXCLUDE_DEST_KW):
            tokens = [w for w in desc.split() if len(w) > 3 and not w.isdigit()]
            dest = tokens[0].title() if tokens else 'Unknown'
            debit_dest[dest]['total'] += amt; debit_dest[dest]['count'] += 1

    # CC pattern analysis
    cc_pattern = []
    for month, payments in sorted(cc_monthly.items()):
        total_paid = sum(payments); count = len(payments)
        pattern = 'Multiple Payments' if count > 1 else 'Single Payment'
        cc_pattern.append({'month': month, 'total_paid': round(total_paid, 2),
                           'count': count, 'pattern': pattern})

    top_10_debits = sorted(
        [{'dest': d, 'total': round(v['total'], 2), 'count': v['count'],
          'avg': round(v['total'] / v['count'], 2)} for d, v in debit_dest.items()],
        key=lambda x: x['total'], reverse=True
    )[:10]

    lender_list = sorted(
        [{'name': k, 'total': round(v, 2)} for k, v in lender_totals.items()],
        key=lambda x: x['total'], reverse=True
    )[:8]

    return {
        'emi_txns': emi_txns, 'emi_count': len(emi_txns),
        'total_emi_outflow': round(sum(t['amount'] for t in emi_txns), 2),
        'monthly_emi': dict(sorted(monthly_emi.items())),
        'avg_monthly_emi': round(sum(monthly_emi.values()) / len(monthly_emi), 2) if monthly_emi else 0,
        'lender_list': lender_list,
        'bounce_txns': bounce_txns, 'bounce_count': len(bounce_txns),
        'ecs_return_txns': ecs_return_txns, 'ecs_count': len(ecs_return_txns),
        'cc_txns': cc_txns, 'cc_count': len(cc_txns),
        'cc_pattern': cc_pattern,
        'cc_total': round(sum(t['amount'] for t in cc_txns), 2),
        'top_10_debits': top_10_debits,
    }


# ═══════════════════════════════════════════════════════════
#  BALANCE & CASH FLOW (Features 13-16)
# ═══════════════════════════════════════════════════════════

def analyze_balance_cashflow(transactions: list) -> dict:
    # Group by month
    monthly = defaultdict(lambda: {'credits': 0, 'debits': 0, 'balances': [], 'eom_balance': None})

    sorted_txns = sorted(transactions, key=lambda t: t.get('date', ''))

    for t in sorted_txns:
        date = t.get('date', ''); month = date[:7] if date else 'Unknown'
        amt = t.get('amount', 0) or 0
        bal = t.get('balance')

        if t.get('type') == 'CR': monthly[month]['credits'] += amt
        else: monthly[month]['debits'] += amt
        if bal is not None: monthly[month]['balances'].append(bal)

    # End-of-month balance = last balance in that month
    for month in monthly:
        bals = monthly[month]['balances']
        if bals: monthly[month]['eom_balance'] = bals[-1]

    months = sorted(monthly.keys())
    n = len(months)

    # Feature 13: ABB — 3/6/12 month
    all_balances = [b for t in sorted_txns if (b := t.get('balance')) is not None]
    def avg_bal_last_n(n_months):
        recent = months[-n_months:] if len(months) >= n_months else months
        bals = []
        for m in recent: bals.extend(monthly[m]['balances'])
        return round(sum(bals) / len(bals), 2) if bals else 0

    abb_3m  = avg_bal_last_n(3)
    abb_6m  = avg_bal_last_n(6)
    abb_12m = avg_bal_last_n(12)

    # Feature 14: Month-end balance trend
    eom_trend = []
    for m in months:
        eob = monthly[m]['eom_balance']
        if eob is not None:
            eom_trend.append({'month': m, 'balance': round(eob, 2)})

    # Trend direction
    if len(eom_trend) >= 2:
        first_half = [e['balance'] for e in eom_trend[:len(eom_trend)//2]]
        second_half = [e['balance'] for e in eom_trend[len(eom_trend)//2:]]
        avg_first = sum(first_half) / len(first_half) if first_half else 0
        avg_second = sum(second_half) / len(second_half) if second_half else 0
        if avg_second > avg_first * 1.1:   trend_direction = 'Improving ↑'
        elif avg_second < avg_first * 0.9: trend_direction = 'Declining ↓'
        else:                               trend_direction = 'Stable →'
    else:
        trend_direction = 'Insufficient data'

    # Feature 15: Net cash flow per month
    monthly_net = []
    for m in months:
        cr = monthly[m]['credits']; dr = monthly[m]['debits']
        net = round(cr - dr, 2)
        monthly_net.append({
            'month': m, 'credits': round(cr, 2), 'debits': round(dr, 2),
            'net': net, 'surplus': net >= 0,
        })

    surplus_months  = sum(1 for x in monthly_net if x['surplus'])
    deficit_months  = sum(1 for x in monthly_net if not x['surplus'])
    avg_net_flow    = round(sum(x['net'] for x in monthly_net) / n, 2) if n else 0

    # Feature 16: Seasonality — high/low income months
    monthly_credits = {m: monthly[m]['credits'] for m in months}
    if monthly_credits:
        avg_cr = sum(monthly_credits.values()) / len(monthly_credits)
        high_months = [m for m, v in monthly_credits.items() if v > avg_cr * 1.25]
        low_months  = [m for m, v in monthly_credits.items() if v < avg_cr * 0.75]
    else:
        high_months = []; low_months = []

    return {
        # Feature 13: ABB
        'abb_3m': abb_3m, 'abb_6m': abb_6m, 'abb_12m': abb_12m,
        'abb_overall': round(sum(all_balances) / len(all_balances), 2) if all_balances else 0,
        # Feature 14: EOM trend
        'eom_trend': eom_trend, 'trend_direction': trend_direction,
        # Feature 15: Net cash flow
        'monthly_net': monthly_net,
        'surplus_months': surplus_months, 'deficit_months': deficit_months,
        'avg_net_flow': avg_net_flow,
        # Feature 16: Seasonality
        'high_months': high_months, 'low_months': low_months,
        'monthly_credits': dict(sorted(monthly_credits.items())),
        'months_analyzed': n,
    }


# ═══════════════════════════════════════════════════════════
#  RED FLAGS & FRAUD (Features 17-21)
# ═══════════════════════════════════════════════════════════

def analyze_red_flags(transactions: list) -> dict:
    # Feature 17: Circular transactions — same amount back and forth within 7 days
    circular = []
    txn_by_date = defaultdict(list)
    for t in transactions:
        d = _parse_date(t.get('date', ''))
        if d: txn_by_date[d].append(t)

    sorted_dates = sorted(txn_by_date.keys())
    for i, d in enumerate(sorted_dates):
        for t in txn_by_date[d]:
            if t.get('type') != 'CR': continue
            amt = t.get('amount', 0)
            # Look for matching debit within 7 days
            for j in range(i, min(i + 8, len(sorted_dates))):
                d2 = sorted_dates[j]
                for t2 in txn_by_date[d2]:
                    if t2.get('type') == 'DR' and abs(t2.get('amount', 0) - amt) < 1.0 and t2 is not t:
                        circular.append({
                            'credit_date': t.get('date'), 'credit_desc': t.get('desc', '')[:50],
                            'debit_date': t2.get('date'), 'debit_desc': t2.get('desc', '')[:50],
                            'amount': round(amt, 2),
                            'days_gap': (d2 - d).days,
                        })

    # Feature 18: Window dressing — large deposit near month end, withdrawn after
    window_dress = []
    for t in transactions:
        if t.get('type') != 'CR': continue
        amt = t.get('amount', 0) or 0
        if amt < 50000: continue
        d = _parse_date(t.get('date', ''))
        if not d: continue
        # Check if deposit is in last 3 days of month
        import calendar
        last_day = calendar.monthrange(d.year, d.month)[1]
        if d.day >= last_day - WINDOW_DRESS_DAYS:
            # Look for matching debit in first 5 days of next month
            next_month_debits = []
            for t2 in transactions:
                if t2.get('type') != 'DR': continue
                d2 = _parse_date(t2.get('date', ''))
                if not d2: continue
                gap = (d2 - d).days
                if 0 < gap <= 8 and abs(t2.get('amount', 0) - amt) / amt < 0.1:
                    next_month_debits.append(t2)
            if next_month_debits:
                window_dress.append({
                    'deposit_date': t.get('date'), 'deposit_desc': t.get('desc', '')[:50],
                    'amount': round(amt, 2), 'withdrawal_count': len(next_month_debits),
                })

    # Feature 19: Gambling / crypto / speculative
    gambling_txns = []
    for t in transactions:
        desc = (t.get('desc') or '').lower()
        if any(k in desc for k in GAMBLING_KW):
            gambling_txns.append(t)

    gambling_total = round(sum(t.get('amount', 0) for t in gambling_txns), 2)

    # Feature 20: Penalty / legal charges
    penalty_txns = []
    for t in transactions:
        desc = (t.get('desc') or '').lower()
        if any(k in desc for k in PENALTY_KW):
            penalty_txns.append(t)

    # Feature 21: Duplicate / suspicious entries
    seen = defaultdict(list)
    for t in transactions:
        key = (t.get('date', ''), round(t.get('amount', 0), 0), t.get('type'))
        seen[key].append(t)
    duplicate_groups = {k: v for k, v in seen.items() if len(v) > 1}
    duplicate_txns = [t for group in duplicate_groups.values() for t in group]

    # Overall red flag score
    flag_score = 0
    if circular:      flag_score += min(len(circular) * 15, 40)
    if window_dress:  flag_score += min(len(window_dress) * 20, 30)
    if gambling_txns: flag_score += min(len(gambling_txns) * 10, 30)
    if penalty_txns:  flag_score += min(len(penalty_txns) * 5, 20)
    if duplicate_txns:flag_score += min(len(duplicate_txns) * 3, 15)
    flag_score = min(flag_score, 100)

    return {
        # Feature 17
        'circular_txns': circular[:10], 'circular_count': len(circular),
        # Feature 18
        'window_dress': window_dress[:10], 'window_dress_count': len(window_dress),
        # Feature 19
        'gambling_txns': gambling_txns[:20], 'gambling_count': len(gambling_txns),
        'gambling_total': gambling_total,
        # Feature 20
        'penalty_txns': penalty_txns[:20], 'penalty_count': len(penalty_txns),
        'penalty_total': round(sum(t.get('amount', 0) for t in penalty_txns), 2),
        # Feature 21
        'duplicate_txns': duplicate_txns[:20], 'duplicate_count': len(duplicate_txns),
        'duplicate_groups': len(duplicate_groups),
        # Summary
        'flag_score': flag_score,
        'flag_level': 'High' if flag_score >= 50 else 'Medium' if flag_score >= 20 else 'Low',
        'flag_color': 'red' if flag_score >= 50 else 'yellow' if flag_score >= 20 else 'green',
        'total_flags': len(circular) + len(window_dress) + len(gambling_txns) + len(penalty_txns) + len(duplicate_groups),
    }


# ═══════════════════════════════════════════════════════════
#  1. EXPENSE CATEGORIZATION
# ═══════════════════════════════════════════════════════════

def analyze_expenses(transactions: list) -> dict:
    business, personal, mixed = [], [], []
    gst_eligible = []
    category_totals = defaultdict(float)
    monthly_spend = defaultdict(lambda: {'business': 0, 'personal': 0})

    for t in transactions:
        if t.get('type') != 'DR' or not t.get('amount'): continue
        desc = (t.get('desc') or '').lower()
        amt = t['amount']; date = t.get('date', '')
        month = date[:7] if date else 'Unknown'
        cat = t.get('category', 'Other')
        category_totals[cat] += amt

        if any(k in desc for k in EMI_KEYWORDS):
            category_totals['EMI/Loan Repayment'] += amt; continue

        is_biz = any(k in desc for k in BUSINESS_KEYWORDS)
        is_per = any(k in desc for k in PERSONAL_KEYWORDS)
        is_gst = any(k in desc for k in GST_ELIGIBLE_KEYWORDS)

        if cat in ('UPI', 'Transfer', 'Charges', 'IMPS', 'NEFT/RTGS', 'ATM/Cash',
                   'Subscription', 'Entertainment', 'Food', 'Shopping', 'Travel', 'Health'):
            is_per = True

        if desc.startswith('pci/'):
            is_per = True
            if any(k in desc for k in ['canva', 'anthropic', 'claude', 'godaddy']):
                is_biz = True; is_gst = True

        txn = {**t, 'gst_eligible': is_gst}
        if is_biz and not is_per:
            business.append(txn); monthly_spend[month]['business'] += amt
        elif is_per and not is_biz:
            personal.append(txn); monthly_spend[month]['personal'] += amt
        else:
            mixed.append(txn); monthly_spend[month]['personal'] += amt
        if is_gst: gst_eligible.append(txn)

    total_dr = sum(t['amount'] for t in transactions if t.get('type') == 'DR' and t.get('amount'))
    return {
        'business': business, 'personal': personal, 'mixed': mixed,
        'gst_eligible': gst_eligible,
        'business_total': round(sum(t['amount'] for t in business), 2),
        'personal_total': round(sum(t['amount'] for t in personal), 2),
        'mixed_total': round(sum(t['amount'] for t in mixed), 2),
        'gst_eligible_total': round(sum(t['amount'] for t in gst_eligible), 2),
        'category_totals': dict(sorted(category_totals.items(), key=lambda x: -x[1])),
        'monthly_spend': dict(monthly_spend), 'total_debits': round(total_dr, 2),
        'business_pct': round(sum(t['amount'] for t in business) / total_dr * 100, 1) if total_dr else 0,
        'personal_pct': round(sum(t['amount'] for t in personal) / total_dr * 100, 1) if total_dr else 0,
    }


# ═══════════════════════════════════════════════════════════
#  2. ITR / TAX FILING
# ═══════════════════════════════════════════════════════════

def analyze_itr(transactions: list) -> dict:
    income_sources = {
        'salary': [], 'freelance': [], 'marketplace': [], 'interest': [],
        'rental': [], 'recurring_person': [], 'mb_transfer': [], 'other_credits': [],
        'loan_disbursal': [], 'family_transfer': [], 'self_transfer': [],
    }
    deductions = {'80C': [], '80D': [], 'tds_paid': []}
    high_value_credits = []; monthly_income = defaultdict(float)

    for t in transactions:
        desc = (t.get('desc') or '').lower()
        amt = t.get('amount', 0); date = t.get('date', '')
        month = date[:7] if date else 'Unknown'

        if t.get('type') == 'CR' and amt:
            credit_type = _classify_credit(desc, amt)
            income_sources[credit_type].append(t)
            if credit_type in ('salary', 'freelance', 'marketplace', 'interest',
                               'rental', 'recurring_person', 'mb_transfer', 'other_credits'):
                monthly_income[month] += amt
            if amt >= 100000: high_value_credits.append({**t, 'credit_type': credit_type})
        elif t.get('type') == 'DR' and amt:
            if any(k in desc for k in SECTION_80C):   deductions['80C'].append(t)
            elif any(k in desc for k in SECTION_80D): deductions['80D'].append(t)
            elif any(k in desc for k in TDS_KEYWORDS):deductions['tds_paid'].append(t)

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
                         interest_total + rental_total + recurring_total + other_total + mb_total)
    total_credits = sum(t['amount'] for t in transactions if t.get('type') == 'CR' and t.get('amount'))
    section_80c_total = sum(t['amount'] for t in deductions['80C'])
    section_80d_total = sum(t['amount'] for t in deductions['80D'])

    if freelance_total > 0 or marketplace_total > 0:
        suggested_itr = 'ITR-3 (Business/Freelance Income detected)'
    elif rental_total > 0: suggested_itr = 'ITR-2 (Rental Income detected)'
    elif salary_total > 0: suggested_itr = 'ITR-1 (Salary Income — verify with CA)'
    else: suggested_itr = 'ITR-1 or ITR-2 (Verify with CA — no clear salary found)'

    return {
        'income_sources': income_sources, 'deductions': deductions,
        'high_value_credits': high_value_credits, 'monthly_income': dict(monthly_income),
        'real_income_total': round(real_income_total, 2),
        'loan_disbursal_total': round(loan_disbursal_total, 2),
        'family_transfer_total': round(family_transfer_total, 2),
        'self_transfer_total': round(self_transfer_total, 2),
        'total_credits': round(total_credits, 2),
        'salary_total': round(salary_total, 2),
        'freelance_total': round(freelance_total, 2),
        'marketplace_total': round(marketplace_total, 2),
        'interest_total': round(interest_total, 2),
        'section_80c_total': round(min(section_80c_total, 150000), 2),
        'section_80d_total': round(min(section_80d_total, 25000), 2),
        'suggested_itr': suggested_itr, 'high_value_count': len(high_value_credits),
        'other_credits': round(other_total + mb_total + recurring_total, 2),
        'income_breakdown': {
            'Salary': round(salary_total, 2), 'Freelance': round(freelance_total, 2),
            'Marketplace': round(marketplace_total, 2), 'Interest': round(interest_total, 2),
            'Rental': round(rental_total, 2),
            'Recurring/Other': round(recurring_total + other_total + mb_total, 2),
            'Loan Received': round(loan_disbursal_total, 2),
            'Family Transfer': round(family_transfer_total, 2),
            'Self Transfer': round(self_transfer_total, 2),
        }
    }


# ═══════════════════════════════════════════════════════════
#  3. AUDIT & RECONCILIATION
# ═══════════════════════════════════════════════════════════

def analyze_reconciliation(transactions: list) -> dict:
    cheques = []; emis = []; bounced = []; balance_mismatches = []
    duplicate_suspects = []; seen = defaultdict(list)

    for t in transactions:
        desc = (t.get('desc') or '').lower()
        amt = t.get('amount', 0); date = t.get('date', '')
        if any(k in desc for k in CHEQUE_KEYWORDS): cheques.append(t)
        if any(k in desc for k in EMI_KEYWORDS): emis.append(t)
        if any(k in desc for k in BOUNCE_KEYWORDS): bounced.append(t)
        key = (date, round(amt, 0), t.get('type'))
        seen[key].append(t)

    for key, txns in seen.items():
        if len(txns) > 1: duplicate_suspects.extend(txns)

    for i in range(1, len(transactions)):
        prev = transactions[i - 1]; curr = transactions[i]
        if prev.get('balance') and curr.get('balance') and curr.get('amount'):
            expected = round(
                prev['balance'] + curr['amount'] if curr.get('type') == 'CR'
                else prev['balance'] - curr['amount'], 2
            )
            actual = round(curr['balance'], 2)
            if abs(expected - actual) > 1.0:
                balance_mismatches.append({**curr, 'expected_balance': expected,
                                           'diff': round(abs(expected - actual), 2)})

    total_emi = sum(t['amount'] for t in emis if t.get('amount') and t.get('type') == 'DR')
    total_cr  = sum(t['amount'] for t in transactions if t.get('type') == 'CR' and t.get('amount'))

    return {
        'cheques': cheques, 'emis': emis, 'bounced': bounced,
        'balance_mismatches': balance_mismatches,
        'duplicate_suspects': list({id(t): t for t in duplicate_suspects}.values()),
        'cheque_count': len(cheques), 'emi_count': len(emis),
        'total_emi_outflow': round(total_emi, 2),
        'emi_to_income_ratio': round(total_emi / total_cr * 100, 1) if total_cr else 0,
        'bounce_count': len(bounced), 'mismatch_count': len(balance_mismatches),
        'duplicate_count': len(set(id(t) for t in duplicate_suspects)),
        'reconciliation_score': max(0, 100 - len(balance_mismatches) * 5
                                    - len(bounced) * 10 - len(duplicate_suspects) * 3),
    }


# ═══════════════════════════════════════════════════════════
#  4. LOAN & CREDIT ASSESSMENT
# ═══════════════════════════════════════════════════════════

def analyze_loan_eligibility(transactions: list) -> dict:
    if not transactions: return {}
    monthly_data = defaultdict(lambda: {
        'credits': 0, 'real_income': 0, 'debits': 0,
        'min_bal': float('inf'), 'max_bal': 0, 'txn_count': 0
    })
    balances = [t['balance'] for t in transactions if t.get('balance')]
    emis_detected = []

    for t in transactions:
        date = t.get('date', ''); month = date[:7] if date else 'Unknown'
        amt = t.get('amount', 0) or 0; desc = (t.get('desc') or '').lower()
        if t.get('type') == 'CR':
            monthly_data[month]['credits'] += amt
            if not is_loan_disbursal(desc) and not is_family_transfer(desc) and not is_self_transfer(desc):
                monthly_data[month]['real_income'] += amt
        else:
            monthly_data[month]['debits'] += amt
        if t.get('balance'):
            monthly_data[month]['min_bal'] = min(monthly_data[month]['min_bal'], t['balance'])
            monthly_data[month]['max_bal'] = max(monthly_data[month]['max_bal'], t['balance'])
        monthly_data[month]['txn_count'] += 1
        if t.get('type') == 'DR' and any(k in desc for k in EMI_KEYWORDS):
            emis_detected.append(t)

    months = sorted(monthly_data.keys()); n = len(months)
    avg_monthly_credit      = sum(monthly_data[m]['credits'] for m in months) / n if n else 0
    avg_monthly_real_income = sum(monthly_data[m]['real_income'] for m in months) / n if n else 0
    avg_monthly_debit       = sum(monthly_data[m]['debits'] for m in months) / n if n else 0
    avg_balance = sum(balances) / len(balances) if balances else 0
    min_balance = min(balances) if balances else 0
    total_emi   = sum(t['amount'] for t in emis_detected if t.get('amount'))
    monthly_emi = total_emi / n if n else 0
    base_income = avg_monthly_real_income if avg_monthly_real_income > 0 else avg_monthly_credit
    dscr  = round(base_income / monthly_emi, 2) if monthly_emi > 0 else None
    foir  = round((monthly_emi / base_income) * 100, 1) if base_income and monthly_emi else 0
    negative_months       = sum(1 for m in months if monthly_data[m]['min_bal'] < 0)
    max_eligible_emi      = round(base_income * 0.50, 2)
    remaining_emi_capacity= round(max(0, max_eligible_emi - monthly_emi), 2)
    monthly_rate = 0.10 / 12
    loan_eligible = round(remaining_emi_capacity * ((1 - (1 + monthly_rate) ** -60) / monthly_rate), 2) if remaining_emi_capacity > 0 else 0

    if negative_months == 0 and foir < 40 and avg_balance > base_income * 0.5:
        credit_indicator = 'Strong'; credit_color = 'green'
    elif negative_months <= 2 and foir < 60:
        credit_indicator = 'Moderate'; credit_color = 'yellow'
    else:
        credit_indicator = 'Needs Improvement'; credit_color = 'red'

    return {
        'monthly_data': {m: {**v, 'min_bal': v['min_bal'] if v['min_bal'] != float('inf') else 0}
                         for m, v in monthly_data.items()},
        'avg_monthly_credit': round(avg_monthly_credit, 2),
        'avg_monthly_real_income': round(avg_monthly_real_income, 2),
        'avg_monthly_debit': round(avg_monthly_debit, 2),
        'avg_balance': round(avg_balance, 2), 'min_balance': round(min_balance, 2),
        'max_balance': round(max(balances) if balances else 0, 2),
        'monthly_emi': round(monthly_emi, 2), 'foir': foir, 'dscr': dscr,
        'loan_eligible': loan_eligible, 'remaining_emi_capacity': remaining_emi_capacity,
        'negative_months': negative_months, 'credit_indicator': credit_indicator,
        'credit_color': credit_color, 'months_analyzed': n,
        'emis_detected': emis_detected[:10],
    }


# ═══════════════════════════════════════════════════════════
#  5. COMPLIANCE REPORTING (Features 22-24)
# ═══════════════════════════════════════════════════════════

def analyze_compliance(transactions: list) -> dict:
    high_value_txns = []; cash_txns = []; round_figure_txns = []; structured_suspects = []
    daily_cash = defaultdict(float); monthly_cash = defaultdict(float)

    for t in transactions:
        amt = t.get('amount', 0) or 0; desc = (t.get('desc') or '').lower()
        date = t.get('date', ''); month = date[:7] if date else 'Unknown'
        if amt >= HIGH_VALUE_THRESHOLD: high_value_txns.append(t)
        if any(k in desc for k in CASH_KEYWORDS):
            cash_txns.append(t)
            if t.get('type') == 'CR':
                daily_cash[date] += amt; monthly_cash[month] += amt
        if amt >= 10000 and amt % 10000 == 0: round_figure_txns.append(t)

    daily_breaches = [{'date': d, 'amount': round(a, 2)} for d, a in daily_cash.items()
                      if a > CASH_DEPOSIT_DAILY_LIMIT]
    annual_cash_total = sum(daily_cash.values())
    form_61a_required = annual_cash_total >= ANNUAL_CASH_LIMIT

    for t in transactions:
        amt = t.get('amount', 0) or 0
        if 180000 <= amt < 200000: structured_suspects.append(t)

    str_candidates = []; daily_totals = defaultdict(list)
    for t in transactions: daily_totals[t.get('date', '')].append(t)
    for date, txns in daily_totals.items():
        day_total = sum(t.get('amount', 0) for t in txns if t.get('type') == 'CR')
        if day_total >= 500000:
            str_candidates.append({'date': date, 'total': round(day_total, 2), 'count': len(txns)})

    risk_score = 0
    if high_value_txns:     risk_score += len(high_value_txns) * 5
    if daily_breaches:      risk_score += len(daily_breaches) * 10
    if structured_suspects: risk_score += len(structured_suspects) * 15
    if str_candidates:      risk_score += len(str_candidates) * 20
    risk_score = min(risk_score, 100)
    risk_level = 'Low' if risk_score < 20 else 'Medium' if risk_score < 50 else 'High'
    risk_color = 'green' if risk_score < 20 else 'yellow' if risk_score < 50 else 'red'

    return {
        'high_value_txns': high_value_txns[:20], 'cash_txns': cash_txns[:20],
        'round_figure_txns': round_figure_txns[:20], 'structured_suspects': structured_suspects[:10],
        'str_candidates': str_candidates[:10], 'daily_breaches': daily_breaches,
        'annual_cash_total': round(annual_cash_total, 2), 'form_61a_required': form_61a_required,
        'high_value_count': len(high_value_txns), 'cash_count': len(cash_txns),
        'round_figure_count': len(round_figure_txns), 'structured_count': len(structured_suspects),
        'str_count': len(str_candidates), 'risk_score': risk_score,
        'risk_level': risk_level, 'risk_color': risk_color,
        'total_credits': round(sum(t['amount'] for t in transactions if t.get('type') == 'CR' and t.get('amount')), 2),
        'total_debits': round(sum(t['amount'] for t in transactions if t.get('type') == 'DR' and t.get('amount')), 2),
    }


# ═══════════════════════════════════════════════════════════
#  6. GSTR-1 AUTO-FILL
# ═══════════════════════════════════════════════════════════

def analyze_gstr1(transactions: list) -> dict:
    b2b_supplies = []; b2c_supplies = []; export_supplies = []; nil_exempt = []
    B2B_INDICATORS = [
        'pvt', 'ltd', 'llp', 'inc', 'technologies', 'solutions', 'services',
        'enterprises', 'trading', 'industries', 'consultancy', 'consulting',
        'agency', 'associates', 'exports', 'imports', 'retail', 'wholesale',
        'razorpay', 'cashfree', 'payu', 'instamojo', 'tramo', 'eko', 'meesho', 'shiprocket',
    ]
    EXPORT_INDICATORS = ['swift', 'foreign', 'usd', 'eur', 'gbp', 'wire transfer', 'paypal', 'stripe', 'wise', 'remittance']
    NIL_EXEMPT_KW    = ['interest', 'dividend', 'gift', 'subsidy']
    monthly_sales = defaultdict(lambda: {'b2b': 0, 'b2c': 0, 'export': 0, 'nil': 0})

    for t in transactions:
        if t.get('type') != 'CR' or not t.get('amount'): continue
        desc = (t.get('desc') or '').lower(); amt = t['amount']
        date = t.get('date', ''); month = date[:7] if date else 'Unknown'
        if is_loan_disbursal(desc) or is_family_transfer(desc) or is_self_transfer(desc): continue
        if any(k in desc for k in SALARY_KEYWORDS): continue
        if any(k in desc for k in EXPORT_INDICATORS):
            export_supplies.append(t); monthly_sales[month]['export'] += amt
        elif any(k in desc for k in NIL_EXEMPT_KW):
            nil_exempt.append(t); monthly_sales[month]['nil'] += amt
        elif any(k in desc for k in B2B_INDICATORS):
            b2b_supplies.append(t); monthly_sales[month]['b2b'] += amt
        else:
            b2c_supplies.append(t); monthly_sales[month]['b2c'] += amt

    total_b2b = round(sum(t['amount'] for t in b2b_supplies), 2)
    total_b2c = round(sum(t['amount'] for t in b2c_supplies), 2)
    total_taxable = round(total_b2b + total_b2c, 2)

    return {
        'b2b_supplies': b2b_supplies, 'b2c_supplies': b2c_supplies[:50],
        'export_supplies': export_supplies, 'nil_exempt': nil_exempt,
        'total_b2b': total_b2b, 'total_b2c': total_b2c,
        'total_export': round(sum(t['amount'] for t in export_supplies), 2),
        'total_nil': round(sum(t['amount'] for t in nil_exempt), 2),
        'total_taxable': total_taxable,
        'estimated_gst': round(total_taxable * 0.18, 2),
        'monthly_sales': dict(monthly_sales), 'months': sorted(monthly_sales.keys()),
        'b2b_count': len(b2b_supplies), 'b2c_count': len(b2c_supplies),
        'export_count': len(export_supplies),
        'filing_status': 'Filing Required' if total_taxable > 0 else 'Verify with CA',
    }


# ═══════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════

def run_dashboard(transactions: list) -> dict:
    itr_data = analyze_itr(transactions)
    total_cr = round(sum(t['amount'] for t in transactions if t.get('type') == 'CR' and t.get('amount')), 2)
    total_dr = round(sum(t['amount'] for t in transactions if t.get('type') == 'DR' and t.get('amount')), 2)

    return {
        'expense':              analyze_expenses(transactions),
        'itr':                  itr_data,
        'audit':                analyze_reconciliation(transactions),
        'loan':                 analyze_loan_eligibility(transactions),
        'compliance':           analyze_compliance(transactions),
        'income':               analyze_income(transactions),
        'obligations':          analyze_obligations(transactions),
        'cashflow':             analyze_balance_cashflow(transactions),
        'red_flags':            analyze_red_flags(transactions),
        'gstr1':                analyze_gstr1(transactions),
        'total_txns':           len(transactions),
        'total_cr':             total_cr,
        'total_dr':             total_dr,
        'real_income':          itr_data.get('real_income_total', 0),
        'loan_disbursal_total': itr_data.get('loan_disbursal_total', 0),
        'family_transfer_total':itr_data.get('family_transfer_total', 0),
    }
