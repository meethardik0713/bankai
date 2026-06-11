"""
core/financial_consolidator.py
────────────────────────────────
Multi-source financial consolidation engine.

Takes inputs:
  - Bank statement transactions (already parsed)
  - 26AS data (parsed)
  - AIS data (parsed)
  - Opening balance (manual entry)
  - Form 16 data (optional)

Produces:
  - Reconciled income summary
  - TDS credit vs advance tax
  - Schedule III ready BS + P&L data
  - Discrepancy flags (bank vs 26AS vs AIS)
  - Printable report data
"""

from collections import defaultdict


def consolidate(
    bank_transactions: list,
    data_26as: dict,
    data_ais: dict,
    opening_balance: float = 0.0,
    form16_data: dict = None,
) -> dict:
    """
    Master consolidation function.
    Returns complete financial picture across all sources.
    """
    result = {
        'income':       _reconcile_income(bank_transactions, data_26as, data_ais, form16_data),
        'tds':          _reconcile_tds(bank_transactions, data_26as),
        'bs_pl':        _build_bs_pl(bank_transactions, data_26as, data_ais, opening_balance),
        'discrepancies':_find_discrepancies(bank_transactions, data_26as, data_ais),
        'summary':      {},
        'sources_used': _sources_used(bank_transactions, data_26as, data_ais),
    }
    result['summary'] = _build_master_summary(result, opening_balance)
    return result


# ── Income Reconciliation ─────────────────────────────────

def _reconcile_income(bank_txns, data_26as, data_ais, form16_data):
    """Cross-match income across all sources."""

    sources = {}

    # ── From Bank Statement ────────────────────────────────
    bank_salary      = sum(t['amount'] for t in bank_txns
                           if t.get('type') == 'CR' and t.get('category') == 'Salary')
    bank_interest    = sum(t['amount'] for t in bank_txns
                           if t.get('type') == 'CR' and t.get('category') == 'Interest')
    bank_freelance   = sum(t['amount'] for t in bank_txns
                           if t.get('type') == 'CR' and t.get('category') == 'Freelance Income')
    bank_total_cr    = sum(t['amount'] for t in bank_txns if t.get('type') == 'CR')

    sources['bank'] = {
        'salary':       round(bank_salary, 2),
        'interest':     round(bank_interest, 2),
        'freelance':    round(bank_freelance, 2),
        'total_credits':round(bank_total_cr, 2),
    }

    # ── From 26AS ─────────────────────────────────────────
    s26 = data_26as.get('summary', {}) if data_26as else {}
    sources['form_26as'] = {
        'total_income_declared': s26.get('total_income_declared', 0),
        'salary_tds':            s26.get('salary_tds', 0),
        'non_salary_tds':        s26.get('non_salary_tds', 0),
        'total_tds_deducted':    s26.get('total_tds_deducted', 0),
        'deductors':             data_26as.get('part_a', []) if data_26as else [],
    }

    # ── From AIS ──────────────────────────────────────────
    sais = data_ais.get('summary', {}) if data_ais else {}
    sources['ais'] = {
        'salary_total':    sais.get('salary_total', 0),
        'interest_total':  sais.get('interest_total', 0),
        'dividend_total':  sais.get('dividend_total', 0),
        'rent_total':      sais.get('rent_total', 0),
        'total_income':    sais.get('total_income', 0),
        'gst_turnover':    sais.get('gst_turnover_total', 0),
    }

    # ── From Form 16 (if provided) ─────────────────────────
    sources['form_16'] = form16_data or {}

    # ── Reconciled figures (best available source) ─────────
    # Priority: AIS > 26AS > Bank
    reconciled_salary   = (sais.get('salary_total') or
                           s26.get('total_income_declared') or
                           bank_salary or 0)
    reconciled_interest = (sais.get('interest_total') or bank_interest or 0)
    reconciled_dividend = sais.get('dividend_total', 0)
    reconciled_rent     = sais.get('rent_total', 0)
    reconciled_business = (sais.get('gst_turnover_total') or bank_freelance or 0)

    reconciled_total = (reconciled_salary + reconciled_interest +
                        reconciled_dividend + reconciled_rent + reconciled_business)

    # ── Gaps / Mismatches ──────────────────────────────────
    gaps = []

    # Bank salary vs 26AS declared income
    if sources['form_26as']['total_income_declared'] > 0 and bank_salary > 0:
        diff = abs(sources['form_26as']['total_income_declared'] - bank_salary)
        if diff > 1000:
            gaps.append({
                'type': 'salary_mismatch',
                'bank': bank_salary,
                'form_26as': sources['form_26as']['total_income_declared'],
                'diff': round(diff, 2),
                'message': f'Bank salary credits ₹{bank_salary:,.0f} vs 26AS declared ₹{sources["form_26as"]["total_income_declared"]:,.0f}',
            })

    # AIS vs Bank interest
    if sais.get('interest_total', 0) > 0 and bank_interest > 0:
        diff = abs(sais['interest_total'] - bank_interest)
        if diff > 500:
            gaps.append({
                'type': 'interest_mismatch',
                'bank': bank_interest,
                'ais': sais['interest_total'],
                'diff': round(diff, 2),
                'message': f'Bank interest ₹{bank_interest:,.0f} vs AIS reported ₹{sais["interest_total"]:,.0f}',
            })

    return {
        'sources':          sources,
        'reconciled': {
            'salary':       round(reconciled_salary, 2),
            'interest':     round(reconciled_interest, 2),
            'dividend':     round(reconciled_dividend, 2),
            'rent':         round(reconciled_rent, 2),
            'business':     round(reconciled_business, 2),
            'total':        round(reconciled_total, 2),
        },
        'gaps': gaps,
    }


# ── TDS Reconciliation ────────────────────────────────────

def _reconcile_tds(bank_txns, data_26as):
    """
    Match TDS deducted (from 26AS) vs what should be deducted.
    Also identify advance tax payments from bank.
    """
    tds_from_26as = 0
    if data_26as:
        tds_from_26as = data_26as.get('summary', {}).get('total_tds_deducted', 0)

    # Advance tax from bank (look for IT-related debits)
    advance_tax = sum(
        t['amount'] for t in bank_txns
        if t.get('type') == 'DR' and
        any(kw in (t.get('desc') or '').lower()
            for kw in ['advance tax', 'self assessment', 'income tax', 'tds'])
    )

    # TDS refund received (from 26AS Part D or bank)
    refund_26as = 0
    if data_26as:
        refund_26as = sum(e.get('refund_amount', 0) for e in data_26as.get('part_d', []))

    refund_bank = sum(
        t['amount'] for t in bank_txns
        if t.get('type') == 'CR' and
        any(kw in (t.get('desc') or '').lower()
            for kw in ['income tax refund', 'it refund', 'itr refund'])
    )

    total_tax_credit = tds_from_26as + advance_tax
    net_refund       = refund_26as or refund_bank

    return {
        'tds_from_26as':    round(tds_from_26as, 2),
        'advance_tax_paid': round(advance_tax, 2),
        'total_tax_credit': round(total_tax_credit, 2),
        'refund_received':  round(net_refund, 2),
        'deductors':        data_26as.get('part_a', []) if data_26as else [],
        'tcs':              data_26as.get('part_c', []) if data_26as else [],
    }


# ── Balance Sheet + P&L ───────────────────────────────────

def _build_bs_pl(bank_txns, data_26as, data_ais, opening_balance):
    """
    Build Schedule III-ready BS + P&L figures.

    P&L:
      Income side  — salary, interest, business income
      Expense side — operating expenses, EMI interest portion, depreciation

    Balance Sheet:
      Assets  — bank balance, investments, property
      Liabilities — loans outstanding
    """

    # ── P&L: Income ───────────────────────────────────────
    sais = data_ais.get('summary', {}) if data_ais else {}
    s26  = data_26as.get('summary', {}) if data_26as else {}

    bank_salary   = sum(t['amount'] for t in bank_txns
                        if t.get('type') == 'CR' and t.get('category') == 'Salary')
    bank_interest = sum(t['amount'] for t in bank_txns
                        if t.get('type') == 'CR' and t.get('category') == 'Interest')
    bank_freelance= sum(t['amount'] for t in bank_txns
                        if t.get('type') == 'CR' and
                        t.get('category') in ('Freelance Income', 'Marketplace Income'))

    salary_income   = sais.get('salary_total') or s26.get('total_income_declared') or bank_salary or 0
    interest_income = sais.get('interest_total') or bank_interest or 0
    dividend_income = sais.get('dividend_total') or 0
    rent_income     = sais.get('rent_total') or 0
    business_income = sais.get('gst_turnover_total') or bank_freelance or 0

    total_income = (salary_income + interest_income + dividend_income +
                    rent_income + business_income)

    # ── P&L: Expenses ─────────────────────────────────────
    total_debits     = sum(t['amount'] for t in bank_txns if t.get('type') == 'DR')
    emi_outflow      = sum(t['amount'] for t in bank_txns
                           if t.get('type') == 'DR' and t.get('category') == 'EMI/Loan Repayment')
    personal_expense = sum(t['amount'] for t in bank_txns
                           if t.get('type') == 'DR' and
                           t.get('category') in ('Food', 'Shopping', 'Entertainment',
                                                  'Travel', 'Health', 'Utilities'))
    business_expense = sum(t['amount'] for t in bank_txns
                           if t.get('type') == 'DR' and
                           t.get('category') in ('UPI', 'NEFT Sent', 'Transfer'))

    # Net profit (rough — for sole proprietors)
    net_profit = max(0, business_income - business_expense)

    # ── Balance Sheet ──────────────────────────────────────
    # Current bank balance (closing)
    closing_balance = 0.0
    if bank_txns:
        last_bal = bank_txns[-1].get('balance', 0)
        closing_balance = last_bal or 0.0

    # Investments from AIS
    investments = (sais.get('securities_total', 0) + sais.get('mutual_fund_total', 0))

    # Loans outstanding (estimated from EMI pattern)
    loan_outstanding = emi_outflow * 36  # rough estimate — 3 years remaining

    # ── Schedule III format ────────────────────────────────
    pl_statement = {
        'income': {
            'salary':         round(salary_income, 2),
            'interest':       round(interest_income, 2),
            'dividend':       round(dividend_income, 2),
            'rent':           round(rent_income, 2),
            'business':       round(business_income, 2),
            'total':          round(total_income, 2),
        },
        'expenses': {
            'emi_repayment':  round(emi_outflow, 2),
            'personal':       round(personal_expense, 2),
            'business':       round(business_expense, 2),
            'total':          round(total_debits, 2),
        },
        'net_profit': round(net_profit, 2),
        'net_surplus': round(total_income - total_debits, 2),
    }

    balance_sheet = {
        'assets': {
            'bank_balance':    round(closing_balance, 2),
            'investments':     round(investments, 2),
            'total_assets':    round(closing_balance + investments, 2),
        },
        'liabilities': {
            'loans_estimated': round(loan_outstanding, 2),
            'total_liabilities': round(loan_outstanding, 2),
        },
        'opening_balance': round(opening_balance, 2),
        'closing_balance': round(closing_balance, 2),
        'net_worth':       round((closing_balance + investments) - loan_outstanding, 2),
    }

    return {
        'pl':  pl_statement,
        'bs':  balance_sheet,
        'note': 'Figures are indicative based on bank statement + 26AS + AIS. CA verification required.'
    }


# ── Discrepancy Detection ─────────────────────────────────

def _find_discrepancies(bank_txns, data_26as, data_ais):
    discrepancies = []

    # 1. TDS in 26AS but no salary in bank
    if data_26as:
        s26 = data_26as.get('summary', {})
        bank_salary = sum(t['amount'] for t in bank_txns
                          if t.get('type') == 'CR' and t.get('category') == 'Salary')
        if s26.get('salary_tds', 0) > 0 and bank_salary == 0:
            discrepancies.append({
                'severity': 'medium',
                'type': 'salary_not_in_bank',
                'message': 'Salary TDS found in 26AS but no salary credits in bank statement. Salary may be in different account.',
            })

    # 2. AIS interest vs bank interest
    if data_ais:
        sais = data_ais.get('summary', {})
        bank_interest = sum(t['amount'] for t in bank_txns
                            if t.get('type') == 'CR' and t.get('category') == 'Interest')
        ais_interest = sais.get('interest_total', 0)
        if ais_interest > 0 and bank_interest == 0:
            discrepancies.append({
                'severity': 'low',
                'type': 'interest_not_in_bank',
                'message': f'AIS shows ₹{ais_interest:,.0f} interest income but no interest credits in bank. Check FD/savings accounts.',
            })

    # 3. High value transactions in bank not in AIS/26AS
    high_value_bank = [t for t in bank_txns
                       if t.get('type') == 'CR' and t.get('amount', 0) >= 200000]
    if high_value_bank and not data_ais:
        discrepancies.append({
            'severity': 'high',
            'type': 'high_value_no_ais',
            'message': f'{len(high_value_bank)} high-value credits (≥₹2L) found in bank. Upload AIS to verify if reported to IT.',
        })

    # 4. GST turnover in AIS vs bank
    if data_ais:
        sais = data_ais.get('summary', {})
        gst_ais = sais.get('gst_turnover_total', 0)
        bank_business = sum(t['amount'] for t in bank_txns
                            if t.get('type') == 'CR' and
                            t.get('category') in ('Freelance Income', 'Marketplace Income'))
        if gst_ais > 0 and abs(gst_ais - bank_business) > 50000:
            discrepancies.append({
                'severity': 'high',
                'type': 'gst_mismatch',
                'message': f'AIS GST turnover ₹{gst_ais:,.0f} vs bank business credits ₹{bank_business:,.0f}. Reconcile before filing.',
            })

    return discrepancies


# ── Helpers ───────────────────────────────────────────────

def _sources_used(bank_txns, data_26as, data_ais) -> dict:
    return {
        'bank_statement': len(bank_txns) > 0,
        'form_26as':      bool(data_26as and data_26as.get('part_a')),
        'ais':            bool(data_ais and data_ais.get('summary', {}).get('total_income', 0) > 0),
    }


def _build_master_summary(result, opening_balance) -> dict:
    income = result['income']['reconciled']
    tds    = result['tds']
    bs_pl  = result['bs_pl']

    return {
        'total_income':       income['total'],
        'total_tds_credit':   tds['total_tax_credit'],
        'net_worth':          bs_pl['bs']['net_worth'],
        'closing_bank_bal':   bs_pl['bs']['closing_balance'],
        'opening_balance':    round(opening_balance, 2),
        'discrepancy_count':  len(result['discrepancies']),
        'high_discrepancies': sum(1 for d in result['discrepancies'] if d['severity'] == 'high'),
        'sources_used':       result['sources_used'],
        'filing_readiness':   _filing_readiness(result),
    }


def _filing_readiness(result) -> str:
    high_issues = sum(1 for d in result['discrepancies'] if d['severity'] == 'high')
    sources     = result['sources_used']

    if not sources['form_26as'] and not sources['ais']:
        return 'incomplete'
    if high_issues > 0:
        return 'review_needed'
    if sources['bank_statement'] and sources['form_26as']:
        return 'ready'
    return 'partial'
