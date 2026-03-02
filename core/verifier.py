"""
core/verifier.py
─────────────────
Accuracy verification engine.
Extracted from app.py — now fully independent of Flask.

Usage:
    from core.verifier import run_accuracy_check
    report = run_accuracy_check(transactions)
"""


def run_accuracy_check(transactions: list) -> dict:
    """
    Run all accuracy checks on a parsed transaction list.
    Returns a structured report dict used by accuracy.html template.
    """
    if not transactions:
        return {'error': 'No transactions found.'}

    total = len(transactions)

    missing_date    = sum(1 for t in transactions if not t.get('date'))
    missing_amount  = sum(1 for t in transactions if not t.get('amount'))
    missing_balance = sum(1 for t in transactions if t.get('balance') is None)
    missing_desc    = sum(1 for t in transactions if not t.get('desc'))

    balance_errors = _check_balance_continuity(transactions)

    opening_bal = transactions[0].get('opening_balance') or transactions[0].get('balance')
    closing_bal = transactions[-1].get('balance')

    total_credits = sum(
        t['amount'] for t in transactions
        if t.get('type') == 'CR' and t.get('amount')
    )
    total_debits = sum(
        t['amount'] for t in transactions
        if t.get('type') == 'DR' and t.get('amount')
    )

    # HDFC edge case: account opening entry
    if opening_bal == 0 and transactions[0].get('type') == 'CR':
        total_credits -= transactions[0].get('amount', 0)
        opening_bal    = transactions[0].get('amount', 0)

    calculated_closing, closing_diff, balance_match = _check_closing_balance(
        opening_bal, closing_bal, total_credits, total_debits
    )

    continuity_ok  = (total - 1) - len(balance_errors)
    continuity_pct = round((continuity_ok / (total - 1)) * 100, 1) if total > 1 else 100.0
    fields_ok      = total - max(missing_date, missing_amount, missing_balance)
    fields_pct     = round((fields_ok / total) * 100, 1)
    overall_score  = round((continuity_pct + fields_pct) / 2, 1)

    return {
        'total':              total,
        'missing_date':       missing_date,
        'missing_amount':     missing_amount,
        'missing_balance':    missing_balance,
        'missing_desc':       missing_desc,
        'balance_errors':     balance_errors,
        'opening_bal':        opening_bal,
        'closing_bal':        closing_bal,
        'calculated_closing': calculated_closing,
        'closing_diff':       closing_diff,
        'balance_match':      balance_match,
        'total_credits':      round(total_credits, 2),
        'total_debits':       round(total_debits, 2),
        'continuity_pct':     continuity_pct,
        'fields_pct':         fields_pct,
        'overall_score':      overall_score,
        'error':              None,
    }


# ═══════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════

def _check_balance_continuity(transactions: list) -> list:
    errors = []
    for i in range(1, len(transactions)):
        prev = transactions[i - 1]
        curr = transactions[i]

        prev_bal = prev.get('balance')
        curr_bal = curr.get('balance')
        amount   = curr.get('amount')
        txn_type = curr.get('type', '')

        if prev_bal is None or curr_bal is None or amount is None:
            continue

        if txn_type == 'CR':
            expected = round(prev_bal + amount, 2)
        elif txn_type == 'DR':
            expected = round(prev_bal - amount, 2)
        else:
            continue

        actual = round(curr_bal, 2)
        diff   = round(abs(expected - actual), 2)

        if diff > 1.0:
            errors.append({
                'row':      i + 1,
                'date':     curr.get('date', 'N/A'),
                'desc':     (curr.get('desc') or '')[:40],
                'expected': expected,
                'actual':   actual,
                'diff':     diff,
            })
    return errors


def _check_closing_balance(opening_bal, closing_bal, total_credits, total_debits):
    if opening_bal is None or closing_bal is None:
        return None, None, None

    adjusted_credits = total_credits
    if opening_bal > 0 and abs(
        total_credits - opening_bal - (closing_bal - opening_bal + total_debits)
    ) <= 1.0:
        adjusted_credits = total_credits - opening_bal

    calculated_closing = round(opening_bal + adjusted_credits - total_debits, 2)
    closing_diff       = round(abs(calculated_closing - closing_bal), 2)
    balance_match      = closing_diff <= 1.0

    return calculated_closing, closing_diff, balance_match