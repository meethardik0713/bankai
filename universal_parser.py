"""
universal_parser.py
────────────────────
Compatibility wrapper — delegates to the new modular parser system.

app.py continues to import parse_transactions from here.
Internally, all logic now lives in parsers/ and core/.

DO NOT add parsing logic here. Use the appropriate parser in parsers/.
"""

import time
from parsers import detector
from core.verifier import run_accuracy_check


def parse_transactions(pdf_path: str) -> list:
    start = time.time()
    try:
        parser       = detector.detect(pdf_path)
        transactions = parser.parse(pdf_path)

        elapsed = round(time.time() - start, 2)
        print(f"[pipeline] Final transactions: {len(transactions)} ({elapsed}s)")

        if transactions and transactions[0].get('opening_balance') is not None:
            print(f"[pipeline] Opening balance: ₹{transactions[0]['opening_balance']:,.2f}")

        # Post-parse: fill missing balances via running calculation
        ob = transactions[0].get('opening_balance') if transactions else None
        _fill_missing_balances(transactions, ob)

        return transactions

    except Exception as e:
        import traceback
        print(f"[pipeline] Fatal: {e}")
        traceback.print_exc()
        return []


def _fill_missing_balances(transactions: list, opening_balance: float):
    """Fill in any None balances using running balance calculation."""
    prev_bal = opening_balance or 0.0
    for i, txn in enumerate(transactions):
        if txn.get('balance') is None and txn.get('amount') is not None:
            if txn.get('type') == 'CR':
                txn['balance'] = round(prev_bal + txn['amount'], 2)
            else:
                txn['balance'] = round(prev_bal - txn['amount'], 2)
        if txn.get('balance') is not None:
            prev_bal = txn['balance']
            