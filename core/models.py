"""
core/models.py
──────────────
Single source of truth for transaction schema.
Every bank parser MUST return a list of dicts matching this structure.
"""

# Required keys in every transaction dict
TRANSACTION_KEYS = [
    'date',           # str  — YYYY-MM-DD
    'desc',           # str  — cleaned narration
    'amount',         # float — always positive
    'type',           # str  — 'CR' or 'DR' only
    'balance',        # float — running balance after this txn
    'category',       # str  — from CATEGORY_MAP
    'reference',      # str  — cheque/UTR/ref number (can be '')
    'opening_balance', # float | None — only on first transaction
]


def make_transaction(
    date: str,
    desc: str,
    amount: float,
    txn_type: str,
    balance: float,
    category: str = 'Other',
    reference: str = '',
    opening_balance: float = None,
) -> dict:
    """
    Factory function — always use this to create transaction dicts.
    Ensures consistent structure across all bank parsers.
    """
    return {
        'date':            date,
        'desc':            desc[:200].strip() if desc else '',
        'amount':          round(float(amount), 2),
        'type':            txn_type.upper() if txn_type else 'DR',
        'balance':         round(float(balance), 2) if balance is not None else None,
        'category':        category,
        'reference':       reference or '',
        'opening_balance': round(float(opening_balance), 2) if opening_balance is not None else None,
    }


def validate_transaction(txn: dict) -> list:
    """
    Returns list of validation errors for a transaction dict.
    Empty list = valid.
    """
    errors = []
    if not txn.get('date'):
        errors.append('missing date')
    if not txn.get('amount'):
        errors.append('missing amount')
    if txn.get('balance') is None:
        errors.append('missing balance')
    if txn.get('type') not in ('CR', 'DR'):
        errors.append(f"invalid type: {txn.get('type')}")
    return errors
