"""
core/statement_info.py
Fast, lightweight bank/account detection for the upload UI.
Does NOT run the full transaction parser — just reads first 2 pages
and regex-extracts bank name, account number, type, statement period.
"""

import re
from datetime import datetime
import pdfplumber
from parsers import detector

_BANK_NAMES = {
    'ICICIBankParser':  'ICICI Bank',
    'HDFCParser':       'HDFC Bank',
    'SBIParser':        'State Bank of India',
    'CanaraParser':     'Canara Bank',
    'KotakParser':      'Kotak Mahindra Bank',
    'AxisBankParser':   'Axis Bank',
    'PNBParser':        'Punjab National Bank',
    'BOBParser':        'Bank of Baroda',
    'SaraswatParser':   'Saraswat Co-operative Bank',
    'GenericParser':    'Other / Unrecognized',
}

_DATE_FORMATS = [
    '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y',
    '%d/%m/%y', '%d-%m-%y',
    '%d %b %Y', '%d-%b-%Y', '%d %B %Y',
    '%Y-%m-%d', '%Y/%m/%d',
]


def _normalize_date(raw: str):
    raw = (raw or '').strip().strip(',')
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def _extract_account_number(text: str):
    patterns = [
        r'a/?c\.?\s*(?:no\.?|number)\s*[:\-]?\s*([X\d]{4,20})',
        r'account\s*(?:no\.?|number)\s*[:\-]?\s*([X\d]{4,20})',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def _extract_account_type(text: str):
    m = re.search(r'\b(savings|current|cash credit|overdraft|od)\b', text, re.IGNORECASE)
    if not m:
        return None
    mapping = {
        'savings': 'Savings', 'current': 'Current',
        'cash credit': 'Cash Credit', 'overdraft': 'Overdraft', 'od': 'Overdraft',
    }
    return mapping.get(m.group(1).lower(), m.group(1).title())


_DATE_TOKEN = r'[\d]{1,2}[\s/.\-][A-Za-z]{0,9}[\s/.\-]?\d{2,4}'

def _extract_statement_period(text: str):
    patterns = [
        r'(?:statement period|statement from|period)\s*[:\-]?\s*(' + _DATE_TOKEN + r')\s*(?:to|-|–|—)\s*(' + _DATE_TOKEN + r')',
        r'from\s*(' + _DATE_TOKEN + r')\s*to\s*(' + _DATE_TOKEN + r')',
        # Label-less range, e.g. Kotak: "01 Apr 2025 - 31 Mar 2026"
        r'(' + _DATE_TOKEN + r')\s*(?:to|-|–|—)\s*(' + _DATE_TOKEN + r')',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            start = _normalize_date(m.group(1))
            end   = _normalize_date(m.group(2))
            if start and end:
                return start, end
    return None, None


def extract_statement_info(pdf_path: str) -> dict:
    """Lightweight — first 2 pages only, no full transaction parse."""
    parser    = detector.detect(pdf_path)
    bank_name = _BANK_NAMES.get(parser.__class__.__name__, 'Other / Unrecognized')

    with pdfplumber.open(pdf_path) as pdf:
        full_text = '\n'.join((p.extract_text() or '') for p in pdf.pages[:2])

    account_number      = _extract_account_number(full_text)
    account_type        = _extract_account_type(full_text)
    start_date, end_date = _extract_statement_period(full_text)

    return {
        'bank_name':      bank_name,
        'account_number': account_number,
        'account_type':   account_type or 'Savings',
        'start_date':     start_date,
        'end_date':       end_date,
        'detected':       parser.__class__.__name__ != 'GenericParser',
    }
