"""
core/utils.py
──────────────
Shared utility functions used by all parsers.
Single source of truth for amount parsing, date detection, etc.
"""

import re
import pdfplumber
from datetime import datetime

_DATE_PATTERNS = [
    (re.compile(r'\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\b'), ['%d %b %Y', '%d %B %Y']),
    (re.compile(r'\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2})\b'), ['%d %b %y', '%d %B %y']),
    (re.compile(r'\b(\d{2}[A-Za-z]{3}\d{4})\b'),            ['%d%b%Y']),
    (re.compile(r'\b(\d{2}[A-Za-z]{3}\d{2})\b'),            ['%d%b%y']),
    (re.compile(r'\b(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{4})\b'),
     ['%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%m/%d/%Y']),
    (re.compile(r'\b(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2})\b'),
     ['%d/%m/%y', '%d-%m-%y', '%d.%m.%y']),
    (re.compile(r'\b(\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2})\b'),
     ['%Y-%m-%d', '%Y/%m/%d']),
]

_RE_PARENS_NUM  = re.compile(r'^\(([0-9.,]+)\)$')
_RE_AMOUNT_JUNK = re.compile(r'[₹$€£,\s]')
_RE_DR_CR_TAG   = re.compile(r'\s*(dr|cr|DR|CR|Dr|Cr)\.?\s*$')

_RE_OB = re.compile(
    r'(?:opening\s+balance|open(?:ing)?\s+bal\.?|ob\s*:?|'
    r'brought\s+forward|b/?f)\s*[:\-]?\s*([\d,]+\.\d{2})',
    re.IGNORECASE
)


def try_date(text: str):
    """Return raw date string if parseable, else None."""
    text = str(text).strip() if text else ''
    if len(text) < 5 or len(text) > 80:
        return None
    for pattern, fmts in _DATE_PATTERNS:
        m = pattern.search(text)
        if not m:
            continue
        raw = m.group(1) if m.lastindex else m.group()
        for fmt in fmts:
            try:
                dt = datetime.strptime(raw.strip(), fmt)
                if 2000 <= dt.year <= 2035:
                    return raw.strip()
            except ValueError:
                continue
    return None


def parse_amt(text: str) -> float:
    """Parse a string into a float amount. Always returns positive value."""
    if not text:
        return 0.0
    s = str(text).strip()
    m = _RE_PARENS_NUM.match(s)
    if m:
        s = m.group(1)
    s = _RE_AMOUNT_JUNK.sub('', s)
    s = _RE_DR_CR_TAG.sub('', s).strip()
    s = s.replace('-', '').strip()
    try:
        return abs(float(s))
    except (ValueError, TypeError):
        return 0.0


def extract_opening_balance_from_pdf(pdf_path: str) -> float:
    """Scan first 2 pages of PDF text for opening balance. Returns None if not found."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:2]:
                text = page.extract_text() or ''
                m    = _RE_OB.search(text)
                if m:
                    val = parse_amt(m.group(1))
                    if val > 0:
                        print(f"[utils] Opening balance from PDF text: ₹{val:,.2f}")
                        return val
    except Exception:
        pass
    return None


def extract_opening_balance_from_table(pages: list) -> float:
    """Scan table rows for an 'opening balance' row. Returns None if not found."""
    for page in pages:
        for row in page:
            combined = ' '.join(str(c) for c in row).lower()
            if 'opening balance' in combined:
                for cell in row:
                    val = parse_amt(str(cell))
                    if val > 0:
                        print(f"[utils] Opening balance from table: ₹{val:,.2f}")
                        return val
    return None
