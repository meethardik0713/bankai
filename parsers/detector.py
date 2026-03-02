"""
parsers/detector.py
────────────────────
Bank detection — reads first page, returns correct parser instance.

To add a new bank: import its parser and add to PARSERS list.
Order matters — more specific checks should come first.
"""

import pdfplumber

from parsers.hdfc    import HDFCParser
from parsers.canara  import CanaraParser
from parsers.sbi     import SBIParser
from parsers.kotak   import KotakParser
from parsers.generic import GenericParser
from parsers.pnb     import PNBParser

# ── Registry — order matters ──────────────────────────────
PARSERS = [
    CanaraParser(),
    HDFCParser(),
    SBIParser(),
    KotakParser(),
    PNBParser(),
]

_FALLBACK = GenericParser()


def detect(pdf_path: str):
    """
    Returns the correct parser instance for the given PDF.
    Falls back to GenericParser if no match found.
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return _FALLBACK
            text     = pdf.pages[0].extract_text() or ''
            text_low = text.lower()
    except Exception:
        return _FALLBACK

    for parser in PARSERS:
        if parser.detect_from_text(text_low):
            print(f"[detector] Matched: {parser.__class__.__name__}")
            return parser

    print("[detector] No match — using GenericParser")
    return _FALLBACK
