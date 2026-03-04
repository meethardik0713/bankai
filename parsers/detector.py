"""
parsers/detector.py
──────────────────────────────────────────────────────────────────────────────
Bank Detection — reads first page of PDF and routes to correct parser.

Detection priority order matters when multiple banks could match.
ICICI checked before Generic to avoid false-positive fallthrough.
──────────────────────────────────────────────────────────────────────────────
"""

import pdfplumber
from parsers.hdfc       import HDFCParser
from parsers.sbi        import SBIParser
from parsers.canara     import CanaraParser
from parsers.kotak      import KotakParser
from parsers.axis_bank  import AxisBankParser
from parsers.pnb        import PNBParser
from parsers.bob        import BOBParser
from parsers.icici_bank import ICICIBankParser
from parsers.generic    import GenericParser


# Ordered list — first match wins
_PARSERS = [
    ICICIBankParser(),   # ← before HDFC to avoid false match
    HDFCParser(),
    SBIParser(),
    CanaraParser(),
    KotakParser(),
    AxisBankParser(),
    PNBParser(),
    BOBParser(),
    GenericParser(),     # Always last — catches everything else
]


def detect(pdf_path: str):
    """
    Detect the correct parser for the given PDF.

    Returns a parser instance. GenericParser is returned if nothing matches.
    Raises RuntimeError only if the file cannot be opened at all.
    """
    try:
        first_page_text = _read_first_page(pdf_path)
    except Exception as e:
        raise RuntimeError(f"Cannot open PDF: {e}") from e

    # Fast keyword-based pre-check before calling detect()
    for parser in _PARSERS:
        try:
            if parser.detect(pdf_path):
                print(f"[detector] Matched: {parser.__class__.__name__}")
                return parser
        except Exception as ex:
            print(f"[detector] {parser.__class__.__name__}.detect() failed: {ex}")
            continue

    # Should never reach here because GenericParser.detect() always returns True
    print("[detector] Fallback: GenericParser")
    return GenericParser()


def _read_first_page(pdf_path: str) -> str:
    """Extract text from first page only (fast detection)."""
    with pdfplumber.open(pdf_path) as pdf:
        if not pdf.pages:
            return ''
        return pdf.pages[0].extract_text() or ''
    