"""
parsers/kotak.py
─────────────────
Kotak Mahindra Bank Parser.
Layout: indexed table rows, uses generic table extraction engine.
"""

import pdfplumber
from parsers.base    import BaseParser
from parsers.generic import GenericParser
from core.utils      import extract_opening_balance_from_pdf, extract_opening_balance_from_table


class KotakParser(BaseParser):

    _DETECT_KEYWORDS = ['kotak', 'kotak mahindra', '811']

    def detect_from_text(self, text_low: str) -> bool:
        return any(k in text_low for k in self._DETECT_KEYWORDS)

    def detect(self, pdf_path: str) -> bool:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = (pdf.pages[0].extract_text() or '').lower()
                return self.detect_from_text(text)
        except Exception:
            return False

    def parse(self, pdf_path: str) -> list:
        # Kotak uses the same generic table engine — just delegates
        self._log("Delegating to GenericParser engine")
        return GenericParser().parse(pdf_path)
    