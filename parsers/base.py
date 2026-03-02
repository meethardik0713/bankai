"""
parsers/base.py
───────────────
Abstract base class for all bank parsers.

To add a new bank:
1. Create parsers/newbank.py
2. class NewBankParser(BaseParser)
3. Implement detect() and parse()
4. Register in parsers/detector.py
"""


class BaseParser:
    """
    Every bank parser must inherit from this and implement both methods.
    """

    def detect(self, pdf_path: str) -> bool:
        """
        Return True if this parser can handle the given PDF.
        Should be fast — only read first page text.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement detect()"
        )

    def parse(self, pdf_path: str) -> list:
        """
        Parse the PDF and return a list of transaction dicts.
        Every dict must conform to the schema in core/models.py.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement parse()"
        )

    def _log(self, msg: str):
        bank = self.__class__.__name__.replace('Parser', '').lower()
        print(f"[{bank}] {msg}")
        