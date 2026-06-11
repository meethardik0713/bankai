"""
parsers/form_ais.py
────────────────────
AIS — Annual Information Statement Parser
Introduced in 2021, replaces/supplements 26AS.

AIS has much more detail than 26AS:
  - Salary
  - Interest (savings, FD, recurring)
  - Dividend
  - Securities transactions (shares, MF)
  - Mutual Fund transactions
  - Foreign remittances
  - GST turnover
  - Rent received
  - Purchase of property
  - Foreign travel
  - Credit card spend

Downloaded from incometax.gov.in → AIS tile → PDF/JSON

Two formats exist:
  1. PDF (password protected — password is PAN+DOB DDMMYYYY)
  2. JSON (machine readable — easier to parse)

This parser handles the text-based PDF format.
"""

import re
import pdfplumber
from collections import defaultdict


_AMOUNT_RE = re.compile(r'[\d,]+\.\d{2}')
_DATE_RE   = re.compile(r'\d{2}[-/]\d{2}[-/]\d{4}')
_PAN_RE    = re.compile(r'[A-Z]{5}\d{4}[A-Z]')

# AIS section headers
_SECTION_MAP = {
    'salary':               ['salary', 'wages'],
    'interest':             ['interest from savings', 'interest from deposit', 'interest from others', 'interest on income tax refund'],
    'dividend':             ['dividend'],
    'securities':           ['sale of securities', 'purchase of securities', 'sale of immovable property', 'purchase of immovable property'],
    'mutual_fund':          ['sale of units of mutual fund', 'purchase of units of mutual fund', 'mutual fund'],
    'foreign_remittance':   ['foreign remittance', 'foreign travel'],
    'gst_turnover':         ['gst turnover', 'gst purchases'],
    'rent':                 ['rent received', 'rent payment'],
    'other_income':         ['winnings', 'cash deposit', 'cash withdrawal', 'credit card'],
    'tds':                  ['tax deducted at source', 'tds', 'tcs'],
}


def _parse_amount(s: str) -> float:
    if not s:
        return 0.0
    try:
        return float(str(s).replace(',', '').replace('₹', '').strip())
    except Exception:
        return 0.0


def _clean(s) -> str:
    return str(s).strip() if s else ''


def _detect_section(line: str) -> str:
    line_lower = line.lower()
    for section, keywords in _SECTION_MAP.items():
        for kw in keywords:
            if kw in line_lower:
                return section
    return 'other'


class AISParser:

    def parse(self, pdf_path: str) -> dict:
        """
        Parse AIS PDF. Returns structured dict.
        """
        result = {
            'taxpayer_info':    {},
            'assessment_year':  '',
            'salary':           [],
            'interest':         [],
            'dividend':         [],
            'securities':       [],
            'mutual_fund':      [],
            'foreign':          [],
            'gst_turnover':     [],
            'rent':             [],
            'other_income':     [],
            'tds_tcs':          [],
            'summary':          {},
            'errors':           [],
        }

        try:
            with pdfplumber.open(pdf_path) as pdf:
                full_text = ''
                all_tables = []

                for page in pdf.pages:
                    text = page.extract_text() or ''
                    full_text += text + '\n'
                    tables = page.extract_tables()
                    if tables:
                        all_tables.extend(tables)

                if len(full_text.strip()) < 50:
                    result['errors'].append(
                        'AIS PDF appears to be image-based or password-protected. '
                        'Download from incometax.gov.in and ensure it is not encrypted.'
                    )
                    return result

                result['taxpayer_info']   = self._extract_taxpayer_info(full_text)
                result['assessment_year'] = self._extract_ay(full_text)

                # Parse all sections
                parsed = self._parse_all_sections(full_text, all_tables)
                result.update(parsed)

                result['summary'] = self._build_summary(result)

        except Exception as e:
            import traceback
            result['errors'].append(f'Parse error: {e}')
            traceback.print_exc()

        print(f"[AIS] Parsed: AY={result['assessment_year']}, "
              f"Salary={len(result['salary'])}, "
              f"Interest={len(result['interest'])}, "
              f"Total Income=₹{result['summary'].get('total_income', 0):,.2f}")
        return result

    # ── Taxpayer Info ─────────────────────────────────────

    def _extract_taxpayer_info(self, text: str) -> dict:
        info = {}

        pan_match = _PAN_RE.search(text)
        if pan_match:
            info['pan'] = pan_match.group()

        name_patterns = [
            r'Name\s*[:\-]\s*([A-Z][A-Za-z\s\.]+)',
            r'Taxpayer Name\s*[:\-]\s*([A-Z][A-Za-z\s\.]+)',
        ]
        for pat in name_patterns:
            m = re.search(pat, text)
            if m:
                info['name'] = m.group(1).strip()[:60]
                break

        mobile_m = re.search(r'Mobile\s*[:\-]\s*(\d{10})', text)
        if mobile_m:
            info['mobile'] = mobile_m.group(1)

        email_m = re.search(r'Email\s*[:\-]\s*([^\s]+@[^\s]+)', text, re.IGNORECASE)
        if email_m:
            info['email'] = email_m.group(1)

        return info

    def _extract_ay(self, text: str) -> str:
        m = re.search(r'(20\d{2}\s*[-–]\s*\d{2,4})', text)
        if m:
            return m.group(1).replace(' ', '')
        return ''

    # ── Section Parser ────────────────────────────────────

    def _parse_all_sections(self, text: str, tables: list) -> dict:
        sections = {
            'salary':       [],
            'interest':     [],
            'dividend':     [],
            'securities':   [],
            'mutual_fund':  [],
            'foreign':      [],
            'gst_turnover': [],
            'rent':         [],
            'other_income': [],
            'tds_tcs':      [],
        }

        lines = text.split('\n')
        current_section = 'other_income'

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Detect section change
            detected = _detect_section(line)
            if detected != 'other':
                current_section = detected

            amounts = _AMOUNT_RE.findall(line)
            dates   = _DATE_RE.findall(line)

            if not amounts:
                continue

            entry = {
                'description':  line[:120],
                'amount':       _parse_amount(amounts[0]),
                'reported_by':  '',
                'date':         dates[0] if dates else '',
                'as_displayed': _parse_amount(amounts[0]),
                'as_modified':  _parse_amount(amounts[1]) if len(amounts) > 1 else 0.0,
            }

            if current_section in sections:
                sections[current_section].append(entry)

        # Also parse from tables
        for table in tables:
            if not table or len(table) < 2:
                continue
            self._parse_ais_table(table, sections)

        return sections

    def _parse_ais_table(self, table: list, sections: dict):
        """Parse a single table and add entries to appropriate section."""
        if not table:
            return

        # Detect section from first row
        header_str = ' '.join(_clean(c) for c in table[0]).lower()
        section = _detect_section(header_str)
        if section == 'other':
            section = 'other_income'

        for row in table[1:]:
            cells = [_clean(c) for c in row]
            if not any(cells):
                continue

            amounts = [_parse_amount(c) for c in cells if _parse_amount(c) > 0]
            if not amounts:
                continue

            dates = []
            for c in cells:
                m = _DATE_RE.search(c)
                if m:
                    dates.append(m.group())

            desc = next((c for c in cells if len(c) > 6 and not _AMOUNT_RE.match(c.replace(',', ''))), '')

            entry = {
                'description':  desc[:120],
                'amount':       amounts[0],
                'reported_by':  '',
                'date':         dates[0] if dates else '',
                'as_displayed': amounts[0],
                'as_modified':  amounts[1] if len(amounts) > 1 else 0.0,
            }

            if section in sections:
                sections[section].append(entry)

    # ── Summary ───────────────────────────────────────────

    def _build_summary(self, result: dict) -> dict:
        salary_total    = sum(e['amount'] for e in result['salary'])
        interest_total  = sum(e['amount'] for e in result['interest'])
        dividend_total  = sum(e['amount'] for e in result['dividend'])
        securities_total= sum(e['amount'] for e in result['securities'])
        mf_total        = sum(e['amount'] for e in result['mutual_fund'])
        rent_total      = sum(e['amount'] for e in result['rent'])
        other_total     = sum(e['amount'] for e in result['other_income'])
        gst_total       = sum(e['amount'] for e in result['gst_turnover'])

        total_income = salary_total + interest_total + dividend_total + rent_total

        return {
            'assessment_year':    result['assessment_year'],
            'salary_total':       round(salary_total, 2),
            'interest_total':     round(interest_total, 2),
            'dividend_total':     round(dividend_total, 2),
            'securities_total':   round(securities_total, 2),
            'mutual_fund_total':  round(mf_total, 2),
            'rent_total':         round(rent_total, 2),
            'other_total':        round(other_total, 2),
            'gst_turnover_total': round(gst_total, 2),
            'total_income':       round(total_income, 2),

            # Counts
            'salary_entries':     len(result['salary']),
            'interest_entries':   len(result['interest']),
            'dividend_entries':   len(result['dividend']),
            'securities_entries': len(result['securities']),
            'tds_entries':        len(result['tds_tcs']),

            # Flags for dashboard
            'has_salary':         salary_total > 0,
            'has_business':       gst_total > 0,
            'has_investments':    (securities_total + mf_total) > 0,
            'has_rent':           rent_total > 0,
        }


# ── Module-level convenience ──────────────────────────────

_parser = AISParser()

def parse_ais(pdf_path: str) -> dict:
    """Parse an AIS PDF. Returns structured dict."""
    return _parser.parse(pdf_path)
