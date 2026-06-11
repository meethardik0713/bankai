"""
parsers/form26as.py
────────────────────
Form 26AS Parser — Annual Tax Statement
Parses TRACES-generated 26AS PDFs from Income Tax portal.

Extracts:
  Part A  — TDS on Salary / Non-Salary
  Part A1 — TDS where TRACES default
  Part B  — TDS on Sale of Property (194IA)
  Part C  — TCS
  Part D  — Paid Refunds
  Part E  — AIR / SFT (high value transactions)
  Part F  — TDS on Rent of Property (194IB)
  Part G  — TDS Defaults

Returns structured dict with all parts + summary.
"""

import re
import pdfplumber
from collections import defaultdict


# ── Regex helpers ─────────────────────────────────────────

_DATE_RE   = re.compile(r'\d{2}-\d{2}-\d{4}|\d{2}/\d{2}/\d{4}')
_AMOUNT_RE = re.compile(r'[\d,]+\.\d{2}')
_TAN_RE    = re.compile(r'[A-Z]{4}\d{5}[A-Z]')
_PAN_RE    = re.compile(r'[A-Z]{5}\d{4}[A-Z]')

# Part header detection
_PART_A_RE  = re.compile(r'PART\s*[-–]?\s*A\b.*?deducted at source', re.IGNORECASE)
_PART_A1_RE = re.compile(r'PART\s*[-–]?\s*A1\b', re.IGNORECASE)
_PART_B_RE  = re.compile(r'PART\s*[-–]?\s*B\b', re.IGNORECASE)
_PART_C_RE  = re.compile(r'PART\s*[-–]?\s*C\b', re.IGNORECASE)
_PART_D_RE  = re.compile(r'PART\s*[-–]?\s*D\b', re.IGNORECASE)
_PART_E_RE  = re.compile(r'PART\s*[-–]?\s*E\b', re.IGNORECASE)
_PART_F_RE  = re.compile(r'PART\s*[-–]?\s*F\b', re.IGNORECASE)
_PART_G_RE  = re.compile(r'PART\s*[-–]?\s*G\b', re.IGNORECASE)


def _parse_amount(s: str) -> float:
    """Parse comma-formatted amount string to float."""
    if not s:
        return 0.0
    try:
        return float(str(s).replace(',', '').replace('₹', '').strip())
    except Exception:
        return 0.0


def _clean(s) -> str:
    if s is None:
        return ''
    return str(s).strip()


# ── Main parser ───────────────────────────────────────────

class Form26ASParser:

    def parse(self, pdf_path: str) -> dict:
        """
        Parse 26AS PDF and return structured data.
        Returns dict with keys: taxpayer_info, part_a, part_b, part_c,
        part_d, part_e, part_f, part_g, summary, assessment_year, errors
        """
        result = {
            'taxpayer_info': {},
            'assessment_year': '',
            'part_a':  [],   # TDS on Salary/Non-Salary
            'part_a1': [],   # TDS defaults
            'part_b':  [],   # TDS on sale of property
            'part_c':  [],   # TCS
            'part_d':  [],   # Paid refunds
            'part_e':  [],   # SFT / AIR high value
            'part_f':  [],   # TDS on rent
            'part_g':  [],   # TDS defaults summary
            'summary': {},
            'errors':  [],
            'raw_text': '',
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

                result['raw_text'] = full_text

                # Extract taxpayer info and AY
                result['taxpayer_info'] = self._extract_taxpayer_info(full_text)
                result['assessment_year'] = self._extract_assessment_year(full_text)

                # Parse each part from tables + text
                result['part_a']  = self._parse_part_a(full_text, all_tables)
                result['part_b']  = self._parse_part_b(full_text, all_tables)
                result['part_c']  = self._parse_part_c(full_text, all_tables)
                result['part_d']  = self._parse_part_d(full_text, all_tables)
                result['part_e']  = self._parse_part_e(full_text, all_tables)
                result['part_f']  = self._parse_part_f(full_text, all_tables)

                # Build summary
                result['summary'] = self._build_summary(result)

        except Exception as e:
            import traceback
            result['errors'].append(f'Parse error: {e}')
            traceback.print_exc()

        print(f"[26AS] Parsed: AY={result['assessment_year']}, "
              f"Part A={len(result['part_a'])} entries, "
              f"Total TDS=₹{result['summary'].get('total_tds_deducted', 0):,.2f}")
        return result

    # ── Taxpayer Info ─────────────────────────────────────

    def _extract_taxpayer_info(self, text: str) -> dict:
        info = {}

        # PAN
        pan_match = _PAN_RE.search(text)
        if pan_match:
            info['pan'] = pan_match.group()

        # Name — usually after "Name :" or "Permanent Account Number"
        name_match = re.search(r'Name\s*[:\-]\s*([A-Z][A-Z\s\.]+)', text)
        if name_match:
            info['name'] = name_match.group(1).strip()[:60]

        # Address
        addr_match = re.search(r'Address\s*[:\-]\s*(.+?)(?:\n|PAN|Assessment)', text, re.IGNORECASE)
        if addr_match:
            info['address'] = addr_match.group(1).strip()[:200]

        return info

    def _extract_assessment_year(self, text: str) -> str:
        ay_match = re.search(r'Assessment\s*Year\s*[:\-]?\s*(\d{4}\s*-\s*\d{2,4})', text, re.IGNORECASE)
        if ay_match:
            return ay_match.group(1).replace(' ', '')
        # Try "AY 2023-24" format
        ay_match2 = re.search(r'\b(20\d{2}-\d{2,4})\b', text)
        if ay_match2:
            return ay_match2.group(1)
        return ''

    # ── Part A: TDS on Salary / Non-Salary ───────────────

    def _parse_part_a(self, text: str, tables: list) -> list:
        """
        Part A columns:
        Sr No | Name of Deductor | TAN | Total Amount Paid | Total TDS Deducted | Total TDS Deposited
        """
        entries = []

        for table in tables:
            if not table or len(table) < 2:
                continue

            # Detect Part A table by header
            header_str = ' '.join(_clean(c) for c in table[0]).lower()
            if not ('deductor' in header_str or 'tan' in header_str):
                continue
            if 'collector' in header_str:  # skip Part C
                continue

            for row in table[1:]:
                if not row or len(row) < 4:
                    continue

                cells = [_clean(c) for c in row]
                combined = ' '.join(cells).lower()

                # Skip header repeats and empty rows
                if 'deductor' in combined or 'total amount' in combined:
                    continue
                if not any(c for c in cells):
                    continue

                # Try to find TAN in any cell
                tan = ''
                for c in cells:
                    m = _TAN_RE.search(c)
                    if m:
                        tan = m.group()
                        break

                # Find amounts (last 3 numeric cells)
                amounts = []
                for c in cells:
                    a = _parse_amount(c)
                    if a > 0:
                        amounts.append(a)

                if not amounts:
                    continue

                # Name of deductor — longest non-numeric, non-TAN cell
                deductor_name = ''
                for c in cells:
                    if (len(c) > 4 and not _TAN_RE.match(c)
                            and not _AMOUNT_RE.match(c.replace(',', ''))
                            and not c.isdigit()):
                        if len(c) > len(deductor_name):
                            deductor_name = c

                entries.append({
                    'sr_no':              cells[0] if cells[0].isdigit() else '',
                    'deductor_name':      deductor_name,
                    'tan':                tan,
                    'amount_paid':        amounts[0] if len(amounts) > 0 else 0.0,
                    'tds_deducted':       amounts[1] if len(amounts) > 1 else 0.0,
                    'tds_deposited':      amounts[2] if len(amounts) > 2 else 0.0,
                    'section':            self._detect_section(deductor_name),
                })

        # Fallback: text-based extraction if no tables found
        if not entries:
            entries = self._parse_part_a_text(text)

        return entries

    def _parse_part_a_text(self, text: str) -> list:
        """Fallback text-based Part A parser."""
        entries = []
        lines = text.split('\n')

        in_part_a = False
        for i, line in enumerate(lines):
            line = line.strip()
            if re.search(r'PART\s*A\b', line, re.IGNORECASE):
                in_part_a = True
                continue
            if in_part_a and re.search(r'PART\s*[B-G]\b', line, re.IGNORECASE):
                break

            if not in_part_a:
                continue

            tan_match = _TAN_RE.search(line)
            if not tan_match:
                continue

            amounts = _AMOUNT_RE.findall(line)
            if not amounts:
                continue

            entries.append({
                'sr_no':         '',
                'deductor_name': line[:60],
                'tan':           tan_match.group(),
                'amount_paid':   _parse_amount(amounts[0]) if amounts else 0.0,
                'tds_deducted':  _parse_amount(amounts[1]) if len(amounts) > 1 else 0.0,
                'tds_deposited': _parse_amount(amounts[2]) if len(amounts) > 2 else 0.0,
                'section':       self._detect_section(line),
            })

        return entries

    # ── Part B: TDS on Sale of Property ──────────────────

    def _parse_part_b(self, text: str, tables: list) -> list:
        entries = []
        in_b = False

        lines = text.split('\n')
        for line in lines:
            if re.search(r'PART\s*[-–]?\s*B\b', line, re.IGNORECASE):
                in_b = True
                continue
            if in_b and re.search(r'PART\s*[-–]?\s*[C-G]\b', line, re.IGNORECASE):
                break
            if not in_b:
                continue

            amounts = _AMOUNT_RE.findall(line)
            if len(amounts) >= 2:
                entries.append({
                    'description': line[:80].strip(),
                    'amount_paid': _parse_amount(amounts[0]),
                    'tds_deducted': _parse_amount(amounts[1]),
                    'tds_deposited': _parse_amount(amounts[2]) if len(amounts) > 2 else 0.0,
                })

        return entries

    # ── Part C: TCS ───────────────────────────────────────

    def _parse_part_c(self, text: str, tables: list) -> list:
        entries = []

        for table in tables:
            if not table or len(table) < 2:
                continue
            header_str = ' '.join(_clean(c) for c in table[0]).lower()
            if 'collector' not in header_str and 'tcs' not in header_str:
                continue

            for row in table[1:]:
                cells = [_clean(c) for c in row]
                if not any(c for c in cells):
                    continue
                amounts = [_parse_amount(c) for c in cells if _parse_amount(c) > 0]
                if not amounts:
                    continue

                tan = ''
                for c in cells:
                    m = _TAN_RE.search(c)
                    if m:
                        tan = m.group()
                        break

                entries.append({
                    'collector_name': next((c for c in cells if len(c) > 4 and not c.isdigit() and not _TAN_RE.match(c)), ''),
                    'tan': tan,
                    'amount_collected': amounts[0] if amounts else 0.0,
                    'tcs_deposited': amounts[1] if len(amounts) > 1 else 0.0,
                })

        return entries

    # ── Part D: Paid Refunds ──────────────────────────────

    def _parse_part_d(self, text: str, tables: list) -> list:
        entries = []
        in_d = False

        lines = text.split('\n')
        for line in lines:
            if re.search(r'PART\s*[-–]?\s*D\b', line, re.IGNORECASE):
                in_d = True
                continue
            if in_d and re.search(r'PART\s*[-–]?\s*[E-G]\b', line, re.IGNORECASE):
                break
            if not in_d:
                continue

            amounts = _AMOUNT_RE.findall(line)
            dates = _DATE_RE.findall(line)
            if amounts:
                entries.append({
                    'description': line[:80].strip(),
                    'refund_amount': _parse_amount(amounts[0]),
                    'interest': _parse_amount(amounts[1]) if len(amounts) > 1 else 0.0,
                    'date': dates[0] if dates else '',
                })

        return entries

    # ── Part E: SFT / High Value Transactions ────────────

    def _parse_part_e(self, text: str, tables: list) -> list:
        """
        Part E (SFT) — Annual Information Report high-value transactions.
        SFT Code | Filer Name | Amount | Remarks
        """
        entries = []
        in_e = False

        lines = text.split('\n')
        for line in lines:
            if re.search(r'PART\s*[-–]?\s*E\b', line, re.IGNORECASE):
                in_e = True
                continue
            if in_e and re.search(r'PART\s*[-–]?\s*[F-G]\b', line, re.IGNORECASE):
                break
            if not in_e:
                continue

            amounts = _AMOUNT_RE.findall(line)
            if amounts and len(line.strip()) > 10:
                entries.append({
                    'description': line[:100].strip(),
                    'amount': _parse_amount(amounts[0]),
                    'transaction_date': _DATE_RE.search(line).group() if _DATE_RE.search(line) else '',
                })

        return entries

    # ── Part F: TDS on Rent ───────────────────────────────

    def _parse_part_f(self, text: str, tables: list) -> list:
        entries = []
        in_f = False

        lines = text.split('\n')
        for line in lines:
            if re.search(r'PART\s*[-–]?\s*F\b', line, re.IGNORECASE):
                in_f = True
                continue
            if in_f and re.search(r'PART\s*[-–]?\s*G\b', line, re.IGNORECASE):
                break
            if not in_f:
                continue

            amounts = _AMOUNT_RE.findall(line)
            if amounts:
                entries.append({
                    'description': line[:80].strip(),
                    'rent_amount': _parse_amount(amounts[0]),
                    'tds_deducted': _parse_amount(amounts[1]) if len(amounts) > 1 else 0.0,
                })

        return entries

    # ── Summary Builder ───────────────────────────────────

    def _build_summary(self, result: dict) -> dict:
        total_tds_deducted  = sum(e.get('tds_deducted', 0) for e in result['part_a'])
        total_tds_deposited = sum(e.get('tds_deposited', 0) for e in result['part_a'])
        total_tcs           = sum(e.get('tcs_deposited', 0) for e in result['part_c'])
        total_refund        = sum(e.get('refund_amount', 0) for e in result['part_d'])
        total_sft_amount    = sum(e.get('amount', 0) for e in result['part_e'])

        # Salary vs Non-Salary TDS split
        salary_tds    = sum(e.get('tds_deducted', 0) for e in result['part_a']
                            if e.get('section') == '192')
        non_salary_tds= sum(e.get('tds_deducted', 0) for e in result['part_a']
                            if e.get('section') != '192')

        # Total income as declared in 26AS
        total_income_declared = sum(e.get('amount_paid', 0) for e in result['part_a'])

        return {
            'assessment_year':        result['assessment_year'],
            'total_tds_deducted':     round(total_tds_deducted, 2),
            'total_tds_deposited':    round(total_tds_deposited, 2),
            'total_tcs':              round(total_tcs, 2),
            'total_refund':           round(total_refund, 2),
            'total_sft_amount':       round(total_sft_amount, 2),
            'salary_tds':             round(salary_tds, 2),
            'non_salary_tds':         round(non_salary_tds, 2),
            'total_income_declared':  round(total_income_declared, 2),
            'tds_deductors_count':    len(result['part_a']),
            'sft_transactions_count': len(result['part_e']),
            'has_refund':             total_refund > 0,
        }

    def _detect_section(self, text: str) -> str:
        """Detect TDS section from deductor name / description."""
        text_lower = text.lower()
        if 'salary' in text_lower or '192' in text:
            return '192'
        if '194j' in text_lower or 'professional' in text_lower:
            return '194J'
        if '194c' in text_lower or 'contract' in text_lower:
            return '194C'
        if '194a' in text_lower or 'interest' in text_lower:
            return '194A'
        if '194b' in text_lower or 'lottery' in text_lower:
            return '194B'
        if '194h' in text_lower or 'commission' in text_lower:
            return '194H'
        if '194i' in text_lower or 'rent' in text_lower:
            return '194I'
        return 'other'


# ── Module-level convenience function ─────────────────────

_parser = Form26ASParser()

def parse_26as(pdf_path: str) -> dict:
    """Parse a 26AS PDF. Returns structured dict."""
    return _parser.parse(pdf_path)
