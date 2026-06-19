"""
ocr_fallback.py
───────────────
OCR-based parser for image-based / scanned Indian bank statement PDFs.

Handles PDFs where pdfplumber / PyMuPDF return empty text — typically
statements generated via browser "Print to PDF" or scanned originals.

Confirmed working formats:
  • Bank of Baroda  — DATE | NARRATION | CHQ | WITHDRAWAL | DEPOSIT | BALANCE
  • PNB             — TXN NO | TXN DATE | DESCRIPTION | BRANCH | AMOUNT | BALANCE

Usage (drop into parsers/ directory):
    from parsers.ocr_fallback import ocr_parse
    transactions = ocr_parse('/path/to/statement.pdf')

Schema matches universal_parser output:
    { 'date', 'desc', 'amount', 'balance', 'type', 'opening_balance' }

Requirements:
    pip install pytesseract pdf2image
    apt-get install tesseract-ocr poppler-utils
"""

from __future__ import annotations
import re, logging
from collections import Counter
from typing import Optional

logger = logging.getLogger(__name__)

DPI            = 300
CONF_THRESHOLD = 30
ROW_GAP_PX     = 35

DATE_RE   = re.compile(r'^\d{2}-\d{2}-\d{4}$')
AMOUNT_RE = re.compile(r'^[\d,]+\.\d{2}$')

SKIP_PHRASES = {
    'OPENING BALANCE', 'CLOSING BALANCE', 'ABBREVIATION', 'ABBREVIATIONS',
    'STOP PAYMENT', 'ERROR CORRECTED', 'MINIMUM BALANCE',
    'STANDING INSTRUCTIONS', 'OUTWARD BILL', 'ELECTRONIC CLEARING',
    'CHEQUE BOOK', 'RETURNED CHEQUE', 'UNCLEARED EFFECT',
    'IMPORTANT MESSAGES', 'BASE BRANCH', 'CUSTOMER CARE',
    'STATEMENT PERIOD', 'BRANCH DETAILS', 'CUSTOMER DETAILS',
    'ACCOUNT RELATED', 'VIDEO ANSWERS', 'BANKING QUERIES',
    'COMPUTER GENERATED', 'CHEQUE LEAVES', 'MINIMUM AVERAGE',
}

# Markers for type detection
DEBIT_MARKERS = {
    'UPI/DR', 'ATM WDR', 'WDR', 'WITHDRAWAL', 'NEFT OUT',
    'IMPS OUT', 'ECS DR', 'SI DR', 'CHRG', 'CHARGE', 'FEE',
    'PENALTY', 'MAB CHRG', 'INCIDENTAL', 'SMS CHRG',
}
CREDIT_MARKERS = {
    'UPI/CR', 'NEFT IN', 'NEFT_IN', 'IMPS IN', 'BY INST',
    'BY TRANSFER', 'BY CLEARING', 'SALARY', 'REVERSAL',
    'REFUND', 'INT CR', 'FD PROCEEDS',
}

# Table column header keywords → recognized as column positions
TABLE_HEADER_KW = {
    'DATE', 'TXN', 'NO.', 'DESCRIPTION', 'NARRATION', 'PARTICULARS',
    'BRANCH', 'NAME', 'BALANCE', 'REMARKS', 'CHQ.NO.', 'WITHDRAWAL',
    'DEPOSIT', 'CREDIT', 'DEBIT', 'AMOUNT', 'DR', 'CR',
}

NOISE_TOKENS = {
    '-', '|', '\\', 'Cr', 'Dr', 'Cr.', 'Dr.',
    'CR', 'DR', '_', '', 'No', 'No.',
}


# ═══════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════

def ocr_parse(pdf_path: str) -> list[dict]:
    """Parse an image-based bank statement PDF. Returns [] on failure."""
    try:
        pytesseract, Output, convert_from_path = _import_ocr_libs()
    except ImportError as e:
        logger.error("[ocr_fallback] Missing deps: %s", e)
        return []

    logger.info("[ocr_fallback] OCR parsing: %s", pdf_path)
    try:
        pages = convert_from_path(pdf_path, dpi=DPI)
    except Exception as e:
        logger.error("[ocr_fallback] PDF→image failed: %s", e)
        return []

    all_transactions: list[dict] = []
    opening_balance:  Optional[float] = None
    inherited_cols:   Optional[dict]  = None   # carry col layout across pages

    for page_num, page_img in enumerate(pages, start=1):
        try:
            df = _ocr_dataframe(page_img, pytesseract, Output)
            if df is None:
                continue

            cols = _detect_columns(df)

            # Inherit column layout from page 1 if this page has no header row
            if not cols.get('balance'):
                if inherited_cols:
                    cols = inherited_cols.copy()
                else:
                    cols = _infer_cols_from_amounts(df)

            if cols.get('balance') and inherited_cols is None:
                inherited_cols = {k: v for k, v in cols.items()}

            page_txns, ob = _parse_page(df, cols, page_num)
            if ob is not None and opening_balance is None:
                opening_balance = ob
            all_transactions.extend(page_txns)

        except Exception as e:
            logger.warning("[ocr_fallback] Page %d error: %s", page_num, e)
            continue

    _fill_amounts_from_delta(all_transactions, opening_balance)

    if all_transactions and opening_balance is not None:
        all_transactions[0]['opening_balance'] = opening_balance

    logger.info("[ocr_fallback] Extracted %d transactions from %s",
                len(all_transactions), pdf_path)
    return all_transactions


# ═══════════════════════════════════════════════════════════
#  PAGE PARSING
# ═══════════════════════════════════════════════════════════

def _ocr_dataframe(page_img, pytesseract, Output):
    raw = pytesseract.image_to_data(page_img, lang='eng', output_type=Output.DATAFRAME)
    df  = raw[(raw['conf'] > CONF_THRESHOLD) & (raw['text'].str.strip() != '')].copy()
    return df.reset_index(drop=True) if not df.empty else None


def _parse_page(df, cols: dict, page_num: int) -> tuple[list[dict], Optional[float]]:

    bal_col = cols.get('balance', 9999)
    dr_col  = cols.get('withdrawal', 0)
    cr_col  = cols.get('deposit', 0)

    # Find date column X
    all_dates = df[df['text'].str.match(DATE_RE)]
    if all_dates.empty:
        return [], None

    date_col_x = cols.get('date', int(all_dates['left'].mode()[0]))

    # Skip period/header dates by requiring proximity to known date column
    txn_dates = all_dates[all_dates['left'].between(date_col_x - 200, date_col_x + 300)]
    if txn_dates.empty:
        return [], None

    table_top = int(txn_dates.iloc[0]['top']) - 5

    # Group words into visual rows
    table_df = df[df['top'] >= table_top].copy().sort_values(['top', 'left'])
    table_df['row_group'] = (table_df['top'].diff().abs() > ROW_GAP_PX).cumsum()

    rows_by_group: dict[int, list[tuple[int, str]]] = {}
    for rg, grp in table_df.groupby('row_group'):
        rows_by_group[int(rg)] = [
            (int(r['left']), str(r['text']))
            for _, r in grp.sort_values('left').iterrows()
        ]

    # Row groups that begin with a transaction date
    date_groups: set[int] = set(
        table_df[
            table_df['text'].str.match(DATE_RE) &
            table_df['left'].between(date_col_x - 200, date_col_x + 300)
        ]['row_group'].astype(int).tolist()
    )

    # Narration zone: just right of date column up to DR column
    narr_min_x = date_col_x + 80
    narr_max_x = (dr_col - 30) if dr_col else (bal_col - 400)

    transactions:    list[dict]       = []
    opening_balance: Optional[float]  = None
    last_txn:        Optional[dict]   = None
    pending_desc:    list[str]        = []

    for rg in sorted(rows_by_group):
        words    = rows_by_group[rg]
        full_up  = ' '.join(w[1] for w in words).upper()

        # ── Detect and skip header rows ────────────────────────────────────
        header_tokens = sum(1 for _, t in words if t.upper() in TABLE_HEADER_KW)
        total_tokens  = len(words)
        if total_tokens > 0 and header_tokens / total_tokens >= 0.5:
            # More than half the words are column headers — skip
            continue

        # ── Skip known non-transaction phrases ────────────────────────────
        if any(skip in full_up for skip in SKIP_PHRASES):
            if 'OPENING BALANCE' in full_up:
                for _, txt in words:
                    if AMOUNT_RE.match(txt):
                        opening_balance = _amt(txt)
                        break
            if last_txn and pending_desc:
                last_txn['desc'] = _merge(last_txn['desc'], pending_desc)
                pending_desc = []
            continue

        if rg in date_groups:
            # Flush previous transaction's pending desc
            if last_txn and pending_desc:
                last_txn['desc'] = _merge(last_txn['desc'], pending_desc)
                pending_desc = []

            date_val = next(
                (txt for left, txt in words
                 if DATE_RE.match(txt) and abs(left - date_col_x) < 300),
                None
            )
            if not date_val:
                continue

            # Amounts sorted by x position
            all_amts = [(left, _amt(txt)) for left, txt in words if AMOUNT_RE.match(txt)]

            bal_candidates = [a for left, a in all_amts if left >= bal_col - 150 and a is not None]
            balance        = bal_candidates[0] if bal_candidates else None
            if balance is None:
                all_vals = [a for _, a in all_amts if a is not None]
                balance  = all_vals[-1] if all_vals else None

            amt_dr = next((a for left, a in all_amts if dr_col and abs(left - dr_col) < 350 and a is not None), None)
            amt_cr = next((a for left, a in all_amts if cr_col and abs(left - cr_col) < 350 and a is not None), None)

            if amt_dr is None and amt_cr is None:
                non_bal = [a for left, a in all_amts
                           if a is not None and (balance is None or abs(a - balance) > 0.001)]
                if non_bal:
                    amt_dr = non_bal[-1]

            amount = amt_dr or amt_cr

            # Narration
            narr_words = [
                txt for left, txt in words
                if narr_min_x <= left <= narr_max_x
                and not DATE_RE.match(txt)
                and not AMOUNT_RE.match(txt)
                and txt not in NOISE_TOKENS
                and txt.upper() not in TABLE_HEADER_KW
                and not re.match(r'^\$[A-Z0-9]{5,}$', txt)
            ]
            desc = ' '.join(narr_words)

            txn_type = _infer_type(desc, amt_cr, amt_dr, words)

            txn = {'date': date_val, 'desc': desc,
                   'amount': amount, 'balance': balance, 'type': txn_type}
            transactions.append(txn)
            last_txn = txn

        else:
            # Continuation row
            extra = [
                txt for left, txt in words
                if not AMOUNT_RE.match(txt)
                and not DATE_RE.match(txt)
                and txt not in NOISE_TOKENS
                and txt.upper() not in TABLE_HEADER_KW
                and not re.match(r'^\$[A-Z0-9]{5,}$', txt)
                and not re.match(r'^Page$', txt, re.I)
                and not re.match(r'^\d{1,3}$', txt)   # bare page numbers
            ]
            if extra:
                pending_desc.extend(extra)

            if last_txn:
                row_amts = [_amt(txt) for _, txt in words
                            if AMOUNT_RE.match(txt) and _amt(txt) is not None]
                if last_txn['balance'] is None and row_amts:
                    last_txn['balance'] = row_amts[-1]
                if last_txn['amount'] is None and len(row_amts) >= 2:
                    last_txn['amount'] = row_amts[-2]

    if last_txn and pending_desc:
        last_txn['desc'] = _merge(last_txn['desc'], pending_desc)

    # Final type re-inference with full desc
    for txn in transactions:
        du = txn['desc'].upper()
        if any(m in du for m in CREDIT_MARKERS):
            txn['type'] = 'CR'
        elif any(m in du for m in DEBIT_MARKERS):
            txn['type'] = 'DR'

    return transactions, opening_balance


# ═══════════════════════════════════════════════════════════
#  POST-PROCESSING
# ═══════════════════════════════════════════════════════════

def _fill_amounts_from_delta(transactions: list[dict], opening_balance: Optional[float]) -> None:
    """Fill None amounts from balance delta. Handles both chronological and reverse order."""
    if len(transactions) < 2:
        return

    # Detect reverse chronological order (newest first — common in PNB, BOB)
    from datetime import datetime
    reverse_order = False
    try:
        first = datetime.strptime(transactions[0]['date'], '%d-%m-%Y')
        last  = datetime.strptime(transactions[-1]['date'], '%d-%m-%Y')
        if first > last:
            reverse_order = True
    except Exception:
        pass

    if reverse_order:
        transactions.reverse()

    prev_bal = opening_balance
    for txn in transactions:
        bal = txn.get('balance')
        if txn.get('amount') is None and bal is not None and prev_bal is not None:
            delta = round(bal - prev_bal, 2)
            txn['amount'] = abs(delta)
            if delta < 0:
                txn['type'] = 'DR'
            elif delta > 0:
                txn['type'] = 'CR'
        if bal is not None:
            prev_bal = bal

    if reverse_order:
        transactions.reverse()


# ═══════════════════════════════════════════════════════════
#  COLUMN DETECTION
# ═══════════════════════════════════════════════════════════

_COL_KW: dict[str, list[str]] = {
    'date':       ['DATE', 'TXN DATE', 'VALUE DATE'],
    'narration':  ['NARRATION', 'DESCRIPTION', 'PARTICULARS'],
    'withdrawal': ['WITHDRAWAL', 'DEBIT', 'DR AMOUNT'],
    'deposit':    ['DEPOSIT', 'CREDIT', 'CR AMOUNT'],
    'balance':    ['BALANCE'],
}


def _detect_columns(df) -> dict[str, int]:
    cols: dict[str, int] = {}
    text_upper = df['text'].str.upper()
    for col_name, keywords in _COL_KW.items():
        for kw in keywords:
            mask = text_upper == kw
            if mask.any():
                cols[col_name] = int(df[mask].iloc[0]['left'])
                break
    if cols.get('balance') and not cols.get('withdrawal'):
        bx = cols['balance']
        cols.setdefault('deposit',    int(bx * 0.80))
        cols.setdefault('withdrawal', int(bx * 0.62))
    return cols


def _infer_cols_from_amounts(df) -> dict[str, int]:
    """
    When no header row exists (continuation pages): cluster amount x-positions.
    Rightmost cluster = balance, second-rightmost = transaction amount.
    """
    amts = df[df['text'].str.match(AMOUNT_RE)]
    if amts.empty:
        return {}

    x_vals = sorted(amts['left'].tolist())
    clusters: list[list[int]] = []
    current = [x_vals[0]]
    for x in x_vals[1:]:
        if x - current[-1] <= 40:
            current.append(x)
        else:
            clusters.append(current)
            current = [x]
    clusters.append(current)
    clusters.sort(key=lambda c: max(c), reverse=True)

    cols: dict[str, int] = {}
    if clusters:
        cols['balance'] = int(sum(clusters[0]) / len(clusters[0]))
    if len(clusters) >= 2:
        txn_x = int(sum(clusters[1]) / len(clusters[1]))
        cols['withdrawal'] = txn_x
        cols['deposit']    = txn_x
    return cols


# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════

def _amt(s: str) -> Optional[float]:
    try:
        return float(str(s).replace(',', '').strip())
    except (ValueError, AttributeError):
        return None


def _merge(current: str, parts: list[str]) -> str:
    extra  = ' '.join(p for p in parts if p.strip())
    merged = (current + ' ' + extra).strip()
    tokens = merged.split()
    if not tokens:
        return ''
    deduped = [tokens[0]]
    for t in tokens[1:]:
        if t != deduped[-1]:
            deduped.append(t)
    return ' '.join(deduped)


def _infer_type(desc, amt_cr, amt_dr, words) -> str:
    du = desc.upper()
    if any(m in du for m in CREDIT_MARKERS): return 'CR'
    if any(m in du for m in DEBIT_MARKERS):  return 'DR'
    suffixes = [txt for _, txt in words if txt.lower() in ('cr', 'dr', 'cr.', 'dr.')]
    if suffixes:
        return 'CR' if suffixes[-1].upper().startswith('C') else 'DR'
    if amt_cr is not None and amt_dr is None: return 'CR'
    if amt_dr is not None and amt_cr is None: return 'DR'
    return 'CR'


def _import_ocr_libs():
    try:
        import pytesseract
        from pytesseract import Output
        from pdf2image import convert_from_path
        return pytesseract, Output, convert_from_path
    except ImportError as e:
        raise ImportError(
            "pip install pytesseract pdf2image\n"
            "apt-get install tesseract-ocr poppler-utils"
        ) from e
