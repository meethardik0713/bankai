"""
Universal Bank Statement Parser
─────────────────────────────────
Tuned from Kotak Mahindra Bank statement debug output.

Key fixes applied:
  1. Skip merged title rows (e.g. "Savings Account Transactions")
  2. Handle null cells from pdfplumber
  3. Clean \\n inside cells before any processing
  4. Skip repeated header rows on every page
  5. Skip reference column (Chq/Ref. No.) correctly
  6. Handle Value Date noise in descriptions
  7. Fixed de-duplication — use full desc + balance as key

Bug fixes (Phase 1):
  B1. _find_amts: len==2 now correctly returns both debit AND credit
  B2. _looks_like_ref: regex fixed to match UPI-/NEFT- prefixed refs
  B3. Date normalized to YYYY-MM-DD in dedup key (prevents false dupes)
  B4. _parse_row: opening-balance skip narrowed — only drops '-' index,
      not blank index, so banks without row numbers still parse
  B5. 'reference' added to output dict (was extracted but thrown away)
"""

import re
import time
import pdfplumber
from datetime import datetime
from collections import Counter

# ═══════════════════════════════════════════════════════════
#  PRE-COMPILED PATTERNS
# ═══════════════════════════════════════════════════════════

_DATE_PATTERNS = [
    # DD Mon YYYY  ← Kotak primary format e.g. "03 Apr 2025"
    (re.compile(r'\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\b'),
     ['%d %b %Y', '%d %B %Y']),

    # DD Mon YY
    (re.compile(r'\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2})\b'),
     ['%d %b %y', '%d %B %y']),

    # DDMonYYYY  e.g. "03Apr2025"
    (re.compile(r'\b(\d{2}[A-Za-z]{3}\d{4})\b'),
     ['%d%b%Y']),

    # DDMonYY
    (re.compile(r'\b(\d{2}[A-Za-z]{3}\d{2})\b'),
     ['%d%b%y']),

    # DD/MM/YYYY  DD-MM-YYYY  DD.MM.YYYY
    (re.compile(r'\b(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{4})\b'),
     ['%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%m/%d/%Y']),

    # DD/MM/YY
    (re.compile(r'\b(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2})\b'),
     ['%d/%m/%y', '%d-%m-%y', '%d.%m.%y']),

    # YYYY-MM-DD
    (re.compile(r'\b(\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2})\b'),
     ['%Y-%m-%d', '%Y/%m/%d']),
]

_RE_VALUE_DATE  = re.compile(
    r'\(?\s*[Vv]alue\s+[Dd]ate\s*:\s*[\d\-/\.]+\s*\)?'
)
_RE_AMOUNT_JUNK = re.compile(r'[₹$€£,\s]')
_RE_DR_CR_TAG   = re.compile(r'\s*(dr|cr|DR|CR|Dr|Cr)\.?\s*$')
_RE_MULTI_SPACE = re.compile(r'\s{2,}')
_RE_WHITESPACE  = re.compile(r'\s+')
_RE_PARENS_NUM  = re.compile(r'^\(([0-9.,]+)\)$')

# ═══════════════════════════════════════════════════════════
#  COLUMN KEYWORDS
# ═══════════════════════════════════════════════════════════

_COL_KEYWORDS = {
    'date': [
        'date', 'txn date', 'txn dt', 'trans date', 'transaction date',
        'value date', 'posting date', 'entry date', 'book date',
    ],
    'description': [
        'description', 'narration', 'particulars', 'details',
        'transaction details', 'txn description', 'remarks',
        'narrative', 'desc', 'transaction particulars',
        'narration/chq. details',
    ],
    'debit': [
        'debit', 'debit amount', 'debit(rs)', 'debit (rs)', 'debit(inr)',
        'withdrawal', 'withdrawals', 'withdrawal amount', 'withdrawal(rs)',
        'withdrawal (dr.)', 'withdrawal (dr)', 'withdrawal(dr)',
        'dr amount', 'debit amt', 'dr', 'paid out', 'debit(₹)',
    ],
    'credit': [
        'credit', 'credit amount', 'credit(rs)', 'credit (rs)', 'credit(inr)',
        'deposit', 'deposits', 'deposit amount', 'deposit(rs)',
        'deposit (cr.)', 'deposit (cr)', 'deposit(cr)',
        'cr amount', 'credit amt', 'cr', 'paid in', 'credit(₹)',
    ],
    'balance': [
        'balance', 'closing balance', 'running balance',
        'available balance', 'bal', 'running bal', 'closing bal',
        'balance(inr)', 'balance(rs)', 'balance(₹)',
    ],
    'amount': [
        'amount', 'txn amount', 'transaction amount', 'amt',
        'amount(rs)', 'amount(inr)', 'amount (rs)', 'amount(₹)',
    ],
    'reference': [
        'chq', 'chq no', 'chq/ref', 'chq/ref no', 'chq/ref. no.',
        'cheque no', 'cheque number', 'ref no', 'ref number',
        'reference', 'reference no', 'transaction id', 'txn id',
        'instrument no', 'utr', 'chq./ref. no.',
    ],
}

_SKIP_PHRASES = [
    'opening balance', 'closing balance', 'brought forward',
    'carried forward', 'page total', 'grand total',
    'statement summary', 'this is a computer',
    'generated on', 'printed on', 'account summary',
    'nominee', 'ifsc code', 'end of statement',
]

_TITLE_PHRASES = [
    'savings account transactions',
    'current account transactions',
    'account transactions',
    'transaction details',
    'statement of account',
]

_HEADER_MARKER_CELLS = {'date', 'description', 'narration',
                        'particulars', 'withdrawal', 'deposit',
                        'balance', 'debit', 'credit'}

_CATEGORY_MAP = {
    'UPI':           ['upi/', 'upi-', 'phonepe', 'gpay', 'google pay',
                      'paytm', 'amazonpay', 'bhim'],
    'NEFT/RTGS':     ['neft', 'rtgs', 'neftinw'],
    'IMPS':          ['imps'],
    'ATM/Cash':      ['atm', 'cash withdrawal', 'cash wdl', 'cwdr',
                      'cash deposit'],
    'Salary':        ['salary', 'payroll', 'sal cr', 'wages'],
    'EMI/Loan':      ['pocketly', 'speel finance', 'stucred', 'mpokket',
                      'branch internat', 'truecredit', 'lazypay',
                      'snapmint', 'emi', 'loan'],
    'POS':           ['pos ', 'point of sale', 'pci/'],
    'Interest':      ['interest', 'int.pd', 'int pd', 'int cr', 'int.pd:'],
    'Charges':       ['charges', 'fee', 'commission', 'gst',
                      'service charge', 'sms alert', 'annual fee', 'chrg:'],
    'Transfer':      ['transfer', 'trf ', 'fund transfer',
                      'mb:sent', 'mb:received'],
    'Cheque':        ['cheque', 'chq', 'clearing', 'cts'],
    'Food':          ['swiggy', 'zomato', 'blinkit', 'zepto', 'dominos',
                      'mcdonalds', 'pizza', 'swad sadan', 'shreejee',
                      'bikaner', 'gianis', 'dosa'],
    'Shopping':      ['amazon', 'flipkart', 'myntra', 'meesho', 'ekart',
                      'westside', 'snitch', 'zudio'],
    'Entertainment': ['netflix', 'spotify', 'zee5', 'jiohotstar',
                      'google play', 'steam', 'valve'],
    'Travel':        ['aeronfly', 'irctc', 'makemytrip', 'redbus'],
}


# ═══════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

def parse_transactions(pdf_path: str) -> list:
    start = time.time()
    try:
        pages = _extract_pdf(pdf_path)
        if not pages:
            print("[parser] No content extracted from PDF")
            return []

        print(f"[parser] Pages extracted: {len(pages)}")

        col_map, hdr_row, hdr_page = _detect_columns(pages)
        print(f"[parser] col_map  : {col_map}")
        print(f"[parser] hdr_row  : {hdr_row}  hdr_page: {hdr_page}")

        if not col_map:
            print("[parser] Column detection failed")
            return []

        raw = _extract_rows(pages, col_map, hdr_row, hdr_page)
        print(f"[parser] Raw transactions: {len(raw)}")
        if not raw:
            return []

        transactions = _normalize(raw)
        print(f"[parser] Final transactions: {len(transactions)}"
              f"  ({time.time()-start:.2f}s)")
        return transactions

    except Exception as e:
        import traceback
        print(f"[parser] Fatal: {e}")
        traceback.print_exc()
        return []


# ═══════════════════════════════════════════════════════════
#  STAGE 1 — PDF EXTRACTION
# ═══════════════════════════════════════════════════════════

def _extract_pdf(pdf_path: str) -> list:
    """
    Single-pass extraction: open PDF once, extract table rows AND raw text
    per page in the same loop. This avoids re-opening and re-reading the PDF
    a second time for footer row recovery, cutting parse time roughly in half.
    """
    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        print(f"[parser] Cannot open: {e}")
        return []

    all_pages     = []
    best_strategy = None
    seen_indices  = set()

    for pg_num, page in enumerate(pdf.pages):
        # Grab raw text NOW while page is already loaded in memory
        try:
            page_text = page.extract_text() or ''
        except Exception:
            page_text = ''

        rows, strategy = _extract_page(page, best_strategy)
        if rows:
            all_pages.append(rows)
            if best_strategy is None and strategy:
                best_strategy = strategy
                print(f"[parser] Strategy={strategy} (page {pg_num+1})")

            for row in rows:
                idx = str(row[0]).strip() if row and row[0] else ''
                if idx.isdigit():
                    seen_indices.add(int(idx))

            # Pass pre-fetched text so _recover_footer_rows doesn't re-read
            extra = _recover_footer_rows_from_text(page_text, seen_indices)
            if extra:
                print(f"[parser] Recovered {len(extra)} footer rows on page {pg_num+1}")
                all_pages[-1].extend(extra)
                all_pages[-1].sort(key=lambda r: int(r[0]) if r and str(r[0]).strip().isdigit() else 0)
                for row in extra:
                    idx = str(row[0]).strip() if row and row[0] else ''
                    if idx.isdigit():
                        seen_indices.add(int(idx))

    pdf.close()
    return all_pages


def _recover_footer_rows(page, seen_indices: set) -> list:
    """Legacy wrapper — prefer _recover_footer_rows_from_text for speed."""
    try:
        text = page.extract_text() or ''
    except Exception:
        text = ''
    return _recover_footer_rows_from_text(text, seen_indices)


def _recover_footer_rows_from_text(text: str, seen_indices: set) -> list:
    """
    Parse raw text lines to recover transaction rows that pdfplumber table
    extractor missed (they fall below the detected table boundary at page bottom).
    Only recovers rows whose numeric index is NOT already in seen_indices.
    Accepts pre-fetched text string to avoid re-reading the page from disk.
    """
    try:
        lines = text.splitlines()
    except Exception:
        return []

    # Matches: <index> <DD Mon YYYY> <rest of line>
    _RE_TXN_START = re.compile(
        r'^(\d+)\s+(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})\s+(.+)$'
    )
    # Amount: comma-separated digits with 2 decimal places
    _RE_AMOUNT = re.compile(r'[\d,]+\.\d{2}')

    recovered = []
    pending   = None

    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Skip page footer/header noise
        if re.search(r'page\s+\d+\s+of\s+\d+', line, re.I):
            continue
        if re.search(r'statement generated|account no|account statement', line, re.I):
            continue

        m = _RE_TXN_START.match(line)
        if m:
            idx = int(m.group(1))
            if idx not in seen_indices:
                # Save previous pending row
                if pending:
                    recovered.append(_build_recovered_row(pending, _RE_AMOUNT))

                pending = {
                    'idx':  str(idx),
                    'date': m.group(2),
                    'rest': m.group(3),
                }
            else:
                # This index is already captured — flush any pending and stop
                if pending:
                    recovered.append(_build_recovered_row(pending, _RE_AMOUNT))
                    pending = None
        elif pending:
            # Continuation line for current pending row
            # Stop if line starts with a new valid transaction
            if not re.match(r'^\d+\s+\d{1,2}\s+[A-Za-z]{3}\s+\d{4}', line):
                pending['rest'] = pending['rest'] + ' ' + line

    if pending:
        recovered.append(_build_recovered_row(pending, _RE_AMOUNT))

    return recovered


def _build_recovered_row(pending: dict, re_amount) -> list:
    """
    From a pending dict {idx, date, rest}, extract amounts and build
    a 7-column row: [idx, date, desc, ref, withdrawal, deposit, balance]

    The 'rest' string looks like:
      "UPI/SOMEONE/123/UPI UPI-987654321 500.00 1,234.56"
    Last amount = balance, second-to-last = debit or credit (need sign from context).
    We can't determine DR/CR from text alone, so we put the non-balance amount
    in withdrawal column and leave deposit empty; _normalize() will correct via balance diff.
    """
    rest   = pending['rest'].strip()
    amounts = re_amount.findall(rest)

    balance    = ''
    withdrawal = ''
    deposit    = ''

    if len(amounts) >= 2:
        balance    = amounts[-1]
        withdrawal = amounts[-2]   # may be corrected to deposit by _normalize()
    elif len(amounts) == 1:
        balance = amounts[0]

    # Description: everything before the first amount (or the whole rest)
    if amounts:
        first_amt_pos = rest.find(amounts[0])
        desc_part = rest[:first_amt_pos].strip()
    else:
        desc_part = rest

    # Try to pull reference number from desc_part (last token that looks like a ref)
    ref = ''
    tokens = desc_part.split()
    if tokens:
        last = tokens[-1]
        if re.match(r'^[A-Z0-9]{2,12}[-/]\d{5,}$', last) or re.match(r'^[A-Z]{2,10}\d{6,}$', last):
            ref = last
            desc_part = ' '.join(tokens[:-1])

    return [
        pending['idx'],
        pending['date'],
        desc_part.strip(),
        ref,
        withdrawal,
        deposit,
        balance,
    ]


def _extract_page(page, preferred: str = None) -> tuple:
    strategies = [
        ('table',         _try_table),
        ('table_relaxed', _try_table_relaxed),
        ('words',         _try_words),
        ('lines',         _try_lines),
    ]

    if preferred:
        for name, fn in strategies:
            if name == preferred:
                rows = fn(page)
                if rows and _good_rows(rows) >= 1:
                    return rows, name

    for name, fn in strategies:
        rows = fn(page)
        if rows and _good_rows(rows) >= 1:
            return rows, name

    return [], None


def _try_table(page) -> list:
    try:
        tables = page.extract_tables()
        if tables:
            return _clean_table(max(tables, key=len))
    except Exception:
        pass
    return []


def _try_table_relaxed(page) -> list:
    try:
        tables = page.extract_tables({
            "vertical_strategy":   "text",
            "horizontal_strategy": "text",
            "snap_tolerance":      5,
            "join_tolerance":      5,
        })
        if tables:
            return _clean_table(max(tables, key=len))
    except Exception:
        pass
    return []


def _try_words(page) -> list:
    try:
        words = page.extract_words(x_tolerance=3, y_tolerance=3)
        if not words:
            return []
        lines = {}
        for w in words:
            y = round(w['top'] / 3) * 3
            lines.setdefault(y, []).append(w)
        rows = []
        for y in sorted(lines):
            lw = sorted(lines[y], key=lambda w: w['x0'])
            merged = []
            for w in lw:
                if merged and (w['x0'] - merged[-1]['x1']) < 15:
                    merged[-1]['text'] += ' ' + w['text']
                    merged[-1]['x1']    = w['x1']
                else:
                    merged.append({'text': w['text'],
                                   'x0':   w['x0'],
                                   'x1':   w['x1']})
            cells = [m['text'].strip() for m in merged if m['text'].strip()]
            if cells:
                rows.append(cells)
        return rows
    except Exception:
        return []


def _try_lines(page) -> list:
    try:
        text = page.extract_text() or ''
        rows = []
        for line in text.split('\n'):
            line = line.strip()
            if not line:
                continue
            cells = [c.strip()
                     for c in _RE_MULTI_SPACE.split(line) if c.strip()]
            if cells:
                rows.append(cells)
        return rows
    except Exception:
        return []


def _clean_table(table: list) -> list:
    out = []
    for row in table:
        cleaned = []
        for cell in row:
            if cell is None:
                cleaned.append('')
            else:
                cleaned.append(
                    str(cell).replace('\n', ' ').replace('\r', '').strip()
                )
        out.append(cleaned)
    return out


def _good_rows(rows: list) -> int:
    return sum(1 for r in rows
               if sum(1 for c in r if str(c).strip()) >= 3)


# ═══════════════════════════════════════════════════════════
#  STAGE 2 — COLUMN DETECTION
# ═══════════════════════════════════════════════════════════

def _detect_columns(pages: list) -> tuple:
    for pg_idx, page in enumerate(pages):
        for row_idx, row in enumerate(page):
            if _is_title_row(row):
                continue
            # NOTE: do NOT skip _is_header_row_repeat here —
            # the very first header IS a "header row repeat" by definition.
            # We detect it via _match_header scoring instead.
            cmap = _match_header(row)
            if cmap and _valid_map(cmap):
                print(f"[parser] Header pg={pg_idx} row={row_idx}: {row}")
                return cmap, row_idx, pg_idx

    for pg_idx, page in enumerate(pages):
        for row_idx in range(len(page) - 1):
            merged = _merge_rows(page[row_idx], page[row_idx + 1])
            cmap   = _match_header(merged)
            if cmap and _valid_map(cmap):
                print(f"[parser] Merged header pg={pg_idx} "
                      f"rows={row_idx}-{row_idx+1}")
                return cmap, row_idx + 1, pg_idx

    print("[parser] No header — inferring columns")
    return _infer_columns(pages)


def _is_title_row(row: list) -> bool:
    non_empty = [c.strip() for c in row if str(c).strip()]
    if len(non_empty) != 1:
        return False
    return non_empty[0].lower() in _TITLE_PHRASES


def _is_header_row_repeat(row: list) -> bool:
    hits = 0
    for cell in row:
        if str(cell).strip().lower() in _HEADER_MARKER_CELLS:
            hits += 1
        if hits >= 3:
            return True
    return False


def _norm(text: str) -> str:
    t = str(text).lower()
    t = re.sub(r'[^a-z0-9 ./]', ' ', t)
    return re.sub(r'\s+', ' ', t).strip()


def _match_header(row: list) -> dict:
    if len(row) < 3:
        return {}

    cmap = {}

    for idx, cell in enumerate(row):
        raw = str(cell).strip()
        if not raw:
            continue

        lo = raw.lower().strip('.')
        if lo in ('#', 's no', 'sno', 'sr', 'sr no', 'srno',
                  'sl no', 'slno', 'no', 'item', 'sl.no.', 'sr.no.'):
            cmap.setdefault('_index', idx)
            continue

        norm = _norm(raw)
        best_role, best_score = None, 0

        for role, keywords in _COL_KEYWORDS.items():
            for kw in keywords:
                kw_n = _norm(kw)
                if norm == kw_n:
                    score = 200
                elif norm.startswith(kw_n):
                    score = 100 + len(kw_n)
                elif kw_n in norm:
                    score = 50 + len(kw_n)
                else:
                    continue
                if score > best_score:
                    best_score = score
                    best_role  = role

        if best_role and best_role not in cmap:
            cmap[best_role] = idx

    return cmap


def _valid_map(cmap: dict) -> bool:
    return ('date' in cmap and
            any(k in cmap for k in ('debit', 'credit', 'amount')))


def _merge_rows(a: list, b: list) -> list:
    n = max(len(a), len(b))
    return [
        ((str(a[i]).strip() if i < len(a) else '') + ' ' +
         (str(b[i]).strip() if i < len(b) else '')).strip()
        for i in range(n)
    ]


def _infer_columns(pages: list) -> tuple:
    all_rows = [r for p in pages for r in p]
    if len(all_rows) < 3:
        return {}, 0, 0

    widths = Counter(len(r) for r in all_rows)
    target = widths.most_common(1)[0][0]
    sample = [r for r in all_rows if len(r) == target][:40]
    if len(sample) < 3:
        return {}, 0, 0

    cmap, num_cols = {}, []

    for ci in range(target):
        vals  = [str(r[ci]) for r in sample]
        n     = len(sample)
        dates = sum(1 for v in vals if _try_date(v))
        nums  = sum(1 for v in vals if _is_num(v))
        texts = sum(1 for v in vals
                    if len(v.strip()) > 8
                    and not _is_num(v)
                    and not _try_date(v))
        idxs  = sum(1 for v in vals if re.match(r'^[\d\-]+$', v.strip()))

        if idxs / n > 0.6:
            cmap.setdefault('_index', ci)
        elif dates / n > 0.4 and 'date' not in cmap:
            cmap['date'] = ci
        elif texts / n > 0.4 and 'description' not in cmap:
            cmap['description'] = ci
        elif nums / n > 0.3:
            num_cols.append(ci)

    for i, ci in enumerate(num_cols[:3]):
        role = ['debit', 'credit', 'balance'][i]
        if role not in cmap:
            cmap[role] = ci

    hdr = 0
    if pages and pages[0] and 'date' in cmap:
        for ri, row in enumerate(pages[0]):
            if cmap['date'] < len(row) and _try_date(str(row[cmap['date']])):
                hdr = max(0, ri - 1)
                break

    return cmap, hdr, 0


# ═══════════════════════════════════════════════════════════
#  STAGE 3 — ROW EXTRACTION
# ═══════════════════════════════════════════════════════════

def _extract_rows(pages: list, col_map: dict,
                  hdr_row: int, hdr_page: int) -> list:
    txns    = []
    ignored = {col_map[k] for k in ('_index', 'reference')
               if k in col_map}

    for pg_idx, page in enumerate(pages):
        start = (hdr_row + 1) if pg_idx == hdr_page else 0

        for ri in range(start, len(page)):
            row = page[ri]

            if _is_title_row(row):
                continue
            if _is_header_row_repeat(row):
                continue
            if _should_skip_row(row):
                continue

            txn = _parse_row(row, col_map, ignored)
            if txn:
                txns.append(txn)
            elif txns:
                cont = _get_continuation(row, ignored)
                if cont:
                    txns[-1]['desc'] = (
                        txns[-1]['desc'] + ' ' + cont
                    ).strip()

    return txns


def _should_skip_row(row: list) -> bool:
    combined = ' '.join(str(c) for c in row).strip().lower()
    if not combined:
        return True
    return any(p in combined for p in _SKIP_PHRASES)


def _parse_row(row: list, col_map: dict, ignored: set) -> dict:
    if len(row) < 2:
        return None

    # ── Date ──────────────────────────────────────────────
    date = None
    if 'date' in col_map:
        date = _try_date(_cell(row, col_map['date']))
    if not date:
        for ci, c in enumerate(row):
            if ci not in ignored:
                date = _try_date(str(c))
                if date:
                    break
    if not date:
        return None

    # ── FIX B4: only skip rows where index is literally '-'
    #    (opening balance marker). A blank index just means
    #    the bank doesn't use row numbers — keep parsing.
    if '_index' in col_map:
        idx_val = _cell(row, col_map['_index']).strip()
        if idx_val == '-':
            return None

    # ── Description ────────────────────────────────────────
    desc = ''
    if 'description' in col_map:
        desc = _cell(row, col_map['description'])

    if not desc.strip():
        mapped = set(col_map.values()) | ignored
        parts  = []
        for ci, c in enumerate(row):
            c = str(c).strip()
            if (ci not in mapped
                    and c
                    and not _is_num(c)
                    and not _try_date(c)
                    and not _looks_like_ref(c)):
                parts.append(c)
        desc = ' '.join(parts)

    desc = _RE_VALUE_DATE.sub('', desc)
    desc = _RE_WHITESPACE.sub(' ', desc).strip()

    # ── Reference number ───────────────────────────────────
    reference = ''
    if 'reference' in col_map:
        reference = _cell(row, col_map['reference']).strip()

    # ── Amounts ────────────────────────────────────────────
    debit, credit = 0.0, 0.0

    if 'debit' in col_map and 'credit' in col_map:
        debit  = _parse_amt(_cell(row, col_map['debit']))
        credit = _parse_amt(_cell(row, col_map['credit']))
    elif 'amount' in col_map:
        debit, credit = _parse_amt_directed(
            _cell(row, col_map['amount']))
    else:
        debit, credit = _find_amts(row, col_map, ignored)

    if debit == 0 and credit == 0:
        return None

    # ── Balance ────────────────────────────────────────────
    balance = None
    if 'balance' in col_map:
        b = _parse_amt(_cell(row, col_map['balance']))
        if b > 0:
            balance = b

    # ── Type ───────────────────────────────────────────────
    if debit > 0:
        typ, amount = 'DR', debit
    else:
        typ, amount = 'CR', credit

    return {
        'date':      date,
        'desc':      desc,
        'amount':    round(amount, 2),
        'balance':   round(balance, 2) if balance is not None else None,
        'type':      typ,
        'reference': reference,   # FIX B5: was discarded before
    }


def _get_continuation(row: list, ignored: set) -> str:
    for ci, c in enumerate(row):
        if ci not in ignored:
            if _try_date(str(c)):
                return ''
    for ci, c in enumerate(row):
        if ci not in ignored:
            if _parse_amt(str(c)) > 0:
                return ''
    text = ' '.join(
        str(c).strip() for ci, c in enumerate(row)
        if ci not in ignored and str(c).strip()
    )
    if len(text) < 3 or _should_skip_row([text]):
        return ''
    return text


# ═══════════════════════════════════════════════════════════
#  STAGE 4 — NORMALIZATION
# ═══════════════════════════════════════════════════════════

def _normalize(txns: list) -> list:
    # Correct DR/CR using balance diffs
    for i in range(1, len(txns)):
        curr   = txns[i]
        prev   = txns[i - 1]
        b_curr = curr.get('balance')
        b_prev = prev.get('balance')
        amt    = curr.get('amount', 0)

        if b_curr and b_prev and amt:
            diff = round(b_curr - b_prev, 2)
            tol  = max(1.0, round(amt * 0.01, 2))
            if abs(diff - amt) <= tol:
                curr['type'] = 'CR'
            elif abs(diff + amt) <= tol:
                curr['type'] = 'DR'

    # ── De-duplicate ──────────────────────────────────────
    # FIX B3: normalize date string to YYYY-MM-DD for dedup key
    # so '03 Apr 2025' and '3 Apr 2025' don't create false dupes
    seen, result = set(), []
    for t in txns:
        norm_date = _normalize_date_str(t['date'])
        key = (
            norm_date,
            t['amount'],
            t['type'],
            (t.get('desc') or '').strip(),
            t.get('balance'),
        )
        if key in seen:
            continue
        seen.add(key)
        t['desc']     = _RE_WHITESPACE.sub(' ', t.get('desc') or '').strip()
        t['category'] = _categorize(t['desc'])
        result.append(t)

    return result


def _normalize_date_str(date_str: str) -> str:
    """Convert any parsed date string to YYYY-MM-DD for consistent comparison."""
    if not date_str:
        return ''
    for pattern, fmts in _DATE_PATTERNS:
        m = pattern.search(str(date_str))
        if not m:
            continue
        raw = m.group(1) if m.lastindex else m.group()
        for fmt in fmts:
            try:
                dt = datetime.strptime(raw.strip(), fmt)
                if 2000 <= dt.year <= 2035:
                    return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
    return date_str  # fallback: return as-is


def _categorize(desc: str) -> str:
    lower = desc.lower()
    for cat, kws in _CATEGORY_MAP.items():
        for kw in kws:
            if kw in lower:
                return cat
    return 'Other'


# ═══════════════════════════════════════════════════════════
#  UTILITY HELPERS
# ═══════════════════════════════════════════════════════════

def _cell(row: list, idx) -> str:
    if isinstance(idx, int) and 0 <= idx < len(row):
        v = row[idx]
        return str(v).strip() if v is not None else ''
    return ''


def _try_date(text: str):
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


def _parse_amt(text: str) -> float:
    if not text:
        return 0.0
    s = str(text).strip()
    if not s:
        return 0.0
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


def _parse_amt_directed(text: str) -> tuple:
    if not text:
        return 0.0, 0.0
    amt = _parse_amt(text)
    if amt == 0:
        return 0.0, 0.0
    upper = str(text).strip().upper()
    if '(' in text or str(text).strip().startswith('-') \
            or upper.endswith('DR'):
        return amt, 0.0
    if upper.endswith('CR'):
        return 0.0, amt
    return amt, 0.0


def _find_amts(row: list, col_map: dict, ignored: set) -> tuple:
    mapped = set(col_map.values()) | ignored
    nums   = []
    for ci, c in enumerate(row):
        if ci not in mapped:
            v = _parse_amt(str(c))
            if v > 0:
                nums.append(v)
    if not nums:       return 0.0, 0.0
    if len(nums) == 1: return nums[0], 0.0
    # FIX B1: was returning nums[0], 0.0 — now correctly returns both
    if len(nums) == 2: return nums[0], nums[1]
    return nums[0], nums[1]


def _is_num(text: str) -> bool:
    s = _RE_AMOUNT_JUNK.sub('', str(text).strip())
    s = _RE_DR_CR_TAG.sub('', s).strip()
    if not s:
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False


def _looks_like_ref(text: str) -> bool:
    t = str(text).strip()
    if not t or len(t) < 6:
        return False
    if re.match(r'^[\d,. ]+$', t):
        return False
    # FIX B2: original regex missed UPI-509479765357 style refs
    # because UPI is 3 chars and dash was ambiguous.
    # New pattern: 2-12 alphanum prefix, optional separator, 5+ digits
    return bool(re.match(r'^[A-Za-z0-9]{2,12}[-/]?\d{5,}$', t))
