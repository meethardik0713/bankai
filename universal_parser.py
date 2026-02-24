"""
Universal Bank Statement Parser
─────────────────────────────────
Supports:
  - Kotak Mahindra Bank (table-based, indexed rows)
  - Canara Bank ePassbook (raw text, no table borders)
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
    (re.compile(r'\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\b'),
     ['%d %b %Y', '%d %B %Y']),
    (re.compile(r'\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2})\b'),
     ['%d %b %y', '%d %B %y']),
    (re.compile(r'\b(\d{2}[A-Za-z]{3}\d{4})\b'),
     ['%d%b%Y']),
    (re.compile(r'\b(\d{2}[A-Za-z]{3}\d{2})\b'),
     ['%d%b%y']),
    (re.compile(r'\b(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{4})\b'),
     ['%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%m/%d/%Y']),
    (re.compile(r'\b(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2})\b'),
     ['%d/%m/%y', '%d-%m-%y', '%d.%m.%y']),
    (re.compile(r'\b(\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2})\b'),
     ['%Y-%m-%d', '%Y/%m/%d']),
]

_RE_VALUE_DATE  = re.compile(r'\(?\s*[Vv]alue\s+[Dd]ate\s*:\s*[\d\-/\.]+\s*\)?')
_RE_AMOUNT_JUNK = re.compile(r'[₹$€£,\s]')
_RE_DR_CR_TAG   = re.compile(r'\s*(dr|cr|DR|CR|Dr|Cr)\.?\s*$')
_RE_MULTI_SPACE = re.compile(r'\s{2,}')
_RE_WHITESPACE  = re.compile(r'\s+')
_RE_PARENS_NUM  = re.compile(r'^\(([0-9.,]+)\)$')
_RE_CHQ_SUFFIX  = re.compile(r'\s*Chq\s*[:.]?\s*[\dA-Za-z]*\s*$', re.IGNORECASE)

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

_HEADER_MARKER_CELLS = {
    'date', 'description', 'narration',
    'particulars', 'withdrawal', 'deposit',
    'balance', 'debit', 'credit',
}

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
    'Interest':      ['interest', 'int.pd', 'int pd', 'int cr',
                      'int.pd:', 'sbint'],
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
                      'google play', 'steam', 'valve', 'bookmyshow'],
    'Travel':        ['aeronfly', 'irctc', 'makemytrip', 'redbus'],
}


# ═══════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

def parse_transactions(pdf_path: str) -> list:
    start = time.time()
    try:
        bank = _detect_bank(pdf_path)
        print(f"[parser] Detected bank: {bank}")

        if bank == 'canara':
            transactions = _parse_canara(pdf_path)
        else:
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

            # ── DEBUG: first two rows to confirm opening balance ──
            if raw:
                r0 = raw[0]
                print(f"[DEBUG] First row  → date={r0['date']}  "
                      f"amt={r0['amount']}  bal={r0['balance']}  type={r0['type']}")
            if len(raw) > 1:
                r1 = raw[1]
                print(f"[DEBUG] Second row → date={r1['date']}  "
                      f"amt={r1['amount']}  bal={r1['balance']}  type={r1['type']}")
                if r0.get('balance') and r1.get('balance'):
                    print(f"[DEBUG] Balance diff (row1-row0): "
                          f"{round(r1['balance'] - r0['balance'], 2)}")

            if not raw:
                return []
            transactions = _normalize(raw)

        print(f"[parser] Final transactions: {len(transactions)}  "
              f"({time.time()-start:.2f}s)")

        # Surface opening balance in logs
        if transactions and transactions[0].get('opening_balance') is not None:
            print(f"[parser] Opening balance: "
                  f"₹{transactions[0]['opening_balance']:,.2f}")

        return transactions

    except Exception as e:
        import traceback
        print(f"[parser] Fatal: {e}")
        traceback.print_exc()
        return []


# ═══════════════════════════════════════════════════════════
#  BANK DETECTION
# ═══════════════════════════════════════════════════════════

def _detect_bank(pdf_path: str) -> str:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return 'generic'
            text = pdf.pages[0].extract_text() or ''
            text_low = text.lower()

            if any(k in text_low for k in [
                'canara bank', 'cnrb', 'canara aspire',
                'canara savings', 'canarabank'
            ]):
                return 'canara'

            if any(k in text_low for k in [
                'kotak', 'kotak mahindra', '811'
            ]):
                return 'kotak'

    except Exception:
        pass
    return 'generic'


# ═══════════════════════════════════════════════════════════
#  OPENING BALANCE EXTRACTOR  (scans PDF header text)
# ═══════════════════════════════════════════════════════════

def _extract_opening_balance_from_pdf(pdf_path: str) -> float:
    """
    Scans the first 2 pages of the PDF for an explicitly stated
    opening balance line and returns it as a float.
    Returns None if not found.
    """
    _RE_OB = re.compile(
        r'(?:opening\s+balance|open(?:ing)?\s+bal\.?|ob\s*:?|'
        r'brought\s+forward|b/?f)\s*[:\-]?\s*'
        r'([\d,]+\.\d{2})',
        re.IGNORECASE
    )
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:2]:
                text = page.extract_text() or ''
                m = _RE_OB.search(text)
                if m:
                    val = _parse_amt(m.group(1))
                    if val > 0:
                        print(f"[parser] Found opening balance in PDF text: ₹{val:,.2f}")
                        return val
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════
#  CANARA BANK PARSER  (raw text based)
# ═══════════════════════════════════════════════════════════

def _parse_canara(pdf_path: str) -> list:
    raw_txns = []

    # Try to grab opening balance from PDF header first
    pdf_opening_bal = _extract_opening_balance_from_pdf(pdf_path)

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for pg_num, page in enumerate(pdf.pages):
                text  = page.extract_text() or ''
                lines = [l.strip() for l in text.splitlines()]

                narration_buf = []

                for line in lines:
                    if not line:
                        continue

                    if _canara_skip_line(line):
                        continue

                    txn = _canara_parse_txn_line(line)
                    if txn:
                        txn['desc'] = _canara_clean_desc(
                            ' '.join(narration_buf)
                        )
                        narration_buf = []
                        raw_txns.append(txn)
                        continue

                    if re.match(r'^Chq\s*:', line, re.IGNORECASE):
                        continue

                    narration_buf.append(line)

    except Exception as e:
        import traceback
        print(f"[canara] Fatal: {e}")
        traceback.print_exc()

    print(f"[canara] Raw transactions: {len(raw_txns)}")
    result = _normalize(raw_txns)

    # Override inferred opening balance with PDF-stated one if found
    if result and pdf_opening_bal is not None:
        result[0]['opening_balance'] = round(pdf_opening_bal, 2)
        print(f"[canara] Opening balance overridden from PDF: "
              f"₹{pdf_opening_bal:,.2f}")

    return result


def _canara_skip_line(line: str) -> bool:
    lo = line.lower().strip()

    if re.match(r'^date\s+particulars\s+deposits\s+withdrawals\s+balance', lo):
        return True

    if re.match(r'^page\s+\d+', lo):
        return True

    if any(p in lo for p in _SKIP_PHRASES):
        return True

    if any(lo.startswith(p) for p in [
        'statement for', 'branch code', 'customer id', 'branch name',
        'phone', 'product code', 'product name', 'address',
        'ifsc code', 'name ', 'a/c ', 'account no',
    ]):
        return True

    _address_fragments = [
        'ghaziabad', 'uttar pradesh', 'delhi', 'noida',
        'gurugram', 'faridabad', 'mumbai', 'bangalore',
        'siddharth vihar', 'siddhartham', 'gaur ',
        'tower ', 'sector-', 'plot no', ' in 2010',
        'vihar ',
    ]
    if any(frag in lo for frag in _address_fragments):
        return True

    return False


def _canara_parse_txn_line(line: str) -> dict:
    date_match = re.match(r'^(\d{1,2}-\d{2}-\d{4})\s+(.*)', line)
    if not date_match:
        return None

    date_str  = date_match.group(1)
    remainder = date_match.group(2).strip()

    nums = re.findall(r'-?[\d,]+\.\d{2}', remainder)
    if len(nums) < 2:
        return None

    amt_raw = nums[-2]
    bal_raw = nums[-1]

    amt     = _parse_amt(amt_raw)
    balance = _parse_amt(bal_raw)

    if amt == 0:
        return None

    raw_signed = amt_raw.replace(',', '').strip()
    try:
        signed_val = float(raw_signed)
    except ValueError:
        signed_val = amt

    if signed_val < 0:
        typ = 'CR'
        amt = abs(signed_val)
    else:
        typ = 'DR'

    date_parsed = _try_date(date_str)
    if not date_parsed:
        return None

    return {
        'date':      date_parsed,
        'desc':      '',
        'amount':    round(amt, 2),
        'balance':   round(balance, 2),
        'type':      typ,
        'reference': '',
    }


def _canara_clean_desc(desc: str) -> str:
    desc = _RE_VALUE_DATE.sub('', desc)
    desc = _RE_CHQ_SUFFIX.sub('', desc)
    desc = re.sub(r'\b\d{2}:\d{2}:\d{2}\b', '', desc)
    desc = re.sub(r'\b[A-Fa-f0-9]{16,}\b', '', desc)
    desc = _RE_WHITESPACE.sub(' ', desc).strip()
    return desc


# ═══════════════════════════════════════════════════════════
#  STAGE 1 — PDF EXTRACTION  (Kotak / generic)
# ═══════════════════════════════════════════════════════════

def _extract_pdf(pdf_path: str) -> list:
    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        print(f"[parser] Cannot open: {e}")
        return []

    all_pages     = []
    best_strategy = None
    seen_indices  = set()

    for pg_num, page in enumerate(pdf.pages):
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

            extra = _recover_footer_rows_from_text(page_text, seen_indices)
            if extra:
                print(f"[parser] Recovered {len(extra)} footer rows on page {pg_num+1}")
                all_pages[-1].extend(extra)
                all_pages[-1].sort(
                    key=lambda r: int(r[0]) if r and str(r[0]).strip().isdigit() else 0
                )
                for row in extra:
                    idx = str(row[0]).strip() if row and row[0] else ''
                    if idx.isdigit():
                        seen_indices.add(int(idx))

    pdf.close()
    return all_pages


def _recover_footer_rows_from_text(text: str, seen_indices: set) -> list:
    try:
        lines = text.splitlines()
    except Exception:
        return []

    _RE_TXN_START = re.compile(r'^(\d+)\s+(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})\s+(.+)$')
    _RE_AMOUNT    = re.compile(r'[\d,]+\.\d{2}')

    recovered = []
    pending   = None

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if re.search(r'page\s+\d+\s+of\s+\d+', line, re.I):
            continue
        if re.search(r'statement generated|account no|account statement', line, re.I):
            continue

        m = _RE_TXN_START.match(line)
        if m:
            idx = int(m.group(1))
            if idx not in seen_indices:
                if pending:
                    recovered.append(_build_recovered_row(pending, _RE_AMOUNT))
                pending = {
                    'idx':  str(idx),
                    'date': m.group(2),
                    'rest': m.group(3),
                }
            else:
                if pending:
                    recovered.append(_build_recovered_row(pending, _RE_AMOUNT))
                    pending = None
        elif pending:
            if not re.match(r'^\d+\s+\d{1,2}\s+[A-Za-z]{3}\s+\d{4}', line):
                pending['rest'] = pending['rest'] + ' ' + line

    if pending:
        recovered.append(_build_recovered_row(pending, _RE_AMOUNT))

    return recovered


def _build_recovered_row(pending: dict, re_amount) -> list:
    rest    = pending['rest'].strip()
    amounts = re_amount.findall(rest)

    balance    = ''
    withdrawal = ''

    if len(amounts) >= 2:
        balance    = amounts[-1]
        withdrawal = amounts[-2]
    elif len(amounts) == 1:
        balance = amounts[0]

    if amounts:
        first_amt_pos = rest.find(amounts[0])
        desc_part     = rest[:first_amt_pos].strip()
    else:
        desc_part = rest

    ref    = ''
    tokens = desc_part.split()
    if tokens:
        last = tokens[-1]
        if (re.match(r'^[A-Z0-9]{2,12}[-/]\d{5,}$', last)
                or re.match(r'^[A-Z]{2,10}\d{6,}$', last)):
            ref       = last
            desc_part = ' '.join(tokens[:-1])

    return [
        pending['idx'],
        pending['date'],
        desc_part.strip(),
        ref,
        withdrawal,
        '',
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
            lw     = sorted(lines[y], key=lambda w: w['x0'])
            merged = []
            for w in lw:
                if merged and (w['x0'] - merged[-1]['x1']) < 15:
                    merged[-1]['text'] += ' ' + w['text']
                    merged[-1]['x1']    = w['x1']
                else:
                    merged.append({'text': w['text'], 'x0': w['x0'], 'x1': w['x1']})
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
            line  = line.strip()
            if not line:
                continue
            cells = [c.strip() for c in _RE_MULTI_SPACE.split(line) if c.strip()]
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
                cleaned.append(str(cell).replace('\n', ' ').replace('\r', '').strip())
        out.append(cleaned)
    return out


def _good_rows(rows: list) -> int:
    return sum(1 for r in rows if sum(1 for c in r if str(c).strip()) >= 3)


# ═══════════════════════════════════════════════════════════
#  STAGE 2 — COLUMN DETECTION  (Kotak / generic)
# ═══════════════════════════════════════════════════════════

def _detect_columns(pages: list) -> tuple:
    for pg_idx, page in enumerate(pages):
        for row_idx, row in enumerate(page):
            if _is_title_row(row):
                continue
            cmap = _match_header(row)
            if cmap and _valid_map(cmap):
                print(f"[parser] Header pg={pg_idx} row={row_idx}: {row}")
                return cmap, row_idx, pg_idx

    for pg_idx, page in enumerate(pages):
        for row_idx in range(len(page) - 1):
            merged = _merge_rows(page[row_idx], page[row_idx + 1])
            cmap   = _match_header(merged)
            if cmap and _valid_map(cmap):
                print(f"[parser] Merged header pg={pg_idx} rows={row_idx}-{row_idx+1}")
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

        norm           = _norm(raw)
        best_role      = None
        best_score     = 0

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
    return (
        'date' in cmap
        and any(k in cmap for k in ('debit', 'credit', 'amount'))
    )


def _merge_rows(a: list, b: list) -> list:
    n = max(len(a), len(b))
    return [
        (
            (str(a[i]).strip() if i < len(a) else '') + ' ' +
            (str(b[i]).strip() if i < len(b) else '')
        ).strip()
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
        texts = sum(
            1 for v in vals
            if len(v.strip()) > 8
            and not _is_num(v)
            and not _try_date(v)
        )
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
#  STAGE 3 — ROW EXTRACTION  (Kotak / generic)
# ═══════════════════════════════════════════════════════════

def _extract_rows(pages: list, col_map: dict, hdr_row: int, hdr_page: int) -> list:
    txns    = []
    ignored = {col_map[k] for k in ('_index', 'reference') if k in col_map}

    for pg_idx, page in enumerate(pages):
        start     = (hdr_row + 1) if pg_idx == hdr_page else 0
        data_rows = page[start:]

        for row in data_rows:
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
                    txns[-1]['desc'] = (txns[-1]['desc'] + ' ' + cont).strip()

    return txns


def _should_skip_row(row: list) -> bool:
    combined = ' '.join(str(c) for c in row).strip().lower()
    if not combined:
        return True
    return any(p in combined for p in _SKIP_PHRASES)


def _parse_row(row: list, col_map: dict, ignored: set) -> dict:
    if len(row) < 2:
        return None

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

    if '_index' in col_map:
        idx_val = _cell(row, col_map['_index']).strip()
        if idx_val == '-':
            return None

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
    desc = _RE_CHQ_SUFFIX.sub('', desc)
    desc = _RE_WHITESPACE.sub(' ', desc).strip()

    reference = ''
    if 'reference' in col_map:
        reference = _cell(row, col_map['reference']).strip()

    debit, credit = 0.0, 0.0

    if 'debit' in col_map and 'credit' in col_map:
        debit  = _parse_amt(_cell(row, col_map['debit']))
        credit = _parse_amt(_cell(row, col_map['credit']))
    elif 'amount' in col_map:
        debit, credit = _parse_amt_directed(_cell(row, col_map['amount']))
    else:
        debit, credit = _find_amts(row, col_map, ignored)

    if debit == 0 and credit == 0:
        return None

    balance = None
    if 'balance' in col_map:
        b = _parse_amt(_cell(row, col_map['balance']))
        if b > 0:
            balance = b

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
        'reference': reference,
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
#  STAGE 4 — NORMALIZATION  (both banks)
# ═══════════════════════════════════════════════════════════

def _normalize(txns: list) -> list:
    if not txns:
        return []

    # ── Step 1: infer opening balance from first transaction ──────────
    first = txns[0]
    b0    = first.get('balance')
    amt0  = first.get('amount', 0)

    opening_balance = None

    if b0 is not None and amt0:
        implied_cr = round(b0 - amt0, 2)   # OB if first txn was a credit
        implied_dr = round(b0 + amt0, 2)   # OB if first txn was a debit

        if abs(b0 - amt0) <= max(1.0, amt0 * 0.01):
            # Balance ≈ amount → truly the first-ever deposit, OB was 0
            opening_balance  = 0.0
            first['type']    = 'CR'
            print(f"[normalize] First txn looks like opening deposit → OB=0.00")
        elif implied_cr >= 0:
            # Opening balance was positive; first txn was a credit
            opening_balance  = implied_cr
            first['type']    = 'CR'
            print(f"[normalize] Inferred OB (CR path): ₹{opening_balance:,.2f}")
        elif implied_dr >= 0:
            # Opening balance was positive; first txn was a debit
            opening_balance  = implied_dr
            first['type']    = 'DR'
            print(f"[normalize] Inferred OB (DR path): ₹{opening_balance:,.2f}")

    # ── Step 2: fix transaction types using sequential balance diff ───
    for i in range(1, len(txns)):
        curr   = txns[i]
        prev   = txns[i - 1]
        b_curr = curr.get('balance')
        b_prev = prev.get('balance')
        amt    = curr.get('amount', 0)

        if b_curr is not None and b_prev is not None and amt:
            diff = round(b_curr - b_prev, 2)
            tol  = max(1.0, round(amt * 0.01, 2))
            if abs(diff - amt) <= tol:
                curr['type'] = 'CR'
            elif abs(diff + amt) <= tol:
                curr['type'] = 'DR'

    # ── Step 3: deduplicate ───────────────────────────────────────────
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
        t['date']     = norm_date or t['date']
        t['desc']     = _RE_WHITESPACE.sub(' ', t.get('desc') or '').strip()
        t['category'] = _categorize(t['desc'])
        result.append(t)

    # ── Step 4: attach opening balance to first transaction ───────────
    if result and opening_balance is not None:
        result[0]['opening_balance'] = round(opening_balance, 2)
    elif result:
        result[0].setdefault('opening_balance', None)

    return result


def _normalize_date_str(date_str: str) -> str:
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
    return date_str


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
    if ('(' in text
            or str(text).strip().startswith('-')
            or upper.endswith('DR')):
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
    return bool(re.match(r'^[A-Za-z0-9]{2,12}[-/]?\d{5,}$', t))
