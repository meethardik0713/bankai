"""
parsers/generic.py
───────────────────
Generic / Fallback Bank Statement Parser.
Handles any bank with a reasonably structured PDF table.
Used by Kotak and as fallback for unknown banks.
"""

import re
import pdfplumber
from collections import Counter

from parsers.base    import BaseParser
from core.normalizer import normalize
from core.utils      import (
    parse_amt, try_date,
    extract_opening_balance_from_pdf,
    extract_opening_balance_from_table,
)

_RE_VALUE_DATE  = re.compile(r'\(?\s*[Vv]alue\s+[Dd]ate\s*:\s*[\d\-/\.]+\s*\)?')
_RE_CHQ_SUFFIX  = re.compile(r'\s*Chq\s*[:.]?\s*[\dA-Za-z]*\s*$', re.IGNORECASE)
_RE_WHITESPACE  = re.compile(r'\s+')
_RE_MULTI_SPACE = re.compile(r'\s{2,}')
_RE_PARENS_NUM  = re.compile(r'^\(([0-9.,]+)\)$')
_RE_DR_CR_TAG   = re.compile(r'\s*(dr|cr|DR|CR|Dr|Cr)\.?\s*$')
_RE_AMOUNT_JUNK = re.compile(r'[₹$€£,\s]')

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
    'savings account transactions', 'current account transactions',
    'account transactions', 'transaction details', 'statement of account',
]

_HEADER_MARKER_CELLS = {
    'date', 'description', 'narration', 'particulars',
    'withdrawal', 'deposit', 'balance', 'debit', 'credit',
}


class GenericParser(BaseParser):

    def detect_from_text(self, text_low: str) -> bool:
        return False  # Only used as fallback

    def detect(self, pdf_path: str) -> bool:
        return False

    def parse(self, pdf_path: str) -> list:
        ob    = extract_opening_balance_from_pdf(pdf_path)
        pages = self._extract_pdf(pdf_path)

        if ob is None:
            ob = extract_opening_balance_from_table(pages)
        if not pages:
            self._log("No content extracted from PDF")
            return []

        self._log(f"Pages extracted: {len(pages)}")
        col_map, hdr_row, hdr_page = self._detect_columns(pages)
        self._log(f"col_map: {col_map}  hdr_row: {hdr_row}  hdr_page: {hdr_page}")

        if not col_map:
            self._log("Column detection failed")
            return []

        raw = self._extract_rows(pages, col_map, hdr_row, hdr_page)
        self._log(f"Raw transactions: {len(raw)}")

        if not raw:
            return []
        return normalize(raw, opening_balance=ob)

    # ══════════════════════════════════════════════════════
    #  PDF EXTRACTION
    # ══════════════════════════════════════════════════════

    def _extract_pdf(self, pdf_path: str) -> list:
        try:
            pdf = pdfplumber.open(pdf_path)
        except Exception as e:
            self._log(f"Cannot open: {e}")
            return []

        all_pages     = []
        best_strategy = None
        seen_indices  = set()

        for pg_num, page in enumerate(pdf.pages):
            try:
                page_text = page.extract_text() or ''
            except Exception:
                page_text = ''

            rows, strategy = self._extract_page(page, best_strategy)
            if rows:
                all_pages.append(rows)
                if best_strategy is None and strategy:
                    best_strategy = strategy

                for row in rows:
                    idx = str(row[0]).strip() if row and row[0] else ''
                    if idx.isdigit():
                        seen_indices.add(int(idx))

                extra = self._recover_footer_rows(page_text, seen_indices)
                if extra:
                    all_pages[-1].extend(extra)
                    all_pages[-1].sort(
                        key=lambda r: int(r[0]) if r and str(r[0]).strip().isdigit() else 0
                    )

        pdf.close()
        return all_pages

    def _extract_page(self, page, preferred=None):
        strategies = [
            ('table',         self._try_table),
            ('table_relaxed', self._try_table_relaxed),
            ('words',         self._try_words),
            ('lines',         self._try_lines),
        ]
        if preferred:
            for name, fn in strategies:
                if name == preferred:
                    rows = fn(page)
                    if rows and self._good_rows(rows) >= 1:
                        return rows, name
        for name, fn in strategies:
            rows = fn(page)
            if rows and self._good_rows(rows) >= 1:
                return rows, name
        return [], None

    def _try_table(self, page):
        try:
            tables = page.extract_tables()
            if tables:
                return self._clean_table(max(tables, key=len))
        except Exception:
            pass
        return []

    def _try_table_relaxed(self, page):
        try:
            tables = page.extract_tables({
                "vertical_strategy": "text", "horizontal_strategy": "text",
                "snap_tolerance": 5, "join_tolerance": 5,
            })
            if tables:
                return self._clean_table(max(tables, key=len))
        except Exception:
            pass
        return []

    def _try_words(self, page):
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

    def _try_lines(self, page):
        try:
            text = page.extract_text() or ''
            rows = []
            for line in text.split('\n'):
                line  = line.strip()
                cells = [c.strip() for c in _RE_MULTI_SPACE.split(line) if c.strip()]
                if cells:
                    rows.append(cells)
            return rows
        except Exception:
            return []

    @staticmethod
    def _clean_table(table):
        out = []
        for row in table:
            cleaned = [str(cell).replace('\n', ' ').replace('\r', '').strip()
                       if cell is not None else '' for cell in row]
            out.append(cleaned)
        return out

    @staticmethod
    def _good_rows(rows):
        return sum(1 for r in rows if sum(1 for c in r if str(c).strip()) >= 3)

    def _recover_footer_rows(self, text: str, seen_indices: set) -> list:
        _RE_TXN = re.compile(r'^(\d+)\s+(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})\s+(.+)$')
        _RE_AMT = re.compile(r'[\d,]+\.\d{2}')
        recovered, pending = [], None

        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if re.search(r'page\s+\d+\s+of\s+\d+', line, re.I):
                continue

            m = _RE_TXN.match(line)
            if m:
                idx = int(m.group(1))
                if idx not in seen_indices:
                    if pending:
                        recovered.append(self._build_row(pending, _RE_AMT))
                    pending = {'idx': str(idx), 'date': m.group(2), 'rest': m.group(3)}
                else:
                    if pending:
                        recovered.append(self._build_row(pending, _RE_AMT))
                        pending = None
            elif pending:
                if not re.match(r'^\d+\s+\d{1,2}\s+[A-Za-z]{3}\s+\d{4}', line):
                    pending['rest'] += ' ' + line

        if pending:
            recovered.append(self._build_row(pending, _RE_AMT))
        return recovered

    @staticmethod
    def _build_row(pending, re_amt):
        rest    = pending['rest'].strip()[:300]
        amounts = re_amt.findall(rest)
        balance = amounts[-1] if len(amounts) >= 1 else ''
        withdrawal = amounts[-2] if len(amounts) >= 2 else ''
        first_pos = rest.find(amounts[0]) if amounts else len(rest)
        desc_part = rest[:first_pos].strip()
        return [pending['idx'], pending['date'], desc_part, '', withdrawal, '', balance]

    # ══════════════════════════════════════════════════════
    #  COLUMN DETECTION
    # ══════════════════════════════════════════════════════

    def _detect_columns(self, pages):
        for pg_idx, page in enumerate(pages):
            for row_idx, row in enumerate(page):
                if self._is_title_row(row):
                    continue
                cmap = self._match_header(row)
                if cmap and self._valid_map(cmap):
                    return cmap, row_idx, pg_idx

        for pg_idx, page in enumerate(pages):
            for row_idx in range(len(page) - 1):
                merged = self._merge_rows(page[row_idx], page[row_idx + 1])
                cmap   = self._match_header(merged)
                if cmap and self._valid_map(cmap):
                    return cmap, row_idx + 1, pg_idx

        return self._infer_columns(pages)

    @staticmethod
    def _is_title_row(row):
        non_empty = [c.strip() for c in row if str(c).strip()]
        return len(non_empty) == 1 and non_empty[0].lower() in _TITLE_PHRASES

    @staticmethod
    def _is_header_row_repeat(row):
        hits = sum(1 for cell in row if str(cell).strip().lower() in _HEADER_MARKER_CELLS)
        return hits >= 3

    @staticmethod
    def _norm(text):
        t = str(text).lower()
        t = re.sub(r'[^a-z0-9 ./]', ' ', t)
        return re.sub(r'\s+', ' ', t).strip()

    def _match_header(self, row):
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
            norm = self._norm(raw)
            best_role, best_score = None, 0
            for role, keywords in _COL_KEYWORDS.items():
                for kw in keywords:
                    kw_n = self._norm(kw)
                    if norm == kw_n:        score = 200
                    elif norm.startswith(kw_n): score = 100 + len(kw_n)
                    elif kw_n in norm:      score = 50 + len(kw_n)
                    else:                   continue
                    if score > best_score:
                        best_score, best_role = score, role
            if best_role and best_role not in cmap:
                cmap[best_role] = idx
        return cmap

    @staticmethod
    def _valid_map(cmap):
        return 'date' in cmap and any(k in cmap for k in ('debit', 'credit', 'amount'))

    @staticmethod
    def _merge_rows(a, b):
        n = max(len(a), len(b))
        return [
            ((str(a[i]).strip() if i < len(a) else '') + ' ' +
             (str(b[i]).strip() if i < len(b) else '')).strip()
            for i in range(n)
        ]

    def _infer_columns(self, pages):
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
            dates = sum(1 for v in vals if try_date(v))
            nums  = sum(1 for v in vals if self._is_num(v))
            texts = sum(1 for v in vals if len(v.strip()) > 8 and not self._is_num(v) and not try_date(v))
            idxs  = sum(1 for v in vals if re.match(r'^[\d\-]+$', v.strip()))

            if idxs / n > 0.6:      cmap.setdefault('_index', ci)
            elif dates / n > 0.4 and 'date' not in cmap: cmap['date'] = ci
            elif texts / n > 0.4 and 'description' not in cmap: cmap['description'] = ci
            elif nums / n > 0.3:    num_cols.append(ci)

        for i, ci in enumerate(num_cols[:3]):
            role = ['debit', 'credit', 'balance'][i]
            if role not in cmap:
                cmap[role] = ci

        hdr = 0
        if pages and pages[0] and 'date' in cmap:
            for ri, row in enumerate(pages[0]):
                if cmap['date'] < len(row) and try_date(str(row[cmap['date']])):
                    hdr = max(0, ri - 1)
                    break
        return cmap, hdr, 0

    # ══════════════════════════════════════════════════════
    #  ROW EXTRACTION
    # ══════════════════════════════════════════════════════

    def _extract_rows(self, pages, col_map, hdr_row, hdr_page):
        txns    = []
        ignored = {col_map[k] for k in ('_index', 'reference') if k in col_map}

        for pg_idx, page in enumerate(pages):
            start     = (hdr_row + 1) if pg_idx == hdr_page else 0
            data_rows = page[start:]
            for row in data_rows:
                if self._is_title_row(row): continue
                if self._is_header_row_repeat(row): continue
                if self._should_skip_row(row): continue
                txn = self._parse_row(row, col_map, ignored)
                if txn:
                    txns.append(txn)
                elif txns:
                    cont = self._get_continuation(row, ignored)
                    if cont:
                        txns[-1]['desc'] = (txns[-1]['desc'] + ' ' + cont).strip()
        return txns

    @staticmethod
    def _should_skip_row(row):
        combined = ' '.join(str(c) for c in row).strip().lower()
        if not combined:
            return True
        return any(p in combined for p in _SKIP_PHRASES)

    def _parse_row(self, row, col_map, ignored):
        if len(row) < 2:
            return None

        date = None
        if 'date' in col_map:
            date = try_date(self._cell(row, col_map['date']))
        if not date:
            for ci, c in enumerate(row):
                if ci not in ignored:
                    date = try_date(str(c))
                    if date:
                        break
        if not date:
            return None

        if '_index' in col_map:
            if self._cell(row, col_map['_index']).strip() == '-':
                return None

        desc = ''
        if 'description' in col_map:
            desc = self._cell(row, col_map['description'])
        if not desc.strip():
            mapped = set(col_map.values()) | ignored
            parts  = [str(c).strip() for ci, c in enumerate(row)
                      if ci not in mapped and str(c).strip()
                      and not self._is_num(str(c)) and not try_date(str(c))
                      and not self._looks_like_ref(str(c))]
            desc = ' '.join(parts)

        desc = _RE_VALUE_DATE.sub('', desc)
        desc = _RE_CHQ_SUFFIX.sub('', desc)
        desc = _RE_WHITESPACE.sub(' ', desc).strip()[:200]

        reference = self._cell(row, col_map['reference']).strip() if 'reference' in col_map else ''

        if 'debit' in col_map and 'credit' in col_map:
            debit  = parse_amt(self._cell(row, col_map['debit']))
            credit = parse_amt(self._cell(row, col_map['credit']))
        elif 'amount' in col_map:
            debit, credit = self._parse_amt_directed(self._cell(row, col_map['amount']))
        else:
            debit, credit = self._find_amts(row, col_map, ignored)

        if debit == 0 and credit == 0:
            return None

        balance = None
        if 'balance' in col_map:
            b = parse_amt(self._cell(row, col_map['balance']))
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

    def _get_continuation(self, row, ignored):
        for ci, c in enumerate(row):
            if ci not in ignored:
                if try_date(str(c)) or parse_amt(str(c)) > 0:
                    return ''
        text = ' '.join(str(c).strip() for ci, c in enumerate(row)
                        if ci not in ignored and str(c).strip())
        if len(text) < 3 or len(text) > 150 or self._should_skip_row([text]):
            return ''
        return text

    @staticmethod
    def _cell(row, idx):
        if isinstance(idx, int) and 0 <= idx < len(row):
            v = row[idx]
            return str(v).strip() if v is not None else ''
        return ''

    @staticmethod
    def _is_num(text):
        s = _RE_AMOUNT_JUNK.sub('', str(text).strip())
        s = _RE_DR_CR_TAG.sub('', s).strip()
        if not s:
            return False
        try:
            float(s)
            return True
        except ValueError:
            return False

    @staticmethod
    def _looks_like_ref(text):
        t = str(text).strip()
        if not t or len(t) < 6:
            return False
        if re.match(r'^[\d,. ]+$', t):
            return False
        return bool(re.match(r'^[A-Za-z0-9]{2,12}[-/]?\d{5,}$', t))

    @staticmethod
    def _parse_amt_directed(text):
        if not text:
            return 0.0, 0.0
        amt   = parse_amt(text)
        upper = str(text).strip().upper()
        if '(' in text or str(text).strip().startswith('-') or upper.endswith('DR'):
            return amt, 0.0
        if upper.endswith('CR'):
            return 0.0, amt
        return amt, 0.0

    @staticmethod
    def _find_amts(row, col_map, ignored):
        mapped = set(col_map.values()) | ignored
        nums   = [parse_amt(str(c)) for ci, c in enumerate(row)
                  if ci not in mapped and parse_amt(str(c)) > 0]
        if not nums:       return 0.0, 0.0
        if len(nums) == 1: return nums[0], 0.0
        return nums[0], nums[1]
    