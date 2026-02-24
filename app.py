import os
import logging
import openpyxl
import time
import json
import hashlib
import threading
from flask import Flask, request, render_template, send_file, abort, session
from io import BytesIO
from collections import defaultdict
from werkzeug.utils import secure_filename
from universal_parser import parse_transactions

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)

# ── App setup ─────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(32))
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ── Rate limiting ─────────────────────────────────────────
request_counts = defaultdict(list)
RATE_LIMIT  = 10
RATE_WINDOW = 60

# ── In-memory transaction cache ───────────────────────────
_CACHE      = {}
_CACHE_LOCK = threading.Lock()
_CACHE_TTL  = 1800   # 30 minutes


def _cache_get(file_hash: str):
    with _CACHE_LOCK:
        entry = _CACHE.get(file_hash)
        if entry and (time.time() - entry['ts']) < _CACHE_TTL:
            return entry
        if entry:
            del _CACHE[file_hash]
        return None


def _cache_set(file_hash: str, transactions: list, filename: str):
    with _CACHE_LOCK:
        if len(_CACHE) >= 10:
            oldest = min(_CACHE, key=lambda k: _CACHE[k]['ts'])
            del _CACHE[oldest]
        _CACHE[file_hash] = {
            'transactions': transactions,
            'filename':     filename,
            'ts':           time.time(),
        }


def _file_hash(file_storage) -> str:
    chunk = file_storage.stream.read(65536)
    file_storage.stream.seek(0)
    return hashlib.md5(chunk).hexdigest()


_PDF_MAGIC = b'%PDF'


def is_rate_limited(ip: str) -> bool:
    now = time.time()
    request_counts[ip] = [t for t in request_counts[ip]
                           if now - t < RATE_WINDOW]
    if len(request_counts[ip]) >= RATE_LIMIT:
        return True
    request_counts[ip].append(now)
    return False


def _is_valid_pdf(file_storage) -> bool:
    header = file_storage.stream.read(4)
    file_storage.stream.seek(0)
    return header == _PDF_MAGIC


def _sanitize_desc(value: str) -> str:
    value = str(value).strip()
    if value and value[0] in ('=', '+', '-', '@', '\t', '\r'):
        value = "'" + value
    return value


# ═══════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════

@app.route('/', methods=['GET', 'POST'])
def home():
    ip = request.remote_addr
    if request.method == 'POST' and is_rate_limited(ip):
        abort(429)

    transaction_data  = []
    keyword           = ''
    total             = 0.0
    total_debit       = 0.0
    total_credit      = 0.0
    amounts_count     = 0
    count             = 0
    selected_filename = ''
    error_message     = ''
    parse_time        = None

    cached_hash = session.get('file_hash')
    cached_name = session.get('file_name', '')

    if request.method == 'POST':
        keyword = request.form.get('keyword', '')[:100]
        file    = request.files.get('pdf_file')
        file_uploaded = file and file.filename

        all_transactions = []

        if file_uploaded:
            safe_name = secure_filename(file.filename)

            if not safe_name.lower().endswith('.pdf'):
                error_message = 'Only PDF files are accepted.'
            elif len(safe_name) >= 200:
                error_message = 'Filename too long.'
            elif not _is_valid_pdf(file):
                error_message = 'Uploaded file does not appear to be a valid PDF.'
            else:
                fhash = _file_hash(file)
                cached = _cache_get(fhash)

                if cached:
                    all_transactions  = cached['transactions']
                    selected_filename = cached['filename']
                    logger.info("Cache hit for %s", safe_name)
                else:
                    selected_filename = safe_name
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_name)
                    file.save(filepath)
                    try:
                        t0 = time.time()
                        all_transactions = parse_transactions(filepath)
                        parse_time = round(time.time() - t0, 1)
                        _cache_set(fhash, all_transactions, safe_name)
                        session['file_hash'] = fhash
                        session['file_name'] = safe_name
                        logger.info("Parsed %s → %d txns in %.1fs",
                                    safe_name, len(all_transactions), parse_time)
                    except Exception as e:
                        logger.exception("Error parsing %s", safe_name)
                        error_message = f'Could not parse the file: {e}'
                    finally:
                        if os.path.exists(filepath):
                            os.remove(filepath)

        elif cached_hash:
            cached = _cache_get(cached_hash)
            if cached:
                all_transactions  = cached['transactions']
                selected_filename = cached['filename']
            else:
                error_message = 'Session expired. Please re-upload the PDF.'
                session.pop('file_hash', None)
                session.pop('file_name', None)

        if all_transactions and not error_message:
            if keyword:
                kw_lower = keyword.lower()
                kw_clean = keyword.replace(',', '')
                filtered = [
                    t for t in all_transactions
                    if kw_lower in (t.get('desc') or '').lower()
                    or kw_lower in (t.get('date') or '').lower()
                    or kw_lower in (t.get('category') or '').lower()
                    or kw_clean in str(t.get('amount') or '')
                    or kw_clean in str(t.get('balance') or '')
                ]
            else:
                filtered = all_transactions

            for t in filtered:
                transaction_data.append(t)
                if t.get('amount'):
                    amounts_count += 1
                    if t.get('type') == 'DR':
                        total_debit  += t['amount']
                    else:
                        total_credit += t['amount']

            total = total_debit + total_credit
            count = len(transaction_data)

    elif request.method == 'GET' and cached_hash:
        selected_filename = cached_name

    return render_template(
        'index.html',
        transaction_data  = transaction_data,
        keyword           = keyword,
        total             = total,
        total_debit       = total_debit,
        total_credit      = total_credit,
        count             = count,
        amounts_count     = amounts_count,
        selected_filename = selected_filename,
        error_message     = error_message,
        parse_time        = parse_time,
        has_cached_file   = bool(session.get('file_hash')),
        cached_filename   = session.get('file_name', ''),
    )


@app.route('/clear', methods=['POST'])
def clear_cache():
    fhash = session.pop('file_hash', None)
    session.pop('file_name', None)
    if fhash:
        with _CACHE_LOCK:
            _CACHE.pop(fhash, None)
    return ('', 204)


@app.route('/about')
def about():
    return render_template('about.html')


@app.route('/privacy')
def privacy():
    return render_template('privacy.html')


@app.route('/terms')
def terms():
    return render_template('terms.html')


# ═══════════════════════════════════════════════════════════
#  ACCURACY ROUTES
# ═══════════════════════════════════════════════════════════

@app.route('/accuracy', methods=['GET'])
def accuracy_page():
    return render_template('accuracy.html', error=None)


@app.route('/accuracy', methods=['POST'])
def accuracy_check():
    ip = request.remote_addr
    if is_rate_limited(ip):
        abort(429)

    file = request.files.get('pdf_file')
    if not file or not file.filename:
        return render_template('accuracy.html', error="No file uploaded.")

    safe_name = secure_filename(file.filename)

    if not safe_name.lower().endswith('.pdf'):
        return render_template('accuracy.html', error="Only PDF files accepted.")
    if len(safe_name) >= 200:
        return render_template('accuracy.html', error="Filename too long.")
    if not _is_valid_pdf(file):
        return render_template('accuracy.html', error="Invalid PDF file.")

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_name)
    file.save(filepath)

    try:
        t0 = time.time()
        transactions = parse_transactions(filepath)
        parse_time = round(time.time() - t0, 1)
        logger.info("Accuracy test: parsed %s → %d txns in %.1fs",
                    safe_name, len(transactions), parse_time)
    except Exception as e:
        logger.exception("Accuracy test parse error: %s", safe_name)
        return render_template('accuracy.html', error=f"Parse error: {e}")
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    if not transactions:
        return render_template('accuracy.html', error="No transactions found in this PDF.")

    total = len(transactions)

    # ── Check 1: Missing fields ───────────────────────────
    missing_date    = sum(1 for t in transactions if not t.get('date'))
    missing_amount  = sum(1 for t in transactions if not t.get('amount'))
    missing_balance = sum(1 for t in transactions if not t.get('balance'))
    missing_desc    = sum(1 for t in transactions if not t.get('desc'))

    # ── Check 2: Balance continuity ───────────────────────
    balance_errors = []
    for i in range(1, len(transactions)):
        prev = transactions[i - 1]
        curr = transactions[i]

        prev_bal = prev.get('balance')
        curr_bal = curr.get('balance')
        amount   = curr.get('amount')
        txn_type = curr.get('type', '')

        if prev_bal is None or curr_bal is None or amount is None:
            continue

        if txn_type == 'CR':
            expected = round(prev_bal + amount, 2)
        elif txn_type == 'DR':
            expected = round(prev_bal - amount, 2)
        else:
            continue

        actual = round(curr_bal, 2)
        diff   = round(abs(expected - actual), 2)

        if diff > 1.0:
            balance_errors.append({
                'row'     : i + 1,
                'date'    : curr.get('date', 'N/A'),
                'desc'    : (curr.get('desc') or '')[:40],
                'expected': expected,
                'actual'  : actual,
                'diff'    : diff,
            })

    # ── Check 3: Opening / Closing balance ────────────────
    opening_bal = transactions[0].get('opening_balance') or transactions[0].get('balance')
    closing_bal = transactions[-1].get('balance')

    total_credits = sum(
        t['amount'] for t in transactions
        if t.get('type') == 'CR' and t.get('amount')
    )
    total_debits = sum(
        t['amount'] for t in transactions
        if t.get('type') == 'DR' and t.get('amount')
    )

    if opening_bal is not None and closing_bal is not None:
        calculated_closing = round(opening_bal + total_credits - total_debits, 2)
        closing_diff       = round(abs(calculated_closing - closing_bal), 2)
        balance_match      = closing_diff <= 1.0
    else:
        calculated_closing = None
        closing_diff       = None
        balance_match      = None

    # ── Accuracy Score ─────────────────────────────────────
    continuity_ok  = (total - 1) - len(balance_errors)
    continuity_pct = round((continuity_ok / (total - 1)) * 100, 1) if total > 1 else 100.0
    fields_ok      = total - max(missing_date, missing_amount, missing_balance)
    fields_pct     = round((fields_ok / total) * 100, 1)
    overall_score  = round((continuity_pct + fields_pct) / 2, 1)

    return render_template(
        'accuracy.html',
        total              = total,
        missing_date       = missing_date,
        missing_amount     = missing_amount,
        missing_balance    = missing_balance,
        missing_desc       = missing_desc,
        balance_errors     = balance_errors,
        opening_bal        = opening_bal,
        closing_bal        = closing_bal,
        calculated_closing = calculated_closing,
        closing_diff       = closing_diff,
        balance_match      = balance_match,
        total_credits      = round(total_credits, 2),
        total_debits       = round(total_debits, 2),
        continuity_pct     = continuity_pct,
        fields_pct         = fields_pct,
        overall_score      = overall_score,
        filename           = safe_name,
        parse_time         = parse_time,
        error              = None,
    )


# ═══════════════════════════════════════════════════════════
#  DEBUG ROUTE
#  Remove this route before going to production!
# ═══════════════════════════════════════════════════════════

@app.route('/debug', methods=['GET', 'POST'])
def debug():
    if request.method == 'GET':
        return '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>PDF Debug Tool</title>
            <style>
                body { font-family: Arial, sans-serif; padding: 30px; }
                h2   { color: #333; }
                form { margin-top: 20px; }
                input[type=file] { padding: 10px; }
                button {
                    margin-top: 15px;
                    padding: 10px 25px;
                    background: #4CAF50;
                    color: white;
                    border: none;
                    border-radius: 4px;
                    cursor: pointer;
                    font-size: 15px;
                }
                button:hover { background: #45a049; }
            </style>
        </head>
        <body>
            <h2>🔍 PDF Debug Tool</h2>
            <p>Upload your bank statement PDF to see exactly
               how pdfplumber extracts it.</p>
            <form method="POST" enctype="multipart/form-data">
                <input type="file" name="pdf_file" accept=".pdf">
                <br>
                <button type="submit">Analyze PDF</button>
            </form>
        </body>
        </html>
        '''

    if 'pdf_file' not in request.files:
        return "No file uploaded.", 400

    file = request.files['pdf_file']
    if not file or not file.filename:
        return "No file selected.", 400

    import pdfplumber

    safe_name = secure_filename(file.filename)
    filepath  = os.path.join(app.config['UPLOAD_FOLDER'], safe_name)
    file.save(filepath)

    results = []

    try:
        with pdfplumber.open(filepath) as pdf:
            total_pages = len(pdf.pages)

            for i, page in enumerate(pdf.pages[:3]):
                page_info = {
                    'page_number': i + 1,
                    'total_pages': total_pages,
                }

                try:
                    tables = page.extract_tables()
                    if tables:
                        biggest = max(tables, key=len)
                        page_info['default_table'] = {
                            'tables_found'     : len(tables),
                            'rows_in_biggest'  : len(biggest),
                            'columns_in_row_0' : len(biggest[0]) if biggest else 0,
                            'first_3_rows'     : biggest[:3],
                            'last_2_rows'      : biggest[-2:],
                        }
                    else:
                        page_info['default_table'] = 'NO TABLES FOUND'
                except Exception as e:
                    page_info['default_table'] = f'ERROR: {e}'

                try:
                    tables_r = page.extract_tables({
                        "vertical_strategy":   "text",
                        "horizontal_strategy": "text",
                        "snap_tolerance":      5,
                        "join_tolerance":      5,
                    })
                    if tables_r:
                        biggest_r = max(tables_r, key=len)
                        page_info['relaxed_table'] = {
                            'tables_found'     : len(tables_r),
                            'rows_in_biggest'  : len(biggest_r),
                            'columns_in_row_0' : len(biggest_r[0]) if biggest_r else 0,
                            'first_3_rows'     : biggest_r[:3],
                        }
                    else:
                        page_info['relaxed_table'] = 'NO TABLES FOUND'
                except Exception as e:
                    page_info['relaxed_table'] = f'ERROR: {e}'

                try:
                    raw = page.extract_text() or ''
                    page_info['raw_text_lines'] = raw.splitlines()[:20]
                except Exception as e:
                    page_info['raw_text_lines'] = f'ERROR: {e}'

                try:
                    words = page.extract_words()
                    page_info['first_10_words'] = [
                        {'text': w['text'], 'x0': round(w['x0'], 1),
                         'top': round(w['top'], 1)}
                        for w in (words[:10] if words else [])
                    ]
                except Exception as e:
                    page_info['first_10_words'] = f'ERROR: {e}'

                results.append(page_info)

    except Exception as e:
        results = [{'fatal_error': str(e)}]

    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    html = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Debug Results</title>
        <style>
            body  { font-family: Arial, sans-serif; padding: 20px;
                    background: #f9f9f9; }
            h2    { color: #222; }
            h3    { color: #444; border-bottom: 2px solid #ddd;
                    padding-bottom: 5px; }
            pre   { background: #1e1e1e; color: #d4d4d4;
                    padding: 15px; border-radius: 6px;
                    overflow-x: auto; font-size: 13px;
                    line-height: 1.5; }
            .box  { background: white; border: 1px solid #ddd;
                    border-radius: 8px; padding: 20px;
                    margin-bottom: 25px; }
            .back { display:inline-block; margin-bottom:20px;
                    padding: 8px 18px; background:#555;
                    color:white; border-radius:4px;
                    text-decoration:none; }
        </style>
    </head>
    <body>
        <a class="back" href="/debug">← Upload Another</a>
        <h2>🔍 PDF Debug Results</h2>
    '''

    for page_data in results:
        pg  = page_data.get('page_number', '?')
        tot = page_data.get('total_pages', '?')
        html += f'<div class="box">'
        html += f'<h3>Page {pg} of {tot}</h3>'
        html += '<pre>'
        html += json.dumps(page_data, indent=2,
                           ensure_ascii=False, default=str)
        html += '</pre>'
        html += '</div>'

    html += '</body></html>'
    return html


# ═══════════════════════════════════════════════════════════
#  EXPORT ROUTE
# ═══════════════════════════════════════════════════════════

@app.route('/export', methods=['POST'])
def export():
    ip = request.remote_addr
    if is_rate_limited(ip):
        abort(429)

    dates    = request.form.getlist('dates')
    descs    = request.form.getlist('descs')
    amounts  = request.form.getlist('amounts')
    balances = request.form.getlist('balances')
    types    = request.form.getlist('types')
    cats     = request.form.getlist('categories')
    keyword  = request.form.get('keyword', '')[:100]

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Transactions"
    ws.append(["#", "Date", "Description", "Type",
               "Amount (₹)", "Balance (₹)", "Category"])

    for i, (d, desc, a, b, tp, cat) in enumerate(
            zip(dates, descs, amounts, balances, types,
                cats or [''] * len(dates)), 1):
        try:
            amt = float(a) if a else ''
        except ValueError:
            amt = ''
        try:
            bal = float(b) if b else ''
        except ValueError:
            bal = ''

        ws.append([i, d, _sanitize_desc(desc), tp, amt, bal, cat])

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    safe_kw = secure_filename(keyword) if keyword else 'transactions'
    return send_file(
        output,
        download_name=f'transactions_{safe_kw}.xlsx',
        as_attachment=True,
    )


# ═══════════════════════════════════════════════════════════
#  ERROR HANDLERS
# ═══════════════════════════════════════════════════════════

@app.errorhandler(429)
def too_many_requests(e):
    return "Too many requests. Please wait a minute and try again.", 429


@app.errorhandler(413)
def file_too_large(e):
    return "File too large. Maximum size is 10 MB.", 413


# ═══════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == '__main__':
    debug_mode = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(debug=debug_mode)
    