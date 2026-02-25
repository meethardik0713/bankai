import os
import logging
import openpyxl
import time
import hashlib
import threading
from flask import Flask, request, render_template, send_file, abort, session, redirect
from flask_talisman import Talisman
from io import BytesIO
from collections import defaultdict
from werkzeug.utils import secure_filename
from universal_parser import parse_transactions
from supabase import create_client, Client

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)

# ── App setup ─────────────────────────────────────────────

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(32))

is_prod = os.environ.get('RAILWAY_ENVIRONMENT') is not None

Talisman(app,
    force_https=is_prod,
    strict_transport_security=is_prod,
    session_cookie_secure=is_prod,
    content_security_policy=False
)
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

# ── Auth Routes ───────────────────────────────────────────

@app.route('/login')
def login():
    if os.environ.get('RAILWAY_ENVIRONMENT'):
        redirect_url = 'https://www.aarogyamfin.com/auth/callback'
    else:
        redirect_url = request.host_url + 'auth/callback'
    response = supabase.auth.sign_in_with_oauth({
        "provider": "google",
        "options": {"redirect_to": redirect_url}
    })
    return redirect(response.url)


@app.route('/auth/callback')
def auth_callback():
    code = request.args.get('code')
    if code:
        supabase.auth.exchange_code_for_session({"auth_code": code})
    return redirect('/')


@app.route('/logout')
def logout():
    supabase.auth.sign_out()
    session.clear()
    return redirect('/')
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
    