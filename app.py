import os
from dotenv import load_dotenv
load_dotenv()
import logging
import openpyxl
import time
import hashlib
import threading
import hmac
import razorpay
from flask import Flask, request, render_template, send_file, abort, session, redirect, jsonify
from flask_cors import CORS
from flask_talisman import Talisman
from io import BytesIO
from collections import defaultdict
from werkzeug.utils import secure_filename
from universal_parser import parse_transactions
from core.verifier import run_accuracy_check
from core.sqlite_indexer import build_index, drop_index, get_db
from core.chat_engine import verify_data, chat as ai_chat
from supabase import create_client, Client

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

RAZORPAY_KEY_ID     = os.environ.get('RAZORPAY_KEY_ID')
RAZORPAY_KEY_SECRET = os.environ.get('RAZORPAY_KEY_SECRET')
rzp_client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ── App setup ─────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(32))
CORS(app, resources={r"/api/*": {"origins": "*"}})

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

CHAT_MESSAGE_LIMIT = 10
CHAT_SESSION_HOURS = 48

# ── Rate limiting ─────────────────────────────────────────
request_counts = defaultdict(list)
RATE_LIMIT  = 10
RATE_WINDOW = 60

# ── In-memory cache ───────────────────────────────────────
_CACHE      = {}
_CACHE_LOCK = threading.Lock()
_CACHE_TTL  = 1800
PAGE_SIZE   = 100
_PDF_MAGIC  = b'%PDF'


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
        _CACHE[file_hash] = {'transactions': transactions, 'filename': filename, 'ts': time.time()}


def _file_hash(file_storage) -> str:
    chunk = file_storage.stream.read(65536)
    file_storage.stream.seek(0)
    return hashlib.md5(chunk).hexdigest()


def is_rate_limited(ip: str) -> bool:
    now = time.time()
    request_counts[ip] = [t for t in request_counts[ip] if now - t < RATE_WINDOW]
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


def _get_current_user():
    try:
        access_token = session.get('access_token')
        if access_token:
            user = supabase.auth.get_user(access_token)
            if user and user.user:
                return True, user.user.email, user.user.id
    except Exception:
        pass
    return False, None, None


def _get_active_chat_session(user_id: str):
    """Check if user has an active paid chat session."""
    try:
        result = supabase.table('chat_sessions').select('*').eq(
            'user_id', user_id
        ).eq('is_active', True).lt(
            'messages_used', CHAT_MESSAGE_LIMIT
        ).order(
            'created_at', desc=True
        ).limit(1).execute()

        if result.data:
            from datetime import datetime, timezone
            session_data = result.data[0]
            expires_at = session_data.get('expires_at')
            if expires_at:
                exp = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                if exp < datetime.now(timezone.utc):
                    return None
            return session_data
    except Exception as e:
        logger.exception("Error checking chat session: %s", e)
    return None


# ═══════════════════════════════════════════════════════════
#  AUTH ROUTES
# ═══════════════════════════════════════════════════════════

@app.route('/login')
def login():
    redirect_url = ('https://www.aarogyamfin.com/auth/callback'
                    if is_prod else request.host_url + 'auth/callback')
    response = supabase.auth.sign_in_with_oauth({
        "provider": "google",
        "options":  {"redirect_to": redirect_url}
    })
    return redirect(response.url)


@app.route('/auth/callback')
def auth_callback():
    code = request.args.get('code')
    if code:
        try:
            result = supabase.auth.exchange_code_for_session({"auth_code": code})
            if result and result.session:
                session['access_token'] = result.session.access_token
                session['user_email']   = result.user.email
                session['user_id']      = result.user.id

                supabase.table('users').upsert({
                    'id':    result.user.id,
                    'email': result.user.email,
                }).execute()

                logger.info("Login success: %s", result.user.email)
        except Exception as e:
            logger.exception("Auth callback error: %s", e)
    return redirect('/')


@app.route('/debug-auth')
def debug_auth():
    return {'access_token_in_session': session.get('access_token') is not None,
            'session_keys': list(session.keys())}


@app.route('/logout')
def logout():
    supabase.auth.sign_out()
    session.clear()
    return redirect('/')


# ═══════════════════════════════════════════════════════════
#  PAYMENT ROUTES
# ═══════════════════════════════════════════════════════════

@app.route('/pay')
def pay_page():
    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return redirect('/login')

    active = _get_active_chat_session(user_id)
    if active:
        return redirect('/chat')

    return render_template('payment.html',
        razorpay_key_id = RAZORPAY_KEY_ID,
        user_email      = user_email,
        is_logged_in    = is_logged_in,
    )


@app.route('/payment/create-order', methods=['POST'])
def create_order():
    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return jsonify({'error': 'Not logged in'}), 401

    try:
        order = rzp_client.order.create({
            'amount':          1000,
            'currency':        'INR',
            'payment_capture': 1,
        })

        supabase.table('payments').insert({
            'user_id':           user_id,
            'amount':            10,
            'status':            'pending',
            'razorpay_order_id': order['id'],
        }).execute()

        logger.info("Order created: %s for %s", order['id'], user_email)
        return jsonify({
            'order_id': order['id'],
            'amount':   1000,
            'currency': 'INR',
            'key_id':   RAZORPAY_KEY_ID,
            'email':    user_email,
        })

    except Exception as e:
        logger.exception("Order creation failed: %s", e)
        return jsonify({'error': 'Order creation failed'}), 500


@app.route('/payment/verify', methods=['POST'])
def verify_payment():
    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return jsonify({'error': 'Not logged in'}), 401

    data                = request.get_json()
    razorpay_order_id   = data.get('razorpay_order_id')
    razorpay_payment_id = data.get('razorpay_payment_id')
    razorpay_signature  = data.get('razorpay_signature')

    try:
        msg      = f"{razorpay_order_id}|{razorpay_payment_id}"
        expected = hmac.new(
            RAZORPAY_KEY_SECRET.encode(),
            msg.encode(),
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(expected, razorpay_signature):
            logger.warning("Signature mismatch for order %s", razorpay_order_id)
            return jsonify({'error': 'Payment verification failed'}), 400

    except Exception as e:
        logger.exception("Signature verification error: %s", e)
        return jsonify({'error': 'Verification error'}), 500

    try:
        result = supabase.table('payments').update({
            'status':              'paid',
            'razorpay_payment_id': razorpay_payment_id,
        }).eq('razorpay_order_id', razorpay_order_id).execute()

        payment_id = result.data[0]['id'] if result.data else None

        supabase.table('chat_sessions').insert({
            'user_id':       user_id,
            'payment_id':    payment_id,
            'messages_used': 0,
            'is_active':     True,
        }).execute()

        logger.info("Payment verified + session created for %s", user_email)
        return jsonify({'success': True, 'redirect': '/chat'})

    except Exception as e:
        logger.exception("Post-payment DB error: %s", e)
        return jsonify({'error': 'Session creation failed'}), 500


# ═══════════════════════════════════════════════════════════
#  CHAT ROUTES
# ═══════════════════════════════════════════════════════════

@app.route('/chat', methods=['GET', 'POST'])
def chat_page():
    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return redirect('/login')

    active_session = _get_active_chat_session(user_id)
    if not active_session:
        return redirect('/pay')

    messages_left = CHAT_MESSAGE_LIMIT - active_session['messages_used']
    session_id    = str(active_session['id'])

    db_ready      = get_db(session_id) is not None
    upload_error  = None
    verify_report = None

    # ── PDF uploaded directly on /chat ────────────────────
    if request.method == 'POST' and 'pdf_file' in request.files:
        file = request.files.get('pdf_file')
        if file and file.filename:
            safe_name = secure_filename(file.filename)
            if not safe_name.lower().endswith('.pdf'):
                upload_error = 'Only PDF files accepted.'
            elif not _is_valid_pdf(file):
                upload_error = 'Invalid PDF file.'
            else:
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_name)
                file.save(filepath)
                try:
                    transactions = parse_transactions(filepath)
                    if transactions:
                        row_count     = build_index(session_id, transactions)
                        verify_report = verify_data(session_id)
                        db_ready      = True
                        logger.info("Chat DB built: %d rows for session %s", row_count, session_id)
                    else:
                        upload_error = 'No transactions found in PDF.'
                except Exception as e:
                    upload_error = f'Parse error: {e}'
                finally:
                    if os.path.exists(filepath):
                        os.remove(filepath)

    # ── Auto-load from main page cache ───────────────────
    elif not db_ready:
        cached_hash = session.get('file_hash')
        if cached_hash:
            cached = _cache_get(cached_hash)
            if cached and cached.get('transactions'):
                row_count     = build_index(session_id, cached['transactions'])
                verify_report = verify_data(session_id)
                db_ready      = True
                logger.info("Chat DB auto-loaded from cache: %d rows", row_count)

    return render_template('chat.html',
        is_logged_in  = is_logged_in,
        user_email    = user_email,
        messages_left = messages_left,
        session_id    = session_id,
        db_ready      = db_ready,
        verify_report = verify_report,
        upload_error  = upload_error,
    )


@app.route('/chat/message', methods=['POST'])
def chat_message():
    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return jsonify({'error': 'Not logged in'}), 401

    active_session = _get_active_chat_session(user_id)
    if not active_session:
        return jsonify({'error': 'No active session. Please pay to continue.'}), 403

    messages_left = CHAT_MESSAGE_LIMIT - active_session['messages_used']
    if messages_left <= 0:
        return jsonify({'error': 'Message limit reached. Please purchase a new session.'}), 403

    data       = request.get_json()
    user_msg   = (data.get('message') or '').strip()[:500]
    history    = data.get('history') or []
    session_id = str(active_session['id'])

    if not user_msg:
        return jsonify({'error': 'Empty message'}), 400

    if not get_db(session_id):
        return jsonify({'error': 'No statement loaded. Please upload a PDF first.'}), 400

    result = ai_chat(session_id, user_msg, history)

    try:
        supabase.table('chat_sessions').update({
            'messages_used': active_session['messages_used'] + 1
        }).eq('id', active_session['id']).execute()
    except Exception as e:
        logger.exception("Failed to update message count: %s", e)

    return jsonify({
        'reply':         result['reply'],
        'messages_left': messages_left - 1,
        'tokens_used':   result.get('tokens_used', 0),
    })


# ═══════════════════════════════════════════════════════════
#  MAIN ROUTE
# ═══════════════════════════════════════════════════════════

@app.route('/', methods=['GET', 'POST'])
def home():
    ip = request.remote_addr
    if request.method == 'POST' and is_rate_limited(ip):
        abort(429)

    is_logged_in, user_email, user_id = _get_current_user()

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
    page_num          = 1
    total_pages       = 1
    total_count       = 0

    cached_hash = session.get('file_hash')
    cached_name = session.get('file_name', '')

    if request.method == 'POST':
        keyword          = request.form.get('keyword', '')[:100]
        file             = request.files.get('pdf_file')
        file_uploaded    = file and file.filename
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
                fhash  = _file_hash(file)
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
                        t0               = time.time()
                        all_transactions = parse_transactions(filepath)
                        parse_time       = round(time.time() - t0, 1)
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
                if t.get('amount'):
                    amounts_count += 1
                    if t.get('type') == 'DR':
                        total_debit  += t['amount']
                    else:
                        total_credit += t['amount']

            total       = total_debit + total_credit
            total_count = len(filtered)
            page_num    = int(request.form.get('page', 1))
            total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
            page_num    = max(1, min(page_num, total_pages))
            start            = (page_num - 1) * PAGE_SIZE
            transaction_data = filtered[start: start + PAGE_SIZE]
            count            = total_count

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
        is_logged_in      = is_logged_in,
        user_email        = user_email,
        page_num          = page_num,
        total_pages       = total_pages,
        total_count       = total_count,
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
        t0           = time.time()
        transactions = parse_transactions(filepath)
        parse_time   = round(time.time() - t0, 1)
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

    report = run_accuracy_check(transactions)
    return render_template('accuracy.html', filename=safe_name, parse_time=parse_time, **report)


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
    ws.append(["#", "Date", "Description", "Type", "Amount (₹)", "Balance (₹)", "Category"])

    for i, (d, desc, a, b, tp, cat) in enumerate(
            zip(dates, descs, amounts, balances, types,
                cats or [''] * len(dates)), 1):
        try:    amt = float(a) if a else ''
        except: amt = ''
        try:    bal = float(b) if b else ''
        except: bal = ''
        ws.append([i, d, _sanitize_desc(desc), tp, amt, bal, cat])

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    safe_kw = secure_filename(keyword) if keyword else 'transactions'
    return send_file(output, download_name=f'transactions_{safe_kw}.xlsx', as_attachment=True)


# ═══════════════════════════════════════════════════════════
#  MOBILE API ROUTES
# ═══════════════════════════════════════════════════════════

@app.route('/api/parse', methods=['POST'])
def api_parse():
    ip = request.remote_addr
    if is_rate_limited(ip):
        return jsonify({'error': 'Too many requests'}), 429

    file = request.files.get('pdf_file')
    if not file or not file.filename:
        return jsonify({'error': 'No file uploaded'}), 400

    safe_name = secure_filename(file.filename)
    if not safe_name.lower().endswith('.pdf'):
        return jsonify({'error': 'Only PDF files accepted'}), 400
    if not _is_valid_pdf(file):
        return jsonify({'error': 'Invalid PDF'}), 400

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_name)
    file.save(filepath)

    try:
        t0           = time.time()
        transactions = parse_transactions(filepath)
        parse_time   = round(time.time() - t0, 1)
        logger.info("API parse: %s → %d txns in %.1fs", safe_name, len(transactions), parse_time)
    except Exception as e:
        logger.exception("API parse error: %s", safe_name)
        return jsonify({'error': str(e)}), 500
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    if not transactions:
        return jsonify({'error': 'No transactions found'}), 400

    return jsonify({
        'transactions': transactions,
        'count':        len(transactions),
        'parse_time':   parse_time,
        'filename':     safe_name,
    }), 200


# ═══════════════════════════════════════════════════════════
#  MOBILE PAYMENT API
# ═══════════════════════════════════════════════════════════

@app.route('/api/payment/create-order', methods=['POST'])
def api_create_order():
    ip = request.remote_addr
    if is_rate_limited(ip):
        return jsonify({'error': 'Too many requests'}), 429

    data = request.get_json()
    firebase_uid = data.get('firebase_uid', '')
    email = data.get('email', '')

    if not firebase_uid:
        return jsonify({'error': 'No user ID'}), 400

    try:
        order = rzp_client.order.create({
            'amount': 1000,
            'currency': 'INR',
            'payment_capture': 1,
        })

        supabase.table('payments').insert({
            'amount': 10,
            'status': 'pending',
            'razorpay_order_id': order['id'],
        }).execute()

        return jsonify({
            'order_id': order['id'],
            'amount': 1000,
            'currency': 'INR',
            'key_id': RAZORPAY_KEY_ID,
            'email': email,
        })
    except Exception as e:
        logger.exception("Mobile order creation failed: %s", e)
        return jsonify({'error': 'Order creation failed'}), 500


@app.route('/api/payment/verify', methods=['POST'])
def api_verify_payment():
    ip = request.remote_addr
    if is_rate_limited(ip):
        return jsonify({'error': 'Too many requests'}), 429

    data = request.get_json()
    firebase_uid = data.get('firebase_uid', '')
    razorpay_order_id = data.get('razorpay_order_id', '')
    razorpay_payment_id = data.get('razorpay_payment_id', '')
    razorpay_signature = data.get('razorpay_signature', '')

    if not firebase_uid:
        return jsonify({'error': 'No user ID'}), 400

    try:
        msg = f"{razorpay_order_id}|{razorpay_payment_id}"
        expected = hmac.new(
            RAZORPAY_KEY_SECRET.encode(),
            msg.encode(),
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(expected, razorpay_signature):
            return jsonify({'error': 'Payment verification failed'}), 400

    except Exception as e:
        return jsonify({'error': 'Verification error'}), 500

    try:
        result = supabase.table('payments').update({
            'status': 'paid',
            'razorpay_payment_id': razorpay_payment_id,
        }).eq('razorpay_order_id', razorpay_order_id).execute()

        payment_id = result.data[0]['id'] if result.data else None

        supabase.table('chat_sessions').insert({
            'firebase_uid': firebase_uid,
            'payment_id': payment_id,
            'messages_used': 0,
            'is_active': True,
            'db_ready': False,
        }).execute()

        return jsonify({'success': True})

    except Exception as e:
        logger.exception("Mobile payment verify DB error: %s", e)
        return jsonify({'error': 'Session creation failed'}), 500


@app.route('/api/payment/status', methods=['POST'])
def api_payment_status():
    data = request.get_json()
    firebase_uid = data.get('firebase_uid', '')

    if not firebase_uid:
        return jsonify({'has_access': False}), 200

    try:
        result = supabase.table('chat_sessions').select('*').eq(
            'firebase_uid', firebase_uid
        ).eq('is_active', True).order(
            'created_at', desc=True
        ).limit(1).execute()

        if result.data:
            session_data = result.data[0]
            messages_used = session_data.get('messages_used', 0)
            if messages_used < 25:
                return jsonify({
                    'has_access': True,
                    'messages_left': 25 - messages_used,
                    'session_id': str(session_data['id']),
                })

        return jsonify({'has_access': False})

    except Exception as e:
        return jsonify({'has_access': False}), 200


# ═══════════════════════════════════════════════════════════
#  MOBILE CHAT API
# ═══════════════════════════════════════════════════════════

@app.route('/api/chat/upload', methods=['POST'])
def api_chat_upload():
    ip = request.remote_addr
    if is_rate_limited(ip):
        return jsonify({'error': 'Too many requests'}), 429

    file = request.files.get('pdf_file')
    session_id = request.form.get('session_id', '')

    if not file or not file.filename:
        return jsonify({'error': 'No file uploaded'}), 400
    if not session_id:
        return jsonify({'error': 'No session_id provided'}), 400

    safe_name = secure_filename(file.filename)
    if not safe_name.lower().endswith('.pdf'):
        return jsonify({'error': 'Only PDF files accepted'}), 400
    if not _is_valid_pdf(file):
        return jsonify({'error': 'Invalid PDF'}), 400

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_name)
    file.save(filepath)

    try:
        transactions = parse_transactions(filepath)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

    if not transactions:
        return jsonify({'error': 'No transactions found'}), 400

    from core.sqlite_indexer import build_index
    row_count = build_index(session_id, transactions)

    return jsonify({
        'success': True,
        'session_id': session_id,
        'transaction_count': row_count,
    }), 200


@app.route('/api/chat/message', methods=['POST'])
def api_chat_message():
    ip = request.remote_addr
    if is_rate_limited(ip):
        return jsonify({'error': 'Too many requests'}), 429

    data = request.get_json()
    session_id = data.get('session_id', '')
    user_message = (data.get('message') or '').strip()[:500]
    history = data.get('history') or []

    if not session_id:
        return jsonify({'error': 'No session_id'}), 400
    if not user_message:
        return jsonify({'error': 'Empty message'}), 400

    from core.sqlite_indexer import get_db
    if not get_db(session_id):
        return jsonify({'error': 'No statement loaded. Upload PDF first.'}), 400

    result = ai_chat(session_id, user_message, history)

    return jsonify({
        'reply': result['reply'],
        'tokens_used': result.get('tokens_used', 0),
    }), 200


# ═══════════════════════════════════════════════════════════
#  ERROR HANDLERS
# ═══════════════════════════════════════════════════════════

@app.errorhandler(429)
def too_many_requests(e):
    return "Too many requests. Please wait a minute and try again.", 429


@app.errorhandler(413)
def file_too_large(e):
    return "File too large. Maximum size is 10 MB.", 413


if __name__ == '__main__':
    debug_mode = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(debug=debug_mode)
