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
from flask import Flask, request, render_template, send_file, abort, session, redirect, jsonify, Response
from flask_cors import CORS
from flask_talisman import Talisman
from io import BytesIO
from collections import defaultdict
from werkzeug.utils import secure_filename
from universal_parser import parse_transactions
from core.verifier import run_accuracy_check
from core.sqlite_indexer import build_index, drop_index, get_db
from core.chat_engine import verify_data, chat as ai_chat
from core.dashboard import run_dashboard
from core.gstr1 import run_gstr1
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
    strict_transport_security_preload=is_prod,
    session_cookie_secure=is_prod,
    content_security_policy={
        'default-src': "'self'",
        'script-src': ["'self'", "'unsafe-inline'", 'https://cdnjs.cloudflare.com', 'https://challenges.cloudflare.com', 'https://checkout.razorpay.com'],
        'frame-src': ["'self'", 'https://api.razorpay.com', 'https://checkout.razorpay.com'],
        'style-src': ["'self'", "'unsafe-inline'", 'https://fonts.googleapis.com'],
        'font-src': ["'self'", 'https://fonts.gstatic.com'],
        'img-src': ["'self'", 'data:', 'https:'],
        'connect-src': "'self'",
        'frame-src': "'none'",
        'object-src': "'none'",
    }
)

UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

CHAT_MESSAGE_LIMIT = 50
CHAT_SESSION_HOURS = 48

# ── Plan config ───────────────────────────────────────────
PLAN_CONFIG = {
    'Basic':    {'price': 10,  'input_tokens': 25000,   'output_tokens': 5000,  'validity_hours': 24},
    'Standard': {'price': 49,  'input_tokens': 100000,  'output_tokens': 10000, 'validity_hours': 75},
    'Pro':      {'price': 99,  'input_tokens': 300000,  'output_tokens': 15000, 'validity_hours': 125},
    'Elite':    {'price': 499, 'input_tokens': 1500000, 'output_tokens': 20000, 'validity_hours': 200},
}

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
        ).eq('is_active', True).order(
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
            'amount':          4900,
            'currency':        'INR',
            'payment_capture': 1,
        })

        supabase.table('payments').insert({
            'user_id':           user_id,
            'amount':            49,
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

        from datetime import datetime, timezone, timedelta
        expires_at = datetime.now(timezone.utc) + timedelta(hours=CHAT_SESSION_HOURS)
        supabase.table('chat_sessions').insert({
            'user_id':            user_id,
            'payment_id':         payment_id,
            'messages_used':      0,
            'is_active':          True,
            'expires_at':         expires_at.isoformat(),
            'input_tokens_limit': 75000,
            'output_tokens_limit': 10000,
            'input_tokens_used':  0,
            'output_tokens_used': 0,
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

    cached_hash    = session.get('file_hash')
    cached_name    = session.get('file_name', '')
    dashboard_data = None

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

            try:
                dashboard_data = run_dashboard(all_transactions[:300])
            except Exception as e:
                logger.exception("Dashboard POST error: %s", e)
                dashboard_data = None

    elif request.method == 'GET' and cached_hash:
        selected_filename = cached_name
        cached = _cache_get(cached_hash)
        if cached and cached.get('transactions'):
            try:
                dashboard_data = run_dashboard(cached['transactions'][:300])
            except Exception:
                dashboard_data = None
        cached = _cache_get(cached_hash)
        if cached:
            try:
                dashboard_data = run_dashboard(cached['transactions'][:300])
            except Exception as e:
                logger.exception("Dashboard GET error: %s", e)
                dashboard_data = None

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
        dashboard_data    = dashboard_data,
    )


@app.route('/clear', methods=['POST'])
def clear_cache():
    fhash = session.pop('file_hash', None)
    session.pop('file_name', None)
    if fhash:
        with _CACHE_LOCK:
            _CACHE.pop(fhash, None)
    return ('', 204)


@app.route('/sitemap.xml')
def sitemap():
    pages = [
        ('https://aarogyamfin.com/',        '2026-04-08'),
        ('https://aarogyamfin.com/about',   '2026-04-08'),
        ('https://aarogyamfin.com/accuracy','2026-04-07'),
        ('https://aarogyamfin.com/privacy', '2026-04-08'),
        ('https://aarogyamfin.com/terms',   '2026-04-08'),
        ('https://aarogyamfin.com/contact', '2026-04-08'),
    ]
    xml = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for url, lastmod in pages:
        xml.append(f'  <url><loc>{url}</loc><lastmod>{lastmod}</lastmod></url>')
    xml.append('</urlset>')
    return Response('\n'.join(xml), mimetype='application/xml')


@app.route('/llms.txt')
def llms():
    content = """# AarogyamFin — AI Bank Statement Analyzer
# https://aarogyamfin.com

## Product
AarogyamFin is an AI-powered Indian bank statement analyzer.
Users upload bank statement PDFs and instantly get transaction analysis, keyword search, Excel export, and AI chat insights.

## Supported Banks
SBI, HDFC, Kotak, Canara, Axis, Punjab National Bank, Bank of Baroda, ICICI, and all major Indian banks.

## Features
- PDF to Excel conversion
- Transaction search by keyword
- Credit/Debit categorization
- AI chat for financial insights
- Dashboard with ITR, loan eligibility, compliance analysis
- Mobile app (Android)

## Pricing
Free basic analysis. AI Chat sessions from ₹10.

## Contact
aarogyamfin@gmail.com

## URLs
Homepage: https://aarogyamfin.com
About: https://aarogyamfin.com/about
Privacy: https://aarogyamfin.com/privacy
Terms: https://aarogyamfin.com/terms
"""
    return Response(content, mimetype='text/plain')

@app.route('/robots.txt')
def robots():
    content = """User-agent: *
Allow: /

User-agent: Googlebot
Allow: /

User-agent: Google-Extended
Allow: /

User-agent: GPTBot
Allow: /

User-agent: ClaudeBot
Allow: /

User-agent: Applebot-Extended
Allow: /

User-agent: meta-externalagent
Allow: /

User-agent: CCBot
Disallow: /

User-agent: Bytespider
Disallow: /

Sitemap: https://aarogyamfin.com/sitemap.xml
"""
    return Response(content, mimetype='text/plain')

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
#  DASHBOARD ROUTES
# ═══════════════════════════════════════════════════════════

@app.route('/dashboard', methods=['GET'])
def dashboard_page():
    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return redirect('/login')
    active = _get_active_chat_session(user_id)
    if not active:
        return redirect('/pay')

    data        = None
    cached_hash = session.get('file_hash')

    if cached_hash:
        cached = _cache_get(cached_hash)
        if cached:
            data = run_dashboard(cached['transactions'])

    return render_template(
        'dashboard.html',
        data         = data,
        is_logged_in = is_logged_in,
        user_email   = user_email,
    )


@app.route('/dashboard', methods=['POST'])
def dashboard_analyze():
    ip = request.remote_addr
    if is_rate_limited(ip):
        abort(429)

    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return redirect('/login')
    active = _get_active_chat_session(user_id)
    if not active:
        return redirect('/pay')

    data          = None
    error_message = ''

    # ── Use already-cached file ────────────────────────────
    if request.form.get('use_cached'):
        cached_hash = session.get('file_hash')
        if cached_hash:
            cached = _cache_get(cached_hash)
            if cached:
                data = run_dashboard(cached['transactions'])
            else:
                error_message = 'Session expired. Please re-upload.'
        return render_template('dashboard.html',
            data          = data,
            error_message = error_message,
            is_logged_in  = is_logged_in,
            user_email    = user_email,
        )

    # ── New PDF upload ─────────────────────────────────────
    file = request.files.get('pdf_file')
    if not file or not file.filename:
        return render_template('dashboard.html',
            data          = None,
            error_message = 'No file uploaded.',
            is_logged_in  = is_logged_in,
            user_email    = user_email,
        )

    safe_name = secure_filename(file.filename)
    if not safe_name.lower().endswith('.pdf'):
        error_message = 'Only PDF files accepted.'
    elif len(safe_name) >= 200:
        error_message = 'Filename too long.'
    elif not _is_valid_pdf(file):
        error_message = 'Invalid PDF file.'
    else:
        fhash  = _file_hash(file)
        cached = _cache_get(fhash)

        if cached:
            transactions = cached['transactions']
            logger.info("Dashboard cache hit for %s", safe_name)
        else:
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_name)
            file.save(filepath)
            try:
                t0           = time.time()
                transactions = parse_transactions(filepath)
                parse_time   = round(time.time() - t0, 1)
                _cache_set(fhash, transactions, safe_name)
                session['file_hash'] = fhash
                session['file_name'] = safe_name
                logger.info("Dashboard parsed %s → %d txns in %.1fs",
                            safe_name, len(transactions), parse_time)
            except Exception as e:
                logger.exception("Dashboard parse error: %s", safe_name)
                error_message = f'Could not parse: {e}'
                transactions  = []
            finally:
                if os.path.exists(filepath):
                    os.remove(filepath)

        if transactions and not error_message:
            data = run_dashboard(transactions)

    return render_template(
        'dashboard.html',
        data          = data,
        error_message = error_message,
        is_logged_in  = is_logged_in,
        user_email    = user_email,
    )


# ═══════════════════════════════════════════════════════════
#  DASHBOARD EXCEL EXPORT
# ═══════════════════════════════════════════════════════════

@app.route('/dashboard/export', methods=['GET'])
def dashboard_export():
    ip = request.remote_addr
    if is_rate_limited(ip):
        abort(429)

    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return redirect('/login')

    cached_hash = session.get('file_hash')
    if not cached_hash:
        return "No data available. Please upload a PDF first.", 400

    cached = _cache_get(cached_hash)
    if not cached:
        return "Session expired. Please re-upload.", 400

    data     = run_dashboard(cached['transactions'])
    filename = session.get('file_name', 'statement')

    d_exp  = data['expense']
    d_itr  = data['itr']
    d_aud  = data['audit']
    d_loan = data['loan']
    d_comp = data['compliance']

    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = openpyxl.Workbook()

    COLOR_DARK       = '0D1117'
    COLOR_GOLD       = 'C9A84C'
    COLOR_GOLD_LIGHT = 'F0D98A'
    COLOR_GREEN      = '4ADE80'
    COLOR_RED        = 'F87171'
    COLOR_YELLOW     = 'FBBF24'
    COLOR_HEADER_BG  = '1A1F2E'
    COLOR_ROW_ALT    = '0F1420'
    COLOR_WHITE      = 'E8E4D9'
    COLOR_MUTED      = '8A8070'

    def hdr_fill(hex_color):
        return PatternFill('solid', fgColor=hex_color)

    def hdr_font(hex_color='E8E4D9', bold=True, size=10):
        return Font(color=hex_color, bold=bold, size=size, name='Calibri')

    def cell_font(hex_color='E8E4D9', bold=False, size=9):
        return Font(color=hex_color, bold=bold, size=size, name='Calibri')

    def thin_border():
        s = Side(style='thin', color='2A2F3E')
        return Border(left=s, right=s, top=s, bottom=s)

    def set_col_widths(ws, widths):
        for i, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = w

    def write_title(ws, title, subtitle=''):
        ws.sheet_view.showGridLines = False
        ws['A1'] = title
        ws['A1'].font = Font(color=COLOR_GOLD, bold=True, size=14, name='Calibri')
        ws['A1'].fill = hdr_fill(COLOR_DARK)
        ws['A1'].alignment = Alignment(horizontal='left', vertical='center')
        ws.row_dimensions[1].height = 32
        if subtitle:
            ws['A2'] = subtitle
            ws['A2'].font = Font(color=COLOR_MUTED, bold=False, size=9, name='Calibri')
            ws['A2'].fill = hdr_fill(COLOR_DARK)
            ws.row_dimensions[2].height = 18
            return 4
        return 3

    def write_section_header(ws, row, label, col_count=6):
        ws.cell(row=row, column=1).value = label
        ws.cell(row=row, column=1).font = Font(color=COLOR_GOLD_LIGHT, bold=True, size=9, name='Calibri')
        ws.cell(row=row, column=1).fill = hdr_fill('1A2030')
        ws.cell(row=row, column=1).alignment = Alignment(vertical='center')
        ws.row_dimensions[row].height = 20
        for c in range(2, col_count + 1):
            ws.cell(row=row, column=c).fill = hdr_fill('1A2030')

    def write_table_header(ws, row, headers):
        for c, h in enumerate(headers, 1):
            cell = ws.cell(row=row, column=c, value=h)
            cell.font = hdr_font(COLOR_GOLD)
            cell.fill = hdr_fill(COLOR_HEADER_BG)
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = thin_border()
        ws.row_dimensions[row].height = 20

    def write_data_row(ws, row, values, colors=None):
        fill_color = COLOR_ROW_ALT if row % 2 == 0 else COLOR_DARK
        for c, v in enumerate(values, 1):
            cell = ws.cell(row=row, column=c, value=v)
            color = (colors[c-1] if colors and c <= len(colors) else COLOR_WHITE)
            cell.font = cell_font(color)
            cell.fill = hdr_fill(fill_color)
            cell.alignment = Alignment(vertical='center', wrap_text=False)
            cell.border = thin_border()
        ws.row_dimensions[row].height = 16

    def write_kv(ws, row, label, value, val_color=COLOR_WHITE):
        ws.cell(row=row, column=1).value = label
        ws.cell(row=row, column=1).font = cell_font(COLOR_MUTED)
        ws.cell(row=row, column=1).fill = hdr_fill(COLOR_DARK)
        ws.cell(row=row, column=2).value = value
        ws.cell(row=row, column=2).font = cell_font(val_color, bold=True)
        ws.cell(row=row, column=2).fill = hdr_fill(COLOR_DARK)
        ws.row_dimensions[row].height = 15

    # ── SHEET 1: Overview ──
    ws1 = wb.active
    ws1.title = 'Overview'
    r = write_title(ws1, '  AarogyamFin — Financial Intelligence Report', f'  Generated for: {filename}')
    write_section_header(ws1, r, '  SUMMARY METRICS', 3)
    r += 1
    write_table_header(ws1, r, ['Metric', 'Value', ''])
    r += 1
    for label, value, color in [
        ('Total Transactions',   data['total_txns'],                                                 COLOR_WHITE),
        ('Total Credits',        f'Rs {data["total_cr"]:,.0f}',                                      COLOR_GREEN),
        ('Total Debits',         f'Rs {data["total_dr"]:,.0f}',                                      COLOR_RED),
        ('Real Income',          f'Rs {d_itr["real_income_total"]:,.0f}',                            COLOR_GREEN),
        ('Loan Disbursals',      f'Rs {d_itr["loan_disbursal_total"]:,.0f}',                         COLOR_RED),
        ('Family Transfers',     f'Rs {d_itr.get("family_transfer_total", 0):,.0f}',                 COLOR_YELLOW),
        ('Avg Monthly Income',   f'Rs {d_loan["avg_monthly_credit"]:,.0f}',                          COLOR_GREEN),
        ('Avg Monthly Spend',    f'Rs {d_loan["avg_monthly_debit"]:,.0f}',                           COLOR_RED),
        ('Avg Balance',          f'Rs {d_loan["avg_balance"]:,.0f}',                                 COLOR_GOLD_LIGHT),
        ('FOIR',                 f'{d_loan["foir"]}%',                                               COLOR_YELLOW),
        ('Loan Eligible',        f'Rs {d_loan["loan_eligible"]:,.0f}',                               COLOR_GREEN),
        ('Credit Profile',       d_loan['credit_indicator'],                                         COLOR_GOLD_LIGHT),
        ('Reconciliation Score', f'{d_aud["reconciliation_score"]}/100',                             COLOR_GREEN),
        ('Balance Mismatches',   d_aud['mismatch_count'],                                            COLOR_RED if d_aud['mismatch_count'] > 0 else COLOR_GREEN),
        ('EMIs Detected',        d_aud['emi_count'],                                                 COLOR_YELLOW),
        ('Compliance Risk',      d_comp['risk_level'],                                               COLOR_RED if d_comp['risk_level'] == 'High' else COLOR_YELLOW if d_comp['risk_level'] == 'Medium' else COLOR_GREEN),
        ('Suggested ITR Form',   d_itr['suggested_itr'].split('(')[0].strip(),                       COLOR_GOLD_LIGHT),
        ('80C Deductions',       f'Rs {d_itr["section_80c_total"]:,.0f}',                            COLOR_YELLOW),
        ('80D Deductions',       f'Rs {d_itr["section_80d_total"]:,.0f}',                            COLOR_YELLOW),
        ('Form 61A Required',    'YES' if d_comp['form_61a_required'] else 'NO',                     COLOR_RED if d_comp['form_61a_required'] else COLOR_GREEN),
    ]:
        write_kv(ws1, r, f'  {label}', value, color)
        r += 1
    set_col_widths(ws1, [32, 28, 10])

    # ── SHEET 2: Expenses ──
    ws2 = wb.create_sheet('Expenses')
    r = write_title(ws2, '  Module 01 — Expense Categorization', '  Business vs Personal · GST-eligible transactions')
    write_section_header(ws2, r, '  EXPENSE SUMMARY', 4)
    r += 1
    write_table_header(ws2, r, ['Category', 'Amount (Rs)', 'Count', '% of Debits'])
    r += 1
    for row_data in [
        ('Business Expenses',  d_exp['business_total'],     len(d_exp['business']),     d_exp['business_pct']),
        ('Personal Expenses',  d_exp['personal_total'],     len(d_exp['personal']),     d_exp['personal_pct']),
        ('GST Input Eligible', d_exp['gst_eligible_total'], len(d_exp['gst_eligible']), round(d_exp['gst_eligible_total'] / d_exp['total_debits'] * 100, 1) if d_exp['total_debits'] else 0),
        ('Mixed/Unclassified', d_exp['mixed_total'],        len(d_exp['mixed']),        round(d_exp['mixed_total'] / d_exp['total_debits'] * 100, 1) if d_exp['total_debits'] else 0),
    ]:
        write_data_row(ws2, r, [f'  {row_data[0]}', f'Rs {row_data[1]:,.0f}', row_data[2], f'{row_data[3]:.1f}%'])
        r += 1
    r += 1
    write_section_header(ws2, r, '  CATEGORY BREAKDOWN', 4)
    r += 1
    write_table_header(ws2, r, ['Category', 'Amount (Rs)', '% of Total', ''])
    r += 1
    for cat, amt in d_exp['category_totals'].items():
        pct = round(amt / d_exp['total_debits'] * 100, 1) if d_exp['total_debits'] else 0
        write_data_row(ws2, r, [f'  {cat}', f'Rs {amt:,.0f}', f'{pct}%', ''])
        r += 1
    r += 1
    write_section_header(ws2, r, '  GST INPUT CREDIT ELIGIBLE TRANSACTIONS', 4)
    r += 1
    write_table_header(ws2, r, ['Date', 'Description', 'Amount (Rs)', 'Type'])
    r += 1
    for t in d_exp['gst_eligible']:
        write_data_row(ws2, r, [t.get('date',''), t.get('desc','')[:80], f'Rs {t.get("amount",0):,.2f}', 'Business'])
        r += 1
    set_col_widths(ws2, [14, 55, 20, 16])

    # ── SHEET 3: ITR ──
    ws3 = wb.create_sheet('ITR Tax')
    r = write_title(ws3, '  Module 02 — ITR / Tax Filing', f'  Suggested: {d_itr["suggested_itr"].split("(")[0].strip()}')
    write_section_header(ws3, r, '  INCOME SUMMARY', 3)
    r += 1
    write_table_header(ws3, r, ['Income Source', 'Total Amount (Rs)', 'Transactions'])
    r += 1
    colors_map = [COLOR_WHITE, COLOR_WHITE, COLOR_WHITE, COLOR_WHITE, COLOR_RED, COLOR_YELLOW, COLOR_GREEN]
    for i, (src, amt, cnt) in enumerate([
        ('Salary',             d_itr['salary_total'],                  len(d_itr['income_sources'].get('salary', []))),
        ('Freelance/Business', d_itr['freelance_total'],               len(d_itr['income_sources'].get('freelance', []))),
        ('Interest Income',    d_itr['interest_total'],                len(d_itr['income_sources'].get('interest', []))),
        ('Other Credits',      d_itr.get('other_credits', 0),          0),
        ('Loan Disbursals',    d_itr['loan_disbursal_total'],          0),
        ('Family Transfers',   d_itr.get('family_transfer_total', 0), 0),
        ('REAL INCOME TOTAL',  d_itr['real_income_total'],             0),
    ]):
        clr = colors_map[i]
        write_data_row(ws3, r, [f'  {src}', f'Rs {amt:,.0f}', cnt if cnt else '—'], colors=[clr, clr, clr])
        r += 1
    r += 1
    write_section_header(ws3, r, '  TAX DEDUCTIONS', 3)
    r += 1
    write_table_header(ws3, r, ['Section', 'Total (Rs)', 'Max Limit'])
    r += 1
    write_data_row(ws3, r, ['  80C (LIC/PF/PPF)', f'Rs {d_itr["section_80c_total"]:,.0f}', 'Rs 1,50,000'])
    r += 1
    write_data_row(ws3, r, ['  80D (Health Ins)',  f'Rs {d_itr["section_80d_total"]:,.0f}', 'Rs 25,000'])
    r += 1
    r += 1
    write_section_header(ws3, r, '  HIGH VALUE CREDITS >= Rs 1 LAKH', 3)
    r += 1
    write_table_header(ws3, r, ['Date', 'Description', 'Amount (Rs)'])
    r += 1
    for t in d_itr['high_value_credits']:
        write_data_row(ws3, r, [t.get('date',''), t.get('desc','')[:80], f'Rs {t.get("amount",0):,.0f}'],
                       colors=[COLOR_MUTED, COLOR_WHITE, COLOR_GREEN])
        r += 1
    set_col_widths(ws3, [30, 55, 22])

    # ── SHEET 4: Audit ──
    ws4 = wb.create_sheet('Audit')
    r = write_title(ws4, '  Module 03 — Audit & Reconciliation', f'  Score: {d_aud["reconciliation_score"]}/100')
    write_section_header(ws4, r, '  AUDIT SUMMARY', 3)
    r += 1
    for label, value, color in [
        ('Reconciliation Score', f'{d_aud["reconciliation_score"]}/100',  COLOR_GREEN if d_aud['reconciliation_score'] >= 80 else COLOR_YELLOW),
        ('Balance Mismatches',   d_aud['mismatch_count'],                 COLOR_RED if d_aud['mismatch_count'] > 0 else COLOR_GREEN),
        ('Bounced Transactions', d_aud['bounce_count'],                   COLOR_RED if d_aud['bounce_count'] > 0 else COLOR_GREEN),
        ('EMIs Detected',        d_aud['emi_count'],                      COLOR_YELLOW),
        ('EMI Total Outflow',    f'Rs {d_aud["total_emi_outflow"]:,.0f}', COLOR_RED),
        ('EMI to Income Ratio',  f'{d_aud["emi_to_income_ratio"]}%',      COLOR_YELLOW),
        ('Duplicate Suspects',   d_aud['duplicate_count'],                COLOR_YELLOW if d_aud['duplicate_count'] > 0 else COLOR_GREEN),
    ]:
        write_kv(ws4, r, f'  {label}', value, color)
        r += 1
    r += 1
    write_section_header(ws4, r, '  EMI / LOAN OUTFLOWS', 4)
    r += 1
    write_table_header(ws4, r, ['Date', 'Description', 'Amount (Rs)', 'Type'])
    r += 1
    for t in d_aud['emis']:
        write_data_row(ws4, r, [t.get('date',''), t.get('desc','')[:80], f'Rs {t.get("amount",0):,.2f}', 'EMI/Loan'],
                       colors=[COLOR_MUTED, COLOR_WHITE, COLOR_RED, COLOR_YELLOW])
        r += 1
    if d_aud['bounced']:
        r += 1
        write_section_header(ws4, r, '  BOUNCED TRANSACTIONS', 3)
        r += 1
        write_table_header(ws4, r, ['Date', 'Description', 'Amount (Rs)'])
        r += 1
        for t in d_aud['bounced']:
            write_data_row(ws4, r, [t.get('date',''), t.get('desc','')[:80], f'Rs {t.get("amount",0):,.2f}'],
                           colors=[COLOR_MUTED, COLOR_WHITE, COLOR_RED])
            r += 1
    set_col_widths(ws4, [14, 55, 20, 16])

    # ── SHEET 5: Loan ──
    ws5 = wb.create_sheet('Loan')
    r = write_title(ws5, '  Module 04 — Loan & Credit Assessment', f'  Profile: {d_loan["credit_indicator"]}')
    write_section_header(ws5, r, '  LOAN METRICS', 3)
    r += 1
    for label, value, color in [
        ('Credit Profile',          d_loan['credit_indicator'],                                               COLOR_GREEN if d_loan['credit_color'] == 'green' else COLOR_RED if d_loan['credit_color'] == 'red' else COLOR_YELLOW),
        ('FOIR',                    f'{d_loan["foir"]}%',                                                     COLOR_RED if d_loan['foir'] > 50 else COLOR_YELLOW if d_loan['foir'] > 35 else COLOR_GREEN),
        ('DSCR',                    f'{d_loan["dscr"]}x' if d_loan.get('dscr') else 'N/A',                   COLOR_WHITE),
        ('Avg Monthly Income',      f'Rs {d_loan["avg_monthly_credit"]:,.0f}',                                COLOR_GREEN),
        ('Avg Monthly Spend',       f'Rs {d_loan["avg_monthly_debit"]:,.0f}',                                 COLOR_RED),
        ('Avg Balance',             f'Rs {d_loan["avg_balance"]:,.0f}',                                       COLOR_GOLD_LIGHT),
        ('Min Balance',             f'Rs {d_loan["min_balance"]:,.0f}',                                       COLOR_YELLOW),
        ('Negative Balance Months', d_loan['negative_months'],                                                COLOR_RED if d_loan['negative_months'] > 0 else COLOR_GREEN),
        ('Remaining EMI Capacity',  f'Rs {d_loan["remaining_emi_capacity"]:,.0f}/mo',                         COLOR_GREEN),
        ('Estimated Loan Eligible', f'Rs {d_loan["loan_eligible"]:,.0f}',                                     COLOR_GREEN if d_loan['loan_eligible'] > 0 else COLOR_RED),
        ('Months Analyzed',         d_loan['months_analyzed'],                                                COLOR_WHITE),
    ]:
        write_kv(ws5, r, f'  {label}', value, color)
        r += 1
    r += 1
    write_section_header(ws5, r, '  MONTHLY CASH FLOW', 4)
    r += 1
    write_table_header(ws5, r, ['Month', 'Credits (Rs)', 'Debits (Rs)', 'Net (Rs)'])
    r += 1
    for month in sorted(d_loan['monthly_data'].keys()):
        md  = d_loan['monthly_data'][month]
        net = md['credits'] - md['debits']
        write_data_row(ws5, r, [month, f'Rs {md["credits"]:,.0f}', f'Rs {md["debits"]:,.0f}', f'Rs {net:,.0f}'],
                       colors=[COLOR_MUTED, COLOR_GREEN, COLOR_RED, COLOR_GREEN if net >= 0 else COLOR_RED])
        r += 1
    set_col_widths(ws5, [18, 22, 22, 22])

    # ── SHEET 6: Compliance ──
    ws6 = wb.create_sheet('Compliance')
    r = write_title(ws6, '  Module 05 — Compliance Reporting', f'  Risk: {d_comp["risk_level"]} · Score: {d_comp["risk_score"]}')
    write_section_header(ws6, r, '  COMPLIANCE SUMMARY', 3)
    r += 1
    for label, value, color in [
        ('Risk Level',             d_comp['risk_level'],                                                       COLOR_RED if d_comp['risk_level'] == 'High' else COLOR_YELLOW if d_comp['risk_level'] == 'Medium' else COLOR_GREEN),
        ('Risk Score',             d_comp['risk_score'],                                                       COLOR_WHITE),
        ('High Value Txns (>=2L)', d_comp['high_value_count'],                                                 COLOR_YELLOW if d_comp['high_value_count'] > 5 else COLOR_GREEN),
        ('Cash Transactions',      d_comp['cash_count'],                                                       COLOR_WHITE),
        ('STR Candidates',         d_comp['str_count'],                                                        COLOR_RED if d_comp['str_count'] > 0 else COLOR_GREEN),
        ('Structuring Suspects',   d_comp['structured_count'],                                                 COLOR_RED if d_comp['structured_count'] > 0 else COLOR_GREEN),
        ('Daily Limit Breaches',   len(d_comp['daily_breaches']),                                              COLOR_RED if d_comp['daily_breaches'] else COLOR_GREEN),
        ('Annual Cash Total',      f'Rs {d_comp["annual_cash_total"]:,.0f}',                                   COLOR_YELLOW),
        ('Form 61A / SFT',         'REQUIRED' if d_comp['form_61a_required'] else 'Not Required',              COLOR_RED if d_comp['form_61a_required'] else COLOR_GREEN),
    ]:
        write_kv(ws6, r, f'  {label}', value, color)
        r += 1
    if d_comp['high_value_txns']:
        r += 1
        write_section_header(ws6, r, '  HIGH VALUE TRANSACTIONS >= Rs 2 LAKH', 4)
        r += 1
        write_table_header(ws6, r, ['Date', 'Description', 'Type', 'Amount (Rs)'])
        r += 1
        for t in d_comp['high_value_txns']:
            clr = COLOR_GREEN if t.get('type') == 'CR' else COLOR_RED
            write_data_row(ws6, r, [t.get('date',''), t.get('desc','')[:80], t.get('type',''), f'Rs {t.get("amount",0):,.0f}'],
                           colors=[COLOR_MUTED, COLOR_WHITE, clr, clr])
            r += 1
    if d_comp['str_candidates']:
        r += 1
        write_section_header(ws6, r, '  STR CANDIDATES — SINGLE DAY >= Rs 5 LAKH', 3)
        r += 1
        write_table_header(ws6, r, ['Date', 'Daily Total (Rs)', 'Transactions'])
        r += 1
        for s in d_comp['str_candidates']:
            write_data_row(ws6, r, [s.get('date',''), f'Rs {s.get("total",0):,.0f}', f'{s.get("count",0)} txns'],
                           colors=[COLOR_MUTED, COLOR_YELLOW, COLOR_WHITE])
            r += 1
    set_col_widths(ws6, [28, 55, 16, 22])

    # ── Full dark background on all used cells ──
    dark_fill = PatternFill('solid', fgColor=COLOR_DARK)
    for ws in wb.worksheets:
        ws.sheet_properties.tabColor = COLOR_GOLD
        # Fill all cells in used range with dark background
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row or 1,
                                 min_col=1, max_col=max(ws.max_column or 1, 8)):
            for cell in row:
                if not cell.fill or cell.fill.patternType in (None, 'none'):
                    cell.fill = dark_fill
                if not cell.font or cell.font.color is None:
                    cell.font = Font(color=COLOR_WHITE, size=9, name='Calibri')
        # Fix column widths
        ws.column_dimensions['A'].width = 35
        ws.column_dimensions['B'].width = 30
        for col_letter in ['C', 'D', 'E', 'F']:
            if ws.column_dimensions[col_letter].width < 12:
                ws.column_dimensions[col_letter].width = 14

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    safe_name = secure_filename(filename.replace('.pdf', ''))
    return send_file(output,
                     download_name=f'AarogyamFin_Report_{safe_name}.xlsx',
                     as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


# ═══════════════════════════════════════════════════════════
#  GSTR-2B RECONCILIATION ROUTE
# ═══════════════════════════════════════════════════════════

@app.route('/gstr2b', methods=['GET'])
def gstr2b_page():
    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return redirect('/login')
    return render_template('gstr2b.html', data=None, is_logged_in=is_logged_in, user_email=user_email)


@app.route('/gstr2b', methods=['POST'])
def gstr2b_analyze():
    ip = request.remote_addr
    if is_rate_limited(ip):
        abort(429)

    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return redirect('/login')

    from core.gstr2b_recon import run_gstr2b_recon
    error_message = ''
    data          = None
    gstr2b_path   = None
    pr_path       = None

    gstr2b_file = request.files.get('gstr2b_excel')
    if not gstr2b_file or not gstr2b_file.filename:
        error_message = 'GSTR-2B Excel file is required.'
        return render_template('gstr2b.html', data=None, error_message=error_message,
                               is_logged_in=is_logged_in, user_email=user_email)

    safe_g = secure_filename(gstr2b_file.filename)
    if not safe_g.lower().endswith(('.xlsx', '.xls')):
        error_message = 'GSTR-2B file must be .xlsx or .xls'
        return render_template('gstr2b.html', data=None, error_message=error_message,
                               is_logged_in=is_logged_in, user_email=user_email)

    gstr2b_path = os.path.join(app.config['UPLOAD_FOLDER'], 'gstr2b_' + safe_g)
    gstr2b_file.save(gstr2b_path)

    pr_file = request.files.get('pr_excel')
    if not pr_file or not pr_file.filename:
        error_message = 'Purchase Register Excel file is required.'
        if gstr2b_path and os.path.exists(gstr2b_path):
            os.remove(gstr2b_path)
        return render_template('gstr2b.html', data=None, error_message=error_message,
                               is_logged_in=is_logged_in, user_email=user_email)

    safe_p = secure_filename(pr_file.filename)
    if not safe_p.lower().endswith(('.xlsx', '.xls')):
        error_message = 'Purchase Register file must be .xlsx or .xls'
        if gstr2b_path and os.path.exists(gstr2b_path):
            os.remove(gstr2b_path)
        return render_template('gstr2b.html', data=None, error_message=error_message,
                               is_logged_in=is_logged_in, user_email=user_email)

    pr_path = os.path.join(app.config['UPLOAD_FOLDER'], 'pr_' + safe_p)
    pr_file.save(pr_path)

    try:
        data = run_gstr2b_recon(gstr2b_path, pr_path)
    except Exception as e:
        logger.exception("GSTR-2B recon error: %s", e)
        error_message = f'Analysis error: {e}'
    finally:
        for fp in [gstr2b_path, pr_path]:
            if fp and os.path.exists(fp):
                os.remove(fp)

    return render_template('gstr2b.html', data=data, error_message=error_message,
                           is_logged_in=is_logged_in, user_email=user_email)


# ═══════════════════════════════════════════════════════════
#  GSTR-3B ROUTE
# ═══════════════════════════════════════════════════════════

@app.route('/gstr3b', methods=['GET'])
def gstr3b_page():
    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return redirect('/login')
    return render_template('gstr3b.html', data=None, is_logged_in=is_logged_in, user_email=user_email)


@app.route('/gstr3b', methods=['POST'])
def gstr3b_analyze():
    ip = request.remote_addr
    if is_rate_limited(ip):
        abort(429)

    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return redirect('/login')

    from core.gstr3b import run_gstr3b
    error_message    = ''
    data             = None
    transactions     = []
    sales_filepath   = None
    purchase_filepath= None

    # ── Bank PDF ──
    bank_pdf = request.files.get('bank_pdf')
    if bank_pdf and bank_pdf.filename:
        safe_name = secure_filename(bank_pdf.filename)
        if not safe_name.lower().endswith('.pdf'):
            error_message = 'Bank file must be a PDF.'
        elif not _is_valid_pdf(bank_pdf):
            error_message = 'Invalid bank PDF.'
        else:
            fhash  = _file_hash(bank_pdf)
            cached = _cache_get(fhash)
            if cached:
                transactions = cached['transactions']
            else:
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_name)
                bank_pdf.save(filepath)
                try:
                    transactions = parse_transactions(filepath)
                    _cache_set(fhash, transactions, safe_name)
                except Exception as e:
                    error_message = f'Bank PDF parse error: {e}'
                finally:
                    if os.path.exists(filepath):
                        os.remove(filepath)
    else:
        cached_hash = session.get('file_hash')
        if cached_hash:
            cached = _cache_get(cached_hash)
            if cached:
                transactions = cached['transactions']

    if error_message:
        return render_template('gstr3b.html', data=None, error_message=error_message,
                               is_logged_in=is_logged_in, user_email=user_email)

    # ── Sales Excel ──
    sales_file = request.files.get('sales_excel')
    if sales_file and sales_file.filename:
        safe_s = secure_filename(sales_file.filename)
        if safe_s.lower().endswith(('.xlsx', '.xls')):
            sales_filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'sales_' + safe_s)
            sales_file.save(sales_filepath)

    # ── Purchase Excel ──
    purchase_file = request.files.get('purchase_excel')
    if purchase_file and purchase_file.filename:
        safe_p = secure_filename(purchase_file.filename)
        if safe_p.lower().endswith(('.xlsx', '.xls')):
            purchase_filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'purchase_' + safe_p)
            purchase_file.save(purchase_filepath)

    try:
        data = run_gstr3b(transactions, sales_filepath, purchase_filepath)
    except Exception as e:
        logger.exception("GSTR-3B error: %s", e)
        error_message = f'Analysis error: {e}'
    finally:
        for fp in [sales_filepath, purchase_filepath]:
            if fp and os.path.exists(fp):
                os.remove(fp)

    return render_template('gstr3b.html', data=data, error_message=error_message,
                           is_logged_in=is_logged_in, user_email=user_email)


# ═══════════════════════════════════════════════════════════
#  GST CALENDAR ROUTE
# ═══════════════════════════════════════════════════════════

@app.route('/gst-calendar')
def gst_calendar():
    is_logged_in, user_email, user_id = _get_current_user()
    from datetime import date
    today = date.today()
    return render_template('gst_calendar.html',
        is_logged_in = is_logged_in,
        user_email   = user_email,
        today        = today.isoformat(),
        current_month= today.month,
        current_year = today.year,
    )


# ═══════════════════════════════════════════════════════════
#  GSTR-1 ROUTE
# ═══════════════════════════════════════════════════════════

@app.route('/gstr1', methods=['GET'])
def gstr1_page():
    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return redirect('/login')
    return render_template('gstr1.html', data=None, is_logged_in=is_logged_in, user_email=user_email)


@app.route('/gstr1', methods=['POST'])
def gstr1_analyze():
    ip = request.remote_addr
    if is_rate_limited(ip):
        abort(429)

    is_logged_in, user_email, user_id = _get_current_user()
    if not is_logged_in:
        return redirect('/login')

    error_message = ''
    data          = None
    transactions  = []

    # ── Bank PDF ──
    bank_pdf = request.files.get('bank_pdf')
    if bank_pdf and bank_pdf.filename:
        safe_name = secure_filename(bank_pdf.filename)
        if not safe_name.lower().endswith('.pdf'):
            error_message = 'Bank file must be a PDF.'
        elif not _is_valid_pdf(bank_pdf):
            error_message = 'Invalid bank PDF.'
        else:
            # Check cache first
            fhash  = _file_hash(bank_pdf)
            cached = _cache_get(fhash)
            if cached:
                transactions = cached['transactions']
            else:
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_name)
                bank_pdf.save(filepath)
                try:
                    transactions = parse_transactions(filepath)
                    _cache_set(fhash, transactions, safe_name)
                except Exception as e:
                    error_message = f'Bank PDF parse error: {e}'
                finally:
                    if os.path.exists(filepath):
                        os.remove(filepath)
    else:
        # Try session cache
        cached_hash = session.get('file_hash')
        if cached_hash:
            cached = _cache_get(cached_hash)
            if cached:
                transactions = cached['transactions']

    if error_message:
        return render_template('gstr1.html', data=None, error_message=error_message,
                               is_logged_in=is_logged_in, user_email=user_email)

    # ── Invoice Excel ──
    invoice_file = request.files.get('invoice_excel')
    if not invoice_file or not invoice_file.filename:
        error_message = 'Invoice Excel file required.'
        return render_template('gstr1.html', data=None, error_message=error_message,
                               is_logged_in=is_logged_in, user_email=user_email)

    safe_inv = secure_filename(invoice_file.filename)
    if not safe_inv.lower().endswith(('.xlsx', '.xls')):
        error_message = 'Invoice file must be .xlsx or .xls'
        return render_template('gstr1.html', data=None, error_message=error_message,
                               is_logged_in=is_logged_in, user_email=user_email)

    inv_filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_inv)
    invoice_file.save(inv_filepath)

    try:
        data = run_gstr1(inv_filepath, transactions)
    except Exception as e:
        logger.exception("GSTR-1 analysis error: %s", e)
        error_message = f'Analysis error: {e}'
    finally:
        if os.path.exists(inv_filepath):
            os.remove(inv_filepath)

    return render_template('gstr1.html', data=data, error_message=error_message,
                           is_logged_in=is_logged_in, user_email=user_email)


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

    data         = request.get_json()
    firebase_uid = data.get('firebase_uid', '')
    email        = data.get('email', '')
    plan_name    = data.get('plan', 'Basic')

    if plan_name not in PLAN_CONFIG:
        plan_name = 'Basic'

    if not firebase_uid:
        return jsonify({'error': 'No user ID'}), 400

    plan   = PLAN_CONFIG[plan_name]
    amount = plan['price'] * 100  # paise

    try:
        order = rzp_client.order.create({
            'amount':          amount,
            'currency':        'INR',
            'payment_capture': 1,
        })

        supabase.table('payments').insert({
            'amount':            plan['price'],
            'status':            'pending',
            'razorpay_order_id': order['id'],
            'plan':              plan_name,
        }).execute()

        logger.info("Mobile order created: %s plan=%s for %s", order['id'], plan_name, email)
        return jsonify({
            'order_id': order['id'],
            'amount':   amount,
            'currency': 'INR',
            'key_id':   RAZORPAY_KEY_ID,
            'email':    email,
        })
    except Exception as e:
        logger.exception("Mobile order creation failed: %s", e)
        return jsonify({'error': 'Order creation failed'}), 500


@app.route('/api/payment/verify', methods=['POST'])
def api_verify_payment():
    ip = request.remote_addr
    if is_rate_limited(ip):
        return jsonify({'error': 'Too many requests'}), 429

    data                = request.get_json()
    firebase_uid        = data.get('firebase_uid', '')
    razorpay_order_id   = data.get('razorpay_order_id', '')
    razorpay_payment_id = data.get('razorpay_payment_id', '')
    razorpay_signature  = data.get('razorpay_signature', '')

    if not firebase_uid:
        return jsonify({'error': 'No user ID'}), 400

    try:
        msg      = f"{razorpay_order_id}|{razorpay_payment_id}"
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
        plan_name = data.get('plan', 'Basic')
        if plan_name not in PLAN_CONFIG:
            plan_name = 'Basic'
        plan = PLAN_CONFIG[plan_name]

        from datetime import datetime, timezone, timedelta
        expires_at = datetime.now(timezone.utc) + timedelta(hours=plan['validity_hours'])

        result = supabase.table('payments').update({
            'status':              'paid',
            'razorpay_payment_id': razorpay_payment_id,
            'plan':                plan_name,
        }).eq('razorpay_order_id', razorpay_order_id).execute()

        payment_id = result.data[0]['id'] if result.data else None

        supabase.table('chat_sessions').insert({
            'firebase_uid':        firebase_uid,
            'payment_id':          payment_id,
            'is_active':           True,
            'db_ready':            False,
            'plan':                plan_name,
            'input_tokens_limit':  plan['input_tokens'],
            'output_tokens_limit': plan['output_tokens'],
            'input_tokens_used':   0,
            'output_tokens_used':  0,
            'expires_at':          expires_at.isoformat(),
        }).execute()

        logger.info("Mobile payment verified: plan=%s for %s", plan_name, firebase_uid)
        return jsonify({'success': True})

    except Exception as e:
        logger.exception("Mobile payment verify DB error: %s", e)
        return jsonify({'error': 'Session creation failed'}), 500


@app.route('/api/payment/status', methods=['POST'])
def api_payment_status():
    data         = request.get_json()
    firebase_uid = data.get('firebase_uid', '')

    if not firebase_uid:
        return jsonify({'has_access': False}), 200

    try:
        from datetime import datetime, timezone
        result = supabase.table('chat_sessions').select('*').eq(
            'firebase_uid', firebase_uid
        ).eq('is_active', True).order(
            'created_at', desc=True
        ).limit(1).execute()

        if result.data:
            s            = result.data[0]
            input_used   = s.get('input_tokens_used', 0)
            output_used  = s.get('output_tokens_used', 0)
            input_limit  = s.get('input_tokens_limit', 25000)
            output_limit = s.get('output_tokens_limit', 5000)
            plan_name    = s.get('plan', 'Basic')
            expires_at   = s.get('expires_at')

            if expires_at:
                exp = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                if exp < datetime.now(timezone.utc):
                    return jsonify({'has_access': False, 'plan': 'Free'})

            if input_used < input_limit and output_used < output_limit:
                saved_txns        = s.get('transactions') or []
                actual_session_id = str(s['id'])
                if saved_txns and not get_db(actual_session_id):
                    try:
                        build_index(actual_session_id, saved_txns)
                        logger.info("SQLite rebuilt for session %s", actual_session_id)
                    except Exception as e:
                        logger.exception("SQLite rebuild failed: %s", e)

                return jsonify({
                    'has_access':          True,
                    'plan':                plan_name,
                    'session_id':          actual_session_id,
                    'input_tokens_used':   input_used,
                    'output_tokens_used':  output_used,
                    'input_tokens_limit':  input_limit,
                    'output_tokens_limit': output_limit,
                    'messages':            s.get('messages') or [],
                    'pdf_loaded':          len(saved_txns) > 0,
                })

        return jsonify({'has_access': False, 'plan': 'Free'})

    except Exception as e:
        logger.exception("Payment status error: %s", e)
        return jsonify({'has_access': False}), 200


# ═══════════════════════════════════════════════════════════
#  MOBILE CHAT API
# ═══════════════════════════════════════════════════════════

@app.route('/api/chat/upload', methods=['POST'])
def api_chat_upload():
    ip = request.remote_addr
    if is_rate_limited(ip):
        return jsonify({'error': 'Too many requests'}), 429

    file       = request.files.get('pdf_file')
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

    row_count = build_index(session_id, transactions)

    try:
        supabase.table('chat_sessions').update({
            'transactions': transactions,
            'db_ready':     True,
        }).eq('id', session_id).execute()
    except Exception as e:
        logger.exception("Failed to save transactions: %s", e)

    return jsonify({
        'success':           True,
        'session_id':        session_id,
        'transaction_count': row_count,
    }), 200


@app.route('/api/chat/message', methods=['POST'])
def api_chat_message():
    ip = request.remote_addr
    if is_rate_limited(ip):
        return jsonify({'error': 'Too many requests'}), 429

    data         = request.get_json()
    session_id   = data.get('session_id', '')
    user_message = (data.get('message') or '').strip()[:500]
    history      = data.get('history') or []

    if not session_id:
        return jsonify({'error': 'No session_id'}), 400
    if not user_message:
        return jsonify({'error': 'Empty message'}), 400

    if not get_db(session_id):
        return jsonify({'error': 'No statement loaded. Upload PDF first.'}), 400

    try:
        session_data = supabase.table('chat_sessions').select(
            'input_tokens_used, input_tokens_limit, output_tokens_used, output_tokens_limit, expires_at'
        ).eq('id', session_id).limit(1).execute()

        if session_data.data:
            s            = session_data.data[0]
            input_used   = s.get('input_tokens_used', 0)
            input_limit  = s.get('input_tokens_limit', 25000)
            output_used  = s.get('output_tokens_used', 0)
            output_limit = s.get('output_tokens_limit', 5000)

            from datetime import datetime, timezone
            expires_at = s.get('expires_at')
            if expires_at:
                exp = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                if exp < datetime.now(timezone.utc):
                    return jsonify({'error': 'Session expired. Please purchase a new plan.'}), 403

            if input_used >= input_limit:
                return jsonify({'error': 'Token limit reached. Please purchase a new plan.'}), 403
            if output_used >= output_limit:
                return jsonify({'error': 'Token limit reached. Please purchase a new plan.'}), 403
    except Exception as e:
        logger.exception("Token limit check failed: %s", e)

    result      = ai_chat(session_id, user_message, history)
    tokens_used = result.get('tokens_used', 0)

    try:
        existing = supabase.table('chat_sessions').select(
            'input_tokens_used, output_tokens_used, messages'
        ).eq('id', session_id).limit(1).execute()

        if existing.data:
            s                 = existing.data[0]
            existing_messages = s.get('messages') or []
            existing_messages.append({'role': 'user',      'content': user_message})
            existing_messages.append({'role': 'assistant', 'content': result['reply']})
            existing_messages = existing_messages[-100:]

            supabase.table('chat_sessions').update({
                'input_tokens_used':  s.get('input_tokens_used', 0) + tokens_used,
                'output_tokens_used': s.get('output_tokens_used', 0),
                'messages':           existing_messages,
            }).eq('id', session_id).execute()
    except Exception as e:
        logger.exception("Token/message update failed: %s", e)

    return jsonify({
        'reply':       result['reply'],
        'tokens_used': tokens_used,
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
