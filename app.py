import fitz
import os
import re
import openpyxl
import time
from flask import Flask, request, render_template, send_file, abort
from io import BytesIO
from collections import defaultdict

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

request_counts = defaultdict(list)
RATE_LIMIT = 10
RATE_WINDOW = 60

def is_rate_limited(ip):
    now = time.time()
    request_counts[ip] = [t for t in request_counts[ip] if now - t < RATE_WINDOW]
    if len(request_counts[ip]) >= RATE_LIMIT:
        return True
    request_counts[ip].append(now)
    return False

def extract_lines(pdf_path):
    doc = fitz.open(pdf_path)
    lines = []
    for page in doc:
        for line in page.get_text().split('\n'):
            if line.strip():
                lines.append(line.strip())
    return lines

def is_amount_str(text):
    return bool(re.match(r'^[\d,]+\.\d{2}$', str(text).strip()))

def clean_amount(text):
    if not text:
        return None
    text = str(text).replace(',', '').replace('₹', '').replace(' ', '').strip()
    if '.' not in text:
        return None
    try:
        val = float(text)
        return val if val >= 0 else None
    except:
        return None

def detect_cr_dr_universal(transactions):
    TOLERANCE = 0.50
    for i, txn in enumerate(transactions):
        prev_bal = transactions[i-1].get('balance') if i > 0 else None
        curr_bal = txn.get('balance')
        if prev_bal is None or curr_bal is None:
            if not txn.get('type'):
                txn['type'] = 'DR'
            continue
        diff = curr_bal - prev_bal
        if diff > TOLERANCE:
            txn['type'] = 'CR'
        elif diff < -TOLERANCE:
            txn['type'] = 'DR'
        else:
            txn['type'] = 'DR'
    return transactions

def detect_bank(lines):
    full_text = ' '.join(lines).upper()  # puri file check karo
    if 'HDFC' in full_text:
        return 'hdfc'
    if 'STATE BANK OF INDIA' in full_text:
        return 'sbi'
    for line in lines[:100]:
        if re.match(r'^\d{2}-\d{2}-\d{4}$', line.strip()):
            return 'canara'
        if re.match(r'^\d{2}\s+\w{3}\s+\d{4}$', line.strip()):
            return 'kotak'
    return 'generic'

def parse_canara(lines):
    transactions = []
    i = 0
    while i < len(lines):
        if re.match(r'^\d{2}-\d{2}-\d{4}$', lines[i]):
            date = lines[i]
            desc_parts = []
            i += 1
            while i < len(lines) and not lines[i].startswith('Chq:'):
                if re.match(r'^\d{2}-\d{2}-\d{4}$', lines[i]):
                    break
                desc_parts.append(lines[i])
                i += 1
            if i < len(lines) and lines[i].startswith('Chq:'):
                i += 1
            amounts_found = []
            while i < len(lines) and is_amount_str(lines[i]) and len(amounts_found) < 3:
                amounts_found.append(float(lines[i].replace(',', '')))
                i += 1
            desc = ' '.join(desc_parts)
            amount = None
            balance = None
            if len(amounts_found) == 2:
                amount = amounts_found[0]
                balance = amounts_found[1]
            elif len(amounts_found) == 1:
                amount = amounts_found[0]
            transactions.append({'date': date, 'desc': desc, 'amount': amount, 'balance': balance, 'type': ''})
        else:
            i += 1
    return transactions

def parse_kotak(lines):
    transactions = []
    i = 0
    while i < len(lines):
        if re.match(r'^\d{1,4}$', lines[i]) and i+1 < len(lines) and re.match(r'^\d{2}\s+\w{3}\s+\d{4}$', lines[i+1]):
            i += 1
            date = lines[i]
            desc_parts = []
            i += 1
            while i < len(lines) and not is_amount_str(lines[i]):
                if re.match(r'^\d{1,4}$', lines[i]) and i+1 < len(lines) and re.match(r'^\d{2}\s+\w{3}\s+\d{4}$', lines[i+1]):
                    break
                desc_parts.append(lines[i])
                i += 1
            amounts_found = []
            while i < len(lines) and is_amount_str(lines[i]) and len(amounts_found) < 3:
                amounts_found.append(float(lines[i].replace(',', '')))
                i += 1
            desc = ' '.join(desc_parts)
            amount = None
            balance = None
            if len(amounts_found) == 2:
                amount = amounts_found[0]
                balance = amounts_found[1]
            elif len(amounts_found) == 1:
                amount = amounts_found[0]
            transactions.append({'date': date, 'desc': desc, 'amount': amount, 'balance': balance, 'type': ''})
        else:
            i += 1
    return transactions

def parse_hdfc(lines):
    transactions = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if re.match(r'^\d{2}/\d{2}/\d{2}$', line):
            date = line
            i += 1
            desc_parts = []
            while i < len(lines):
                l = lines[i].strip()
                if re.match(r'^[A-Z0-9]{10,}$', l):
                    i += 1
                    continue
                if re.match(r'^\d{2}/\d{2}/\d{2}$', l):
                    i += 1
                    continue
                if is_amount_str(l):
                    break
                desc_parts.append(l)
                i += 1
            amounts_found = []
            while i < len(lines) and is_amount_str(lines[i].strip()) and len(amounts_found) < 3:
                amounts_found.append(float(lines[i].strip().replace(',', '')))
                i += 1
            desc = ' '.join(desc_parts).strip()
            amount = None
            balance = None
            if len(amounts_found) == 2:
                amount = amounts_found[0]
                balance = amounts_found[1]
            elif len(amounts_found) == 1:
                balance = amounts_found[0]
            if desc or amount or balance:
                transactions.append({'date': date, 'desc': desc, 'amount': amount, 'balance': balance, 'type': ''})
        else:
            i += 1
    return transactions

def parse_sbi(lines):
    transactions = []
    i = 0
    found_start = False
    while i < len(lines):
        if 'brought forward' in lines[i].lower():
            i += 1
            found_start = True
            while i < len(lines) and is_amount_str(lines[i].strip()):
                i += 1
            break
        i += 1
    if not found_start:
        i = 0
        while i < len(lines):
            if re.match(r'^\d{2}/\d{2}/\d{2}$', lines[i].strip()):
                break
            i += 1
    while i < len(lines):
        line = lines[i].strip()
        if re.match(r'^\d{2}/\d{2}/\d{2}$', line):
            date = line
            i += 1
            if i < len(lines) and re.match(r'^\d{2}/\d{2}/\d{2}$', lines[i].strip()):
                i += 1
            desc_parts = []
            while i < len(lines):
                l = lines[i].strip()
                if re.match(r'^\d{2}/\d{2}/\d{2}$', l):
                    break
                if is_amount_str(l):
                    break
                if re.match(r'^\d{5,}$', l):
                    i += 1
                    continue
                desc_parts.append(l)
                i += 1
            amounts_found = []
            while i < len(lines) and is_amount_str(lines[i].strip()) and len(amounts_found) < 3:
                amounts_found.append(float(lines[i].strip().replace(',', '')))
                i += 1
            desc = ' '.join(desc_parts).strip()
            amount = None
            balance = None
            if len(amounts_found) == 3:
                debit  = amounts_found[0]
                credit = amounts_found[1]
                balance = amounts_found[2]
                amount = debit if debit > 0 else credit
            elif len(amounts_found) == 2:
                amount = amounts_found[0]
                balance = amounts_found[1]
            elif len(amounts_found) == 1:
                balance = amounts_found[0]
            if desc or amount or balance:
                transactions.append({'date': date, 'desc': desc, 'amount': amount, 'balance': balance, 'type': ''})
        else:
            i += 1
    return transactions

def parse_generic(lines):
    transactions = []
    date_pattern = re.compile(
        r'^\d{2}[-/]\d{2}[-/]\d{2,4}$|^\d{2}\s+\w{3}\s+\d{2,4}$|^\d{4}-\d{2}-\d{2}$'
    )
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if date_pattern.match(line):
            date = line
            desc_parts = []
            i += 1
            while i < len(lines):
                l = lines[i].strip()
                if date_pattern.match(l):
                    break
                if is_amount_str(l):
                    break
                desc_parts.append(l)
                i += 1
            amounts_found = []
            while i < len(lines) and is_amount_str(lines[i].strip()) and len(amounts_found) < 3:
                amounts_found.append(float(lines[i].strip().replace(',', '')))
                i += 1
            desc = ' '.join(desc_parts).strip()
            amount = None
            balance = None
            if len(amounts_found) == 2:
                amount = amounts_found[0]
                balance = amounts_found[1]
            elif len(amounts_found) == 1:
                amount = amounts_found[0]
            if amount or balance:
                transactions.append({'date': date, 'desc': desc, 'amount': amount, 'balance': balance, 'type': ''})
        else:
            i += 1
    return transactions

def parse_transactions(pdf_path):
    lines = extract_lines(pdf_path)
    bank = detect_bank(lines)
    print(f"DETECTED BANK: {bank}")
    if bank == 'canara':
        transactions = parse_canara(lines)
    elif bank == 'kotak':
        transactions = parse_kotak(lines)
    elif bank == 'hdfc':
        transactions = parse_hdfc(lines)
    elif bank == 'sbi':
        transactions = parse_sbi(lines)
    else:
        transactions = parse_generic(lines)
    print(f"TOTAL TRANSACTIONS: {len(transactions)}")
    transactions = detect_cr_dr_universal(transactions)
    return transactions

@app.route('/', methods=['GET', 'POST'])
def home():
    ip = request.remote_addr
    if request.method == 'POST' and is_rate_limited(ip):
        abort(429)
    transaction_data = []
    keyword = ''
    total = 0
    amounts_count = 0
    count = 0
    selected_filename = ''
    if request.method == 'POST' and 'pdf_file' in request.files:
        keyword = request.form.get('keyword', '')[:100]
        file = request.files.get('pdf_file')
        if file and file.filename.endswith('.pdf') and len(file.filename) < 200:
            selected_filename = file.filename
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(filepath)
            try:
                all_transactions = parse_transactions(filepath)
                if keyword:
                    filtered = [t for t in all_transactions if 
    keyword.lower() in t['desc'].lower() or 
    keyword.lower() in t['date'].lower() or
    keyword.replace(',','') in str(t['amount'] or '') or
    keyword.replace(',','') in str(t['balance'] or '')]
                else:
                    filtered = all_transactions
                for t in filtered:
                    transaction_data.append(t)
                    if t['amount']:
                        total += t['amount']
                        amounts_count += 1
                count = len(transaction_data)
            finally:
                if os.path.exists(filepath):
                    os.remove(filepath)
    return render_template('index.html',
                           transaction_data=transaction_data,
                           keyword=keyword,
                           total=total,
                           count=count,
                           amounts_count=amounts_count,
                           selected_filename=selected_filename)

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/privacy')
def privacy():
    return render_template('privacy.html')

@app.route('/terms')
def terms():
    return render_template('terms.html')

@app.route('/export', methods=['POST'])
def export():
    ip = request.remote_addr
    if is_rate_limited(ip):
        abort(429)
    dates = request.form.getlist('dates')
    descs = request.form.getlist('descs')
    amounts = request.form.getlist('amounts')
    balances = request.form.getlist('balances')
    types = request.form.getlist('types')
    keyword = request.form.get('keyword', '')[:100]
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Transactions"
    ws.append(["#", "Date", "Description", "Type", "Amount (₹)", "Balance (₹)"])
    for i, (d, desc, a, b, tp) in enumerate(zip(dates, descs, amounts, balances, types), 1):
        ws.append([i, d, desc, tp, float(a) if a else '', float(b) if b else ''])
    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return send_file(output, download_name=f'transactions_{keyword}.xlsx', as_attachment=True)

@app.errorhandler(429)
def too_many_requests(e):
    return "Too many requests. Please wait a minute and try again.", 429

@app.errorhandler(413)
def file_too_large(e):
    return "File too large. Maximum size is 10MB.", 413

if __name__ == '__main__':
    app.run(debug=True)
