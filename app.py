import fitz
import os
import re
import openpyxl
from flask import Flask, request, render_template, send_file
from io import BytesIO

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def extract_transactions(pdf_path):
    doc = fitz.open(pdf_path)
    lines = []
    for page in doc:
        text = page.get_text()
        for line in text.split('\n'):
            if line.strip():
                lines.append(line.strip())
    return lines

def is_amount(text):
    return bool(re.match(r'^[\d,]+\.\d{2}$', text.strip()))

def is_canara_date(text):
    return bool(re.match(r'^\d{2}-\d{2}-\d{4}$', text.strip()))

def is_kotak_date(text):
    return bool(re.match(r'^\d{2}\s+\w{3}\s+\d{4}$', text.strip()))

def is_row_number(text):
    return bool(re.match(r'^\d{1,4}$', text.strip()))

def detect_type(desc):
    desc_upper = desc.upper()
    if 'UPI/DR/' in desc_upper or 'NEFT/DR/' in desc_upper or 'IMPS/DR/' in desc_upper:
        return 'DR'
    if 'UPI/CR/' in desc_upper or 'NEFT/CR/' in desc_upper or 'IMPS/CR/' in desc_upper:
        return 'CR'
    cr_keywords = ['CREDIT', 'SALARY', 'DEPOSIT', 'REFUND', 'CASHBACK', 'INTEREST', 'INWARD', 'RECEIVED', 'BY CASH', 'BY CLG']
    dr_keywords = ['DEBIT', 'WITHDRAWAL', 'SENT', 'PAYMENT', 'PURCHASE', 'ATM', 'TRANSFER TO']
    for kw in cr_keywords:
        if kw in desc_upper:
            return 'CR'
    for kw in dr_keywords:
        if kw in desc_upper:
            return 'DR'
    if 'UPI/' in desc_upper:
        return 'DR'
    return ''

def detect_bank(lines):
    print("=== FIRST 15 LINES ===")
    for l in lines[:15]:
        print(repr(l))
    print("======================")
    for line in lines[:50]:
        if is_canara_date(line):
            print("BANK: canara")
            return 'canara'
        if is_kotak_date(line):
            print("BANK: kotak")
            return 'kotak'
    print("BANK: generic")
    return 'generic'

def parse_canara(lines):
    transactions = []
    i = 0
    while i < len(lines):
        if is_canara_date(lines[i]):
            date = lines[i]
            desc_parts = []
            i += 1
            while i < len(lines) and not lines[i].startswith('Chq:'):
                if is_canara_date(lines[i]):
                    break
                desc_parts.append(lines[i])
                i += 1
            if i < len(lines) and lines[i].startswith('Chq:'):
                i += 1
            amounts_found = []
            while i < len(lines) and is_amount(lines[i]) and len(amounts_found) < 3:
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
            txn_type = detect_type(desc)
            transactions.append({'date': date, 'desc': desc, 'amount': amount, 'balance': balance, 'type': txn_type})
        else:
            i += 1
    return transactions

def parse_kotak(lines):
    transactions = []
    i = 0
    while i < len(lines):
        if is_row_number(lines[i]) and i + 1 < len(lines) and is_kotak_date(lines[i+1]):
            i += 1
            date = lines[i]
            desc_parts = []
            i += 1
            while i < len(lines) and not is_amount(lines[i]):
                if is_row_number(lines[i]) and i + 1 < len(lines) and is_kotak_date(lines[i+1]):
                    break
                desc_parts.append(lines[i])
                i += 1
            amounts_found = []
            while i < len(lines) and is_amount(lines[i]) and len(amounts_found) < 3:
                amounts_found.append(float(lines[i].replace(',', '')))
                i += 1
            desc = ' '.join(desc_parts)
            print(f"KOTAK | DATE: {date} | AMOUNTS: {amounts_found} | DESC: {desc[:50]}")
            amount = None
            balance = None
            if len(amounts_found) == 2:
                amount = amounts_found[0]
                balance = amounts_found[1]
            elif len(amounts_found) == 1:
                amount = amounts_found[0]
            txn_type = detect_type(desc)
            transactions.append({'date': date, 'desc': desc, 'amount': amount, 'balance': balance, 'type': txn_type})
        else:
            i += 1
    return transactions

def parse_generic(lines):
    transactions = []
    for line in lines:
        amount = None
        matches = re.findall(r'[\d,]+\.\d{2}', line)
        if matches:
            try:
                amount = float(matches[-1].replace(',', ''))
            except:
                pass
        transactions.append({'date': '', 'desc': line, 'amount': amount, 'balance': None, 'type': ''})
    return transactions

def parse_transactions(lines):
    bank = detect_bank(lines)
    if bank == 'canara':
        return parse_canara(lines)
    elif bank == 'kotak':
        return parse_kotak(lines)
    else:
        return parse_generic(lines)

@app.route('/', methods=['GET', 'POST'])
def home():
    transaction_data = []
    keyword = ''
    total = 0
    amounts_count = 0
    count = 0
    selected_filename = ''

    if request.method == 'POST' and 'pdf_file' in request.files:
        keyword = request.form.get('keyword', '')
        file = request.files.get('pdf_file')
        if file and file.filename.endswith('.pdf'):
            selected_filename = file.filename
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(filepath)
            all_lines = extract_transactions(filepath)
            all_transactions = parse_transactions(all_lines)
            if keyword:
                filtered = [t for t in all_transactions if keyword.lower() in t['desc'].lower() or keyword.lower() in t['date'].lower()]
            else:
                filtered = all_transactions
            for t in filtered:
                transaction_data.append(t)
                if t['amount']:
                    total += t['amount']
                    amounts_count += 1
            count = len(transaction_data)

    return render_template('index.html',
                           transaction_data=transaction_data,
                           keyword=keyword,
                           total=total,
                           count=count,
                           amounts_count=amounts_count,
                           selected_filename=selected_filename)

@app.route('/export', methods=['POST'])
def export():
    dates = request.form.getlist('dates')
    descs = request.form.getlist('descs')
    amounts = request.form.getlist('amounts')
    balances = request.form.getlist('balances')
    types = request.form.getlist('types')
    keyword = request.form.get('keyword', '')
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

if __name__ == '__main__':
    app.run(debug=True)