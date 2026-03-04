import re
import pdfplumber

pdf_path = r"C:\Users\Admin\Downloads\544516929-ICICI-BANK-STATEMENT.pdf"

_DATE_RE   = re.compile(r'^\d{2}-\d{2}-\d{4}$')
_AMOUNT_RE = re.compile(r'^[\d,]+\.\d{2}$')
_BF_RE     = re.compile(r'\bB/?F\b', re.IGNORECASE)
_SKIP_RE   = re.compile(
    r'(Page \d+ of \d+|Visit www\.|Dial your Bank|Did you know|'
    r'KYC compliant|Relationship Manager|Summary of Accounts|'
    r'ACCOUNT DETAILS|ACCOUNT TYPE|A/c BALANCE|FIXED DEPOSITS|'
    r'TOTAL BALANCE|NOMINATION|Savings A/c|Statement of Trans|'
    r'DATE\s+MODE|PARTICULARS\s+DEPOSITS|Legends for|VAT/MAT|'
    r'RTGS|Mode is available|Income tax|REGD ADDRESS|'
    r'Authorised Signatory|authenticated|customers are|'
    r'For ICICI Bank|khayaal aapka|^\s*$|'
    r'MHW\d|Your Base Branch|PANJAGUTTA|TELANGANA)',
    re.IGNORECASE
)

# Extract all lines from page 1 only
with pdfplumber.open(pdf_path) as pdf:
    text = pdf.pages[0].extract_text() or ''

lines = [l.strip() for l in text.split('\n') if l.strip()]

opening_balance = None
print("=== TRACING FIRST 5 DATE LINES ===\n")
date_count = 0

for i, line in enumerate(lines):
    parts = line.split()
    if not parts:
        continue

    # Check skip
    if _SKIP_RE.search(line):
        continue

    if not _DATE_RE.match(parts[0]):
        continue

    date_str = parts[0]
    rest = parts[1:]

    # B/F check
    is_bf = any(_BF_RE.match(t) for t in rest)

    amount_positions = [j for j, t in enumerate(rest) if _AMOUNT_RE.match(t)]

    print(f"Line #{i}: '{line[:80]}'")
    print(f"  date_str      = {date_str}")
    print(f"  rest          = {rest}")
    print(f"  is_bf         = {is_bf}")
    print(f"  amounts found = {[rest[j] for j in amount_positions]}")

    if is_bf:
        from core.utils import parse_amt
        import sys
        sys.path.insert(0, r'C:\Users\Admin\Desktop\BankAI')
        opening_balance = float(rest[-1].replace(',', ''))
        print(f"  ✅ OPENING BALANCE SET = {opening_balance}")
    else:
        if amount_positions:
            bal = float(rest[amount_positions[-1]].replace(',', ''))
            prev = opening_balance if opening_balance else 0.0
            diff = round(bal - prev, 2)
            typ = 'CR' if diff >= 0 else 'DR'
            print(f"  opening_balance at this point = {opening_balance}")
            print(f"  prev_bal used = {prev}")
            print(f"  balance = {bal}, diff = {diff}")
            print(f"  → TYPE = {typ}")

    print()
    date_count += 1
    if date_count >= 5:
        break
    