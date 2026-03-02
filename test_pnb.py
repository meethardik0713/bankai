import pdfplumber
import os

PDF_PATH = r"C:\Users\Admin\Downloads\664691018-Sonu-pnb-bank-1 (1).pdf"

print("File exists:", os.path.exists(PDF_PATH))

with pdfplumber.open(PDF_PATH) as pdf:
    print("Total pages:", len(pdf.pages))
    page = pdf.pages[0]

    # Text test
    text = page.extract_text() or ''
    print("Text length:", len(text))
    print("Text preview:", repr(text[:300]))
    print()

    # Table test
    tables = page.extract_tables()
    print("Tables found:", len(tables))
    if tables:
        print("First 5 rows of first table:")
        for row in tables[0][:5]:
            print(row)
            