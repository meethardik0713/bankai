import pdfplumber

pdf_path = "2026-02-18-05-57-42-Account_01_Apr_2025_-_18_Feb_2026_XX7250.pdf"

with pdfplumber.open(pdf_path) as pdf:
    for i, page in enumerate(pdf.pages[:2]):
        print(f"\n{'='*60}")
        print(f"PAGE {i+1} TEXT:")
        print('='*60)
        text = page.extract_text() or ''
        for line in text.splitlines()[:50]:
            print(repr(line))

    for i, page in enumerate(pdf.pages[:2]):
        print(f"\n{'='*60}")
        print(f"PAGE {i+1} TABLES:")
        print('='*60)
        tables = page.extract_tables() or []
        for t_idx, table in enumerate(tables):
            print(f"  Table {t_idx+1}:")
            for row in table[:10]:
                print(f"    {row}")
                