"""
test_parser.py
──────────────────────────────────────────────────────────────
Standalone test script — sirf parser + dashboard.py test karne ke liye.
Poora Flask app / login / payment / Supabase kuch bhi chalane ki
zaroorat NAHI hai. Bas ek PDF do, yeh sab print kar dega.

USAGE:
    python test_parser.py path/to/statement.pdf

Isko 'bankai' repo ke root folder mein rakhna hai (jahan parsers/
aur core/ folder dikhte hain), tabhi imports kaam karenge.
──────────────────────────────────────────────────────────────
"""

import sys
import json

from parsers.detector import detect
from core.dashboard import run_dashboard


def main():
    if len(sys.argv) < 2:
        print("Usage: python test_parser.py path/to/statement.pdf")
        sys.exit(1)

    pdf_path = sys.argv[1]

    # ── Step 1: Detect bank + parse transactions ──────────────
    print(f"\n[1/3] Detecting bank for: {pdf_path}")
    parser = detect(pdf_path)
    print(f"      Matched parser: {parser.__class__.__name__}")

    print(f"\n[2/3] Parsing transactions...")
    transactions = parser.parse(pdf_path)
    print(f"      Total transactions parsed: {len(transactions)}")

    if not transactions:
        print("\n⚠️  Koi transaction nahi mila. Parser check karo.")
        sys.exit(1)

    # Pehli aur akhri 3 transactions dikhado — sanity check ke liye
    print("\n      --- First 3 transactions ---")
    for t in transactions[:3]:
        print(f"      {t}")
    print("      --- Last 3 transactions ---")
    for t in transactions[-3:]:
        print(f"      {t}")

    # ── Step 2: Run dashboard engine on parsed transactions ───
    print(f"\n[3/3] Running dashboard engine...")
    result = run_dashboard(transactions)

    # ── Step 3: Print key accuracy signals ────────────────────
    print("\n===== SUMMARY =====")
    print(f"Total transactions : {result['total_txns']}")
    print(f"Total credits (CR) : Rs{result['total_cr']:,.2f}")
    print(f"Total debits (DR)  : Rs{result['total_dr']:,.2f}")
    print(f"Real income total  : Rs{result['real_income']:,.2f}")

    needs_ver = result.get('income', {}).get('needs_verification_total', 0)
    others_total = result.get('expense', {}).get('category_totals', {}).get('Others', 0)
    print(f"\nNeeds Verification (unclassified credits): Rs{needs_ver:,.2f}")
    print(f"'Others' category (unclassified debits)   : Rs{others_total:,.2f}")
    print("   ↑ Yeh dono numbers jitne kam honge, accuracy utni zyada hai.")
    print("     Inme jo transactions aa rahe hain, unke 'desc' text dekho —")
    print("     wahi keyword lists (core/dashboard.py) mein add karne hain.")

    validation = result.get('_validation', {})
    print(f"\nValidation passed  : {validation.get('passed')}")
    if validation.get('errors'):
        print(f"Validation errors  : {validation['errors']}")

    # Optional: full JSON dump for deep debugging
    dump = input("\nPoora JSON dump file mein save karna hai? (y/n): ").strip().lower()
    if dump == 'y':
        out_path = "dashboard_output.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
