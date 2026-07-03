"""
universal_parser.py
────────────────────
Compatibility wrapper — delegates to the new modular parser system.

app.py continues to import parse_transactions from here.
Internally, all logic now lives in parsers/ and core/.

DO NOT add parsing logic here. Use the appropriate parser in parsers/.
"""

import time
import logging
from parsers import detector
from core.verifier import run_accuracy_check

logger = logging.getLogger("aarogyamfin.universal_parser")

MIN_TRUSTED_TXNS = 3  # below this, try Tier-3 LLM fallback


def parse_transactions(pdf_path: str) -> list:
    start = time.time()
    try:
        parser       = detector.detect(pdf_path)
        transactions = parser.parse(pdf_path)

        elapsed = round(time.time() - start, 2)
        print(f"[pipeline] Final transactions: {len(transactions)} ({elapsed}s)")

        if transactions and transactions[0].get('opening_balance') is not None:
            print(f"[pipeline] Opening balance: ₹{transactions[0]['opening_balance']:,.2f}")

        # OCR fallback for image-based PDFs (BOB, PNB Print-to-PDF style)
        if not transactions:
            print(f"[pipeline] 0 transactions, trying OCR fallback...")
            from parsers.ocr_fallback import ocr_parse
            transactions = ocr_parse(pdf_path)

        # Post-parse: fill missing balances via running calculation
        ob = transactions[0].get('opening_balance') if transactions else None
        _fill_missing_balances(transactions, ob)

        # ── Tier-3: LLM fallback when Tier-1/2 + OCR still returned too few
        if not transactions or len(transactions) < MIN_TRUSTED_TXNS:
            logger.info("Tier-1/2/OCR gave %d txns — trying Tier-3 LLM fallback",
                        len(transactions) if transactions else 0)
            llm_txns = _try_tier3_llm(pdf_path, transactions)
            if llm_txns and len(llm_txns) > len(transactions or []):
                return llm_txns

        return transactions

    except Exception as e:
        import traceback
        print(f"[pipeline] Fatal: {e}")
        traceback.print_exc()
        return []


def _fill_missing_balances(transactions: list, opening_balance: float):
    """Fill in any None balances using running balance calculation."""
    prev_bal = opening_balance or 0.0
    for i, txn in enumerate(transactions):
        if txn.get('balance') is None and txn.get('amount') is not None:
            if txn.get('type') == 'CR':
                txn['balance'] = round(prev_bal + txn['amount'], 2)
            else:
                txn['balance'] = round(prev_bal - txn['amount'], 2)
        if txn.get('balance') is not None:
            prev_bal = txn['balance']


def _try_tier3_llm(pdf_path: str, fallback_txns: list) -> list:
    """Isolated so a Tier-3 failure (API error, no pages, etc.) never
    breaks the existing Tier-1/2/OCR return path."""
    try:
        import re
        import pdfplumber
        from parsers.llm_parser import parse_with_llm

        with pdfplumber.open(pdf_path) as pdf:
            pages = [(p.extract_text() or '') for p in pdf.pages]
        if not pages:
            return []

        opening_balance = 0.0
        m = re.search(
            r'(?:opening balance|b/f|balance forward)[:\s]*(?:rs\.?)?\s*([\d,]+\.?\d*)',
            pages[0], re.IGNORECASE
        )
        if m:
            opening_balance = float(m.group(1).replace(',', ''))
        elif fallback_txns:
            t0  = fallback_txns[0]
            bal = t0.get('balance')
            amt = t0.get('amount', 0) or 0
            if bal is not None:
                opening_balance = float(bal) - amt if t0.get('type') == 'CR' else float(bal) + amt

        result = parse_with_llm(pages, opening_balance)
        if not result['transactions']:
            return []

        logger.info("Tier-3 extracted %d txns (validated=%s, flagged_pages=%s)",
                    len(result['transactions']), result['validated'], result['flagged_pages'])

        normalized = []
        for t in result['transactions']:
            debit  = t.get('debit')
            credit = t.get('credit')
            bal    = t.get('balance')
            normalized.append({
                'date':      t.get('date'),
                'desc':      t.get('description'),
                'amount':    round(float(debit) if debit else (float(credit) if credit else 0), 2),
                'type':      'DR' if debit else 'CR',
                'balance':   round(float(bal), 2) if bal else None,
                'reference': t.get('ref_no') or '',
            })
        return normalized

    except Exception as e:
        logger.error("Tier-3 fallback failed, returning Tier-1/2/OCR result: %s", e)
        return []
            