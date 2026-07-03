"""
AarogyamFin — Tier-3 LLM Statement Parser (Claude Haiku fallback)
=================================================================
Use when deterministic (Tier-1) and universal pattern (Tier-2) parsers
fail or return low confidence. Extracts transactions via Claude with a
forced JSON tool schema, then PROVES correctness with balance-continuity
validation (opening + credits - debits must equal printed running balance).

pip install anthropic
env: ANTHROPIC_API_KEY
"""

import json
import time
import random
import logging
from anthropic import Anthropic, APIStatusError, APIConnectionError

MODEL_FAST = "claude-haiku-4-5-20251001"   # cheap workhorse
MODEL_ESCALATE = "claude-sonnet-5"         # retry model when a page fails validation
TOLERANCE = 0.01                           # rupee tolerance for float comparison
MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 529}

logger = logging.getLogger("aarogyamfin.llm_parser")

client = Anthropic()


def _call_with_retry(**kwargs):
    """
    Wraps client.messages.create with exponential backoff + jitter.
    Retries on rate limits (429) and transient server/overload errors
    (500/502/503/529). Anything else (400 bad request, 401 auth, etc.)
    fails fast since retrying won't help.
    """
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            return client.messages.create(**kwargs)
        except APIStatusError as e:
            last_err = e
            status = getattr(e, "status_code", None)
            if status not in RETRYABLE_STATUS_CODES or attempt == MAX_RETRIES - 1:
                raise
            wait = (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"Anthropic API {status} on attempt {attempt + 1}/{MAX_RETRIES}, retrying in {wait:.1f}s")
            time.sleep(wait)
        except APIConnectionError as e:
            last_err = e
            if attempt == MAX_RETRIES - 1:
                raise
            wait = (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"Anthropic connection error on attempt {attempt + 1}/{MAX_RETRIES}, retrying in {wait:.1f}s")
            time.sleep(wait)
    raise last_err

# ---------------------------------------------------------------------------
# Forced tool schema — Claude MUST return this structure, no free text
# ---------------------------------------------------------------------------
EXTRACT_TOOL = {
    "name": "record_transactions",
    "description": "Record bank statement transactions exactly as printed.",
    "input_schema": {
        "type": "object",
        "properties": {
            "transactions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "date":        {"type": "string", "description": "Exactly as printed, e.g. 03/04/2026 or 03-Apr-26"},
                        "description": {"type": "string", "description": "Full narration, multi-line joined with single spaces"},
                        "ref_no":      {"type": ["string", "null"], "description": "Cheque/UPI/ref number if a separate column exists"},
                        "debit":       {"type": ["string", "null"], "description": "Withdrawal amount. Digits + decimal only (123456.78). Remove ALL commas incl. Indian format 1,23,456.78. null if blank"},
                        "credit":      {"type": ["string", "null"], "description": "Deposit amount, same rules. null if blank"},
                        "balance":     {"type": ["string", "null"], "description": "Printed running balance, commas removed, Dr/Cr suffix stripped. null if not printed on this row"},
                        "balance_type":{"type": ["string", "null"], "enum": ["Dr", "Cr", None], "description": "If balance had Dr/Cr suffix. Dr = overdraft (negative)"},
                    },
                    "required": ["date", "description", "debit", "credit", "balance"],
                },
            },
        },
        "required": ["transactions"],
    },
}

SYSTEM_PROMPT = """You are a precise data extractor for Indian bank statements.

STRICT RULES:
1. Extract ONLY what is printed. NEVER calculate, infer, or fill in missing values.
2. Amounts: digits and one decimal point only. Convert Indian comma format 1,23,456.78 -> 123456.78.
3. If the statement has ONE amount column with a Dr/Cr or Withdrawal/Deposit flag:
   Dr/DEBIT/W -> debit field, Cr/CREDIT/D -> credit field.
4. Multi-line narrations (long UPI strings) belong to ONE transaction — join them.
5. SKIP: page headers/footers, column headers, page numbers, "B/F", "C/F",
   "Brought Forward", "Carried Forward", summary/total rows, blank rows.
6. A row is a transaction only if it has a date AND at least one amount.
7. Keep dates exactly as printed. Do not reformat.
8. If balance shows a Cr/Dr suffix (e.g. 1,234.56Cr), strip it into balance_type.
Use the record_transactions tool. Output nothing else."""


def _extract_page(model: str, page_text: str, prev_balance: str | None) -> list[dict]:
    """One Claude call for one page of statement text."""
    hint = (
        f"\n\nContext: the previous page ended at running balance {prev_balance}. "
        "The first transaction on this page should chain from that value. "
        "Still extract only what is printed."
        if prev_balance else ""
    )
    try:
        resp = _call_with_retry(
            model=model,
            max_tokens=8000,
            system=[{  # cache_control -> system prompt billed at 10% on repeat calls
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }],
            tools=[EXTRACT_TOOL],
            tool_choice={"type": "tool", "name": "record_transactions"},
            messages=[{"role": "user", "content": f"Extract all transactions from this statement page:{hint}\n\n{page_text}"}],
        )
    except (APIStatusError, APIConnectionError) as e:
        logger.error(f"Tier-3 extraction failed after retries (model={model}): {e}")
        return []  # caller treats this as a page with 0 txns -> gets flagged by continuity check

    for block in resp.content:
        if block.type == "tool_use":
            return block.input.get("transactions", [])
    return []


def _signed(bal: str | float, bal_type: str | None) -> float:
    """Dr balance = overdraft = negative."""
    v = float(bal)
    return -v if bal_type == "Dr" else v


def validate_continuity(txns: list[dict], opening_balance: float) -> list[dict]:
    """
    Reconstruct running balance and compare with printed balance on every row.
    Returns list of mismatches (empty list == mathematically verified extraction).
    Re-anchors after each mismatch so one bad row doesn't cascade.
    """
    bal = float(opening_balance)
    mismatches = []
    for i, t in enumerate(txns):
        bal += float(t.get("credit") or 0)
        bal -= float(t.get("debit") or 0)
        printed = t.get("balance")
        if printed is None:
            continue
        printed_val = _signed(printed, t.get("balance_type"))
        if abs(bal - printed_val) > TOLERANCE:
            mismatches.append({
                "row": i, "date": t.get("date"), "desc": (t.get("description") or "")[:60],
                "computed": round(bal, 2), "printed": printed_val,
            })
            bal = printed_val  # re-anchor
    return mismatches


def parse_with_llm(pages: list[str], opening_balance: float) -> dict:
    """
    Main entry point for Tier-3.
      pages: list of per-page text (from PyMuPDF/pdfplumber, or OCR output)
      opening_balance: from statement header (parse deterministically upstream)

    Flow per page: Haiku -> validate chunk -> on failure retry with Sonnet
    -> still failing => flag page for manual review (never silently pass bad data).
    """
    all_txns: list[dict] = []
    flagged_pages: list[int] = []
    prev_bal_str: str | None = str(opening_balance)
    chain_bal = float(opening_balance)

    for page_no, page_text in enumerate(pages, start=1):
        if not page_text.strip():
            continue

        txns = _extract_page(MODEL_FAST, page_text, prev_bal_str)
        extraction_failed = not txns
        issues = validate_continuity(txns, chain_bal)

        if issues or extraction_failed:  # escalate this page only
            txns_retry = _extract_page(MODEL_ESCALATE, page_text, prev_bal_str)
            issues_retry = validate_continuity(txns_retry, chain_bal)
            # prefer retry if it actually got data and isn't worse
            if txns_retry and (not txns or len(issues_retry) < len(issues)):
                txns, issues, extraction_failed = txns_retry, issues_retry, not txns_retry
            if issues or extraction_failed:
                flagged_pages.append(page_no)

        all_txns.extend(txns)
        # advance the chain to this page's last printed balance
        printed = [(t["balance"], t.get("balance_type")) for t in txns if t.get("balance")]
        if printed:
            chain_bal = _signed(*printed[-1])
            prev_bal_str = printed[-1][0]

    final_issues = validate_continuity(all_txns, float(opening_balance))
    return {
        "transactions": all_txns,
        "validated": not final_issues,
        "mismatches": final_issues,        # feed into your Reconciliation Score
        "flagged_pages": flagged_pages,    # surface in dashboard: "needs review"
        "parser_tier": 3,
    }


# ---------------------------------------------------------------------------
# BONUS: format-spec learning — run ONCE after a successful Tier-3 parse of a
# new bank, save the spec, and let your Tier-2 universal parser consume it.
# Next statement from this bank parses deterministically, for free.
# ---------------------------------------------------------------------------
def learn_format_spec(sample_page_text: str, verified_txns: list[dict]) -> dict | None:
    """
    Non-critical path: if this fails for any reason (API error, model added
    prose, malformed JSON), we log and return None. Caller should just skip
    saving a spec and fall back to Tier-3 again next time for this bank —
    never let this crash the main parse flow.
    """
    try:
        resp = _call_with_retry(
            model=MODEL_FAST,
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": (
                    "Given this raw bank statement page and its correctly extracted "
                    "transactions, output ONLY a JSON format spec with keys: "
                    "bank_name_pattern (regex to fingerprint this bank), column_order "
                    "(list), date_format (strftime), amount_style ('separate_dr_cr' | "
                    "'single_amount_with_flag'), balance_suffix (true/false), "
                    "header_row_pattern (regex), skip_row_patterns (list of regex). "
                    "No markdown, no prose.\n\nRAW PAGE:\n" + sample_page_text[:4000] +
                    "\n\nVERIFIED TRANSACTIONS (first 5):\n" +
                    json.dumps(verified_txns[:5], ensure_ascii=False)
                ),
            }],
        )
    except (APIStatusError, APIConnectionError) as e:
        logger.error(f"learn_format_spec: API call failed, skipping spec learning: {e}")
        return None

    text = "".join(b.text for b in resp.content if b.type == "text")
    cleaned = text.replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error(f"learn_format_spec: model returned non-JSON, skipping spec learning: {e}\nraw: {cleaned[:200]}")
        return None
    