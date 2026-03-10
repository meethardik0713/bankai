"""
core/chat_engine.py
────────────────────
Haiku 3.5 powered chat engine.
IMPORTANT: Raw transactions / PDF never passed here.
Haiku only sees:
  - DB summary stats
  - SQL query results (max 50 rows)
  - Conversation history
"""

import os
import json
import anthropic
from core.sqlite_indexer import run_query, get_summary

client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY'))

MODEL = 'claude-haiku-4-5-20251001'
MAX_TOKENS = 1024

SYSTEM_PROMPT = """You are AarogyamFin's AI financial assistant. You help users understand their Indian bank statement data.

CRITICAL RULES:
1. You NEVER see raw transaction data or PDFs. You ONLY see SQL query results.
2. Always write a SQL query first to fetch relevant data, then answer based on results.
3. SQL table schema:
   transactions(id, date TEXT, desc TEXT, type TEXT, amount REAL, balance REAL)
   - type is 'CR' for credit, 'DR' for debit
   - amounts are always positive numbers
4. Respond in the same language the user writes in (Hindi, English, or Hinglish).
5. Be concise and helpful. Format numbers with ₹ symbol.
6. If user asks something not related to their statement, politely redirect.

RESPONSE FORMAT (always):
First output your SQL query in this exact format:
<sql>SELECT ... FROM transactions WHERE ... LIMIT 20</sql>

Then give your answer based on the results.
If no SQL needed (e.g. greeting), skip the sql tag."""


def verify_data(session_id: str) -> dict:
    """
    One-time verification call per session.
    Haiku checks for anomalies using summary stats only.
    Returns verification report.
    """
    summary = get_summary(session_id)
    if not summary:
        return {'verified': False, 'message': 'No data found'}

    # Check for obvious issues via SQL
    issues = []

    # Check for missing types
    missing_type = run_query(session_id,
        "SELECT COUNT(*) as cnt FROM transactions WHERE type IS NULL OR type = ''")
    if missing_type and missing_type[0].get('cnt', 0) > 0:
        issues.append(f"{missing_type[0]['cnt']} transactions have missing type (CR/DR)")

    # Check for zero amounts
    zero_amt = run_query(session_id,
        "SELECT COUNT(*) as cnt FROM transactions WHERE amount = 0 OR amount IS NULL")
    if zero_amt and zero_amt[0].get('cnt', 0) > 0:
        issues.append(f"{zero_amt[0]['cnt']} transactions have zero/missing amount")

    # Check balance continuity (sample check)
    neg_bal = run_query(session_id,
        "SELECT COUNT(*) as cnt FROM transactions WHERE balance < 0")
    if neg_bal and neg_bal[0].get('cnt', 0) > 0:
        issues.append(f"{neg_bal[0]['cnt']} transactions show negative balance")

    # Ask Haiku to verify with summary only
    prompt = f"""Bank statement summary stats:
- Total transactions: {summary.get('total', 0)}
- Total Credits: ₹{summary.get('total_cr', 0):,.2f}
- Total Debits: ₹{summary.get('total_dr', 0):,.2f}  
- Date range: {summary.get('from_date')} to {summary.get('to_date')}
- Balance range: ₹{summary.get('min_bal', 0):,.2f} to ₹{summary.get('max_bal', 0):,.2f}
- Issues detected: {issues if issues else 'None'}

Give a brief 2-line verification status. Is this data reliable for analysis?"""

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=200,
            messages=[{'role': 'user', 'content': prompt}]
        )
        haiku_verdict = response.content[0].text
    except Exception as e:
        haiku_verdict = f"Verification error: {e}"

    return {
        'verified': len(issues) == 0,
        'issues': issues,
        'summary': summary,
        'verdict': haiku_verdict
    }


def chat(session_id: str, user_message: str, history: list) -> dict:
    """
    Main chat function.
    1. Haiku writes SQL if needed
    2. SQL runs on local SQLite
    3. Haiku formats answer from results
    Returns: {'reply': str, 'sql_used': str or None}
    """
    summary = get_summary(session_id)

    # Build context message
    context = f"""Statement context:
- {summary.get('total', 0)} transactions indexed
- Date range: {summary.get('from_date')} to {summary.get('to_date')}
- Total Credits: ₹{summary.get('total_cr', 0):,.2f}
- Total Debits: ₹{summary.get('total_dr', 0):,.2f}"""

    # Build messages with history
    messages = []

    # Add conversation history (last 6 turns only — cost control)
    for turn in history[-6:]:
        messages.append({'role': turn['role'], 'content': turn['content']})

    # Add current user message with context
    messages.append({
        'role': 'user',
        'content': f"{context}\n\nUser question: {user_message}"
    })

    try:
        # First call — get SQL from Haiku
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=messages
        )

        reply_text = response.content[0].text
        sql_used = None
        final_reply = reply_text

        # Extract and execute SQL if present
        if '<sql>' in reply_text and '</sql>' in reply_text:
            sql_start = reply_text.index('<sql>') + 5
            sql_end = reply_text.index('</sql>')
            sql_query = reply_text[sql_start:sql_end].strip()
            sql_used = sql_query

            # Run SQL on local SQLite
            results = run_query(session_id, sql_query)

            # Second call — format answer with real results
            result_str = json.dumps(results[:30], indent=2) if results else "No results found"

            followup_messages = messages + [
                {'role': 'assistant', 'content': reply_text},
                {'role': 'user', 'content': f"SQL results:\n{result_str}\n\nNow give your final answer based on these results. Do not show the SQL or results to user — just answer naturally."}
            ]

            final_response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=SYSTEM_PROMPT,
                messages=followup_messages
            )
            final_reply = final_response.content[0].text

        return {
            'reply': final_reply,
            'sql_used': sql_used,
            'tokens_used': response.usage.input_tokens + response.usage.output_tokens
        }

    except Exception as e:
        return {
            'reply': f"Sorry, kuch error aaya: {str(e)}",
            'sql_used': None,
            'tokens_used': 0
        }
    