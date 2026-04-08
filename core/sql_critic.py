# core/sql_critic.py
import os
import re
import time
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

_client = OpenAI(
    api_key=os.environ.get("CEREBRAS_API_KEY", ""),
    base_url="https://api.groq.com/openai/v1",
)


def _call_with_retry(fn, max_retries=3, base_wait=5):
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower() or "queue" in str(e).lower():
                if attempt < max_retries - 1:
                    time.sleep(base_wait * (2 ** attempt))
                    continue
            raise
    raise RuntimeError("Max retries exceeded")

_ENFORCED_RULES = """
ENFORCED JOIN RULES (violations = reject and fix):
1. Never join sellers directly from orders — always via order_items
2. Never join warehouses directly from orders — always via shipments
3. Never join refunds without going through returns first
4. Never join settlements without going through order_items
5. Never use SELECT *
6. Always use explicit JOIN ... ON ... syntax
7. ALWAYS use LEFT JOIN for returns — NEVER INNER JOIN/JOIN for returns
   Correct: LEFT JOIN returns ret ON ret.order_item_id = oi.order_item_id
   Wrong:   JOIN returns ret ON ret.order_item_id = oi.order_item_id
   Reason: INNER JOIN makes return_rate always 100% which is mathematically wrong
8. For revenue lost from returns: use ret.refund_amount from returns table ONLY
   NEVER join the refunds table for return revenue
9. Payment mode values are ONLY 'COD' and 'Prepaid' — never 'Online', 'UPI', 'Card'
10. Always show seller_name not seller_id

VALID JOIN CONDITIONS ONLY:
- orders -> order_items   : o.order_id = oi.order_id
- order_items -> products : oi.product_id = p.product_id
- order_items -> sellers  : oi.seller_id = s.seller_id
- orders -> customers     : o.customer_id = c.customer_id
- orders -> payments      : o.order_id = pay.order_id
- orders -> shipments     : o.order_id = sh.order_id
- returns -> order_items  : ret.order_item_id = oi.order_item_id (LEFT JOIN only)
"""

_PROMPT = """\
You are a strict SQL auditor. Review the SQL below against the enforced rules.

{rules}

SQL TO REVIEW:
{sql}

If the SQL violates any rule, rewrite ONLY the violating part and return the corrected SQL.
If the SQL is correct, return it unchanged.

Respond ONLY in this exact format:
APPROVED: YES | NO
REASON: <one sentence>
SQL:
```sql
<corrected or original sql>
```
"""


def critique(sql: str) -> dict:
    if not sql or not sql.strip():
        return {"approved": False, "fixed_sql": None, "reason": "Empty SQL"}

    prompt = _PROMPT.format(rules=_ENFORCED_RULES, sql=sql)
    for model in ("llama-3.3-70b-versatile", "llama-3.1-8b-instant", "gemma2-9b-it"):
        try:
            resp = _call_with_retry(lambda m=model: _client.chat.completions.create(
                model=m,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=1024,
            ))
            break
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower():
                continue
            return {"approved": True, "fixed_sql": sql, "reason": f"Critic skipped: {e}"}
    else:
        return {"approved": True, "fixed_sql": sql, "reason": "Critic skipped: all models rate-limited"}
    raw = resp.choices[0].message.content

    approved = "APPROVED: YES" in raw.upper()

    reason_match = re.search(r"REASON:\s*(.+?)(?:\n|$)", raw)
    reason = reason_match.group(1).strip() if reason_match else ""

    sql_match = re.search(r"```sql\s*(.*?)```", raw, re.DOTALL)
    fixed_sql = sql_match.group(1).strip() if sql_match else sql

    return {"approved": approved, "fixed_sql": fixed_sql, "reason": reason}
