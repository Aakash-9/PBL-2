# core/sql_generator.py
import os
import re
import time
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# Use OpenAI or Groq based on which key is available
if os.environ.get("OPENAI_API_KEY") and os.environ.get("OPENAI_API_KEY") != "YOUR_OPENAI_KEY_HERE":
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    DEFAULT_MODEL = "gpt-4-turbo-preview"
else:
    # Use Groq (key stored as CEREBRAS_API_KEY)
    client = OpenAI(
        api_key=os.environ.get("CEREBRAS_API_KEY", ""),
        base_url="https://api.groq.com/openai/v1",
    )
    DEFAULT_MODEL = "llama-3.1-8b-instant"


def _call_with_retry(fn, max_retries=3, base_wait=5):
    """Call fn() with exponential backoff on rate limit errors."""
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            err_str = str(e)
            is_rate_limit = "429" in err_str or "rate" in err_str.lower() or "queue" in err_str.lower()
            is_daily_limit = "tokens per day" in err_str.lower() or "tpd" in err_str.lower()

            if is_daily_limit:
                # Daily limit hit — no point retrying, raise immediately
                raise RuntimeError("DAILY_LIMIT_EXCEEDED") from e

            if is_rate_limit:
                if attempt < max_retries - 1:
                    wait = base_wait * (2 ** attempt)
                    time.sleep(wait)
                    continue
            raise
    raise RuntimeError("Max retries exceeded")


def _get_fallback_client():
    """Try llama3.1-8b on Groq as fallback (cheaper, lower token usage)."""
    return client, "llama3.1-8b"

SYSTEM_PROMPT = """You are a PostgreSQL query generator for an ecommerce analytics platform.
You MUST strictly follow ALL rules below. No exceptions.

═══ CRITICAL JOIN RULES ═══
1. ALWAYS use LEFT JOIN for returns: LEFT JOIN returns ret ON ret.order_item_id = oi.order_item_id
   NEVER use JOIN/INNER JOIN for returns — it makes return_rate always 100% (mathematically wrong)
2. For revenue lost from returns: use ret.refund_amount from returns table ONLY
   NEVER join the refunds table for return revenue
3. NEVER join sellers directly from orders — always via order_items
4. NEVER join warehouses directly from orders — always via shipments

═══ CRITICAL DATA RULES ═══
5. Payment mode values in DB are ONLY: 'COD' and 'Prepaid'
   NEVER use 'Online', 'UPI', 'Card', 'Wallet', 'Netbanking' — they do not exist
6. Category values in DB are ONLY: 'Footwear', 'Accessories', 'Dresses', 'Topwear', 'Bottomwear'
7. Order status values: 'Delivered', 'Shipped', 'Cancelled', 'Processing'
8. NEVER add WHERE clauses not in the plan
9. NEVER interpret time words (last, this, next) as city names

═══ OUTPUT RULES ═══
10. ALWAYS show seller_name (not seller_id) — JOIN sellers s ON oi.seller_id = s.seller_id
11. ALWAYS show customer city/name (not customer_id) when customer info needed
12. For dynamic month intervals: o.order_date >= date_trunc('month', current_date - interval 'N months')
13. COALESCE all aggregated columns for null safety
14. GROUP BY all non-aggregated selected columns
15. Use NULLIF for all divisions to prevent divide-by-zero

═══ EXACT DATABASE SCHEMA ═══
- customers: customer_id, signup_date, city, state, gender, age_group, loyalty_tier, preferred_payment_mode, risk_score, is_active
- inventory: inventory_id, product_id, seller_id, warehouse_id, available_qty, reserved_qty, damaged_qty, last_updated
- inventory_movements: movement_id, inventory_id, movement_type, quantity, reference_id, movement_date
- order_items: order_item_id, order_id, product_id, seller_id, quantity, item_price, item_status
- orders: order_id, customer_id, order_date, order_status, payment_mode, total_amount, discount_amount, final_payable, order_channel
- payments: payment_id, order_id, payment_method, payment_status, payment_date, paid_amount, gateway
- products: product_id, seller_id, brand, category, sub_category, size, color, mrp, selling_price, season, is_returnable
- refunds: refund_id, return_id, refund_method, refund_status, refund_date, refunded_amount
- returns: return_id, order_item_id, return_date, return_reason, return_type, return_status, pickup_date, refund_amount
- seller_settlements: settlement_id, seller_id, order_item_id, gross_amount, commission_amount, net_payable, settlement_date, settlement_status
- sellers: seller_id, seller_name, seller_type, onboarding_date, seller_rating, seller_region, commission_rate, risk_flag, is_active
- shipments: shipment_id, order_id, warehouse_id, courier_partner, shipped_date, promised_delivery_date, actual_delivery_date, delivery_status
- warehouses: warehouse_id, warehouse_city, warehouse_state, warehouse_type, is_active

═══ OUTPUT FORMAT (EXACTLY this) ═══
REASONING: <one sentence>
CONFIDENCE: <HIGH | MEDIUM | LOW>
SQL:
```sql
<complete query>
```

═══ STYLE RULES ═══
- Use explicit JOIN ... ON ... syntax, never implicit comma joins
- Never use SELECT *
- Always alias: orders=o, order_items=oi, products=p, sellers=s, customers=c, payments=pay, returns=ret, shipments=sh
- ORDER BY the primary metric DESC
- Use LIMIT 1000 unless a specific limit is requested
- Use CTEs (WITH clause) for multi-step logic"""


def build_prompt(user_query: str, context: str, session_history: list = None,
                 metric_instruction: str = "", intent: dict = None) -> list:
    history_text = ""
    if session_history:
        last = session_history[-1]
        prev_ctx = last.get("context", {})
        history_text = f"""
PREVIOUS QUERY IN THIS SESSION:
User asked: {last.get('query','')}
SQL used: {last.get('sql','')}
Structured context: metric={prev_ctx.get('metric','')}, dimension={prev_ctx.get('dimension','')}, time_filter={prev_ctx.get('time_filter','')}
If the new question is a follow-up, refine that SQL rather than starting fresh.
"""

    # Add inline examples for complex queries (no additional LLM call)
    few_shot_text = ""
    if intent and intent.get("filters") and len(intent["filters"]) > 1:
        few_shot_text = """
EXAMPLE OF COMPLEX QUERY WITH MULTIPLE FILTERS:
Question: Top 5 footwear brands where payment is Prepaid and item price above 2000
SQL:
SELECT p.brand, COALESCE(SUM(oi.item_price), 0) AS gmv
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status IN ('Delivered', 'Shipped')
  AND p.category ILIKE 'Footwear'
  AND o.payment_mode = 'Prepaid'
  AND oi.item_price >= 2000
GROUP BY p.brand
ORDER BY gmv DESC
LIMIT 5
"""

    user_content = f"""BUSINESS RULES (retrieved for this query):
{context}
{metric_instruction}
{few_shot_text}
{history_text}
USER QUESTION: {user_query}"""

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": user_content},
    ]


def generate_sql(user_query: str, context: str, session_history: list = None,
                 metric_instruction: str = "", intent: dict = None) -> dict:
    messages = build_prompt(user_query, context, session_history, metric_instruction, intent)
    try:
        response = _call_with_retry(lambda: client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=messages,
            temperature=0.0,
            max_tokens=1024,
        ))
    except RuntimeError as e:
        if "DAILY_LIMIT_EXCEEDED" in str(e):
            # Try fallback smaller model
            try:
                response = client.chat.completions.create(
                    model="llama-3.1-8b-instant",
                    messages=messages,
                    temperature=0.0,
                    max_tokens=1024,
                )
            except Exception:
                return {"reasoning": "", "confidence": "LOW", "sql": "",
                        "raw": "", "error": "API rate limit reached. Please wait a few minutes and try again."}
        else:
            return {"reasoning": "", "confidence": "LOW", "sql": "",
                    "raw": "", "error": str(e)}
    raw = response.choices[0].message.content
    return _parse_output(raw)


def _parse_output(raw: str) -> dict:
    reasoning = ""
    confidence = "MEDIUM"
    sql = ""

    # extract REASONING
    r_match = re.search(r"REASONING:\s*(.+?)(?=\nCONFIDENCE:|$)", raw, re.DOTALL)
    if r_match:
        reasoning = r_match.group(1).strip()

    # extract CONFIDENCE
    c_match = re.search(r"CONFIDENCE:\s*(HIGH|MEDIUM|LOW)", raw)
    if c_match:
        confidence = c_match.group(1)

    # extract SQL block
    s_match = re.search(r"```sql\s*(.*?)```", raw, re.DOTALL)
    if s_match:
        sql = s_match.group(1).strip()
    else:
        # fallback: everything after SQL:
        s_match2 = re.search(r"SQL:\s*(.+)", raw, re.DOTALL)
        if s_match2:
            sql = s_match2.group(1).strip().lstrip("`").rstrip("`")

    return {"reasoning": reasoning, "confidence": confidence, "sql": sql, "raw": raw}


def generate_insight(user_query: str, sql: str, result_rows: list) -> str:
    """Second Groq call: turn query results into a business narrative."""
    prompt = f"""You are a senior business analyst. Given this analytics question and result data,
write a 2-3 sentence business insight. Be specific with numbers. Identify the key driver.

Question: {user_query}
SQL Used: {sql}
Result (first 10 rows): {result_rows[:10]}

Write ONLY the insight. No preamble."""

    response = client.chat.completions.create(
        model="llama3.1-8b",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=300,
    )
    return response.choices[0].message.content.strip()


def recommend_visualization(columns: list, sample_data: list) -> dict:
    """Ask LLM to recommend best chart type given column types and data."""
    col_info = "\n".join([f"- {c['name']}: {c['dtype']}" for c in columns])
    prompt = f"""You are a data visualization expert. Given these columns and sample data,
recommend the BEST chart type and explain why in one sentence.

Columns:
{col_info}

Sample (3 rows): {sample_data[:3]}

Respond ONLY with JSON:
{{"chart_type": "bar|line|scatter|pie|heatmap|area|combo|table",
  "x_axis": "column_name",
  "y_axis": "column_name_or_null",
  "color_by": "column_name_or_null",
  "reason": "one sentence"}}"""

    response = client.chat.completions.create(
        model="llama3.1-8b",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
        max_tokens=200,
    )
    import json
    try:
        text = response.choices[0].message.content
        text = re.sub(r"```json|```", "", text).strip()
        return json.loads(text)
    except Exception:
        return {"chart_type": "table", "reason": "Could not determine best chart type"}
