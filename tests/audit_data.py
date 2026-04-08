# audit_data.py
"""
Comprehensive data audit.
Every metric, every dimension, every operation.
Agent output vs direct Supabase ground truth.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dotenv import load_dotenv; load_dotenv()

from core.supabase_client import execute_sql
from core.agent import run
from core.intent_parser import parse

SEP   = "-" * 75
PASS  = "[PASS]"
FAIL  = "[FAIL]"
INFO  = "[INFO]"
WARN  = "[WARN]"
failures = []
warnings = []

def check(label, condition, detail=""):
    if condition:
        print(f"  {PASS} {label}")
    else:
        print(f"  {FAIL} {label}" + (f"\n         {detail}" if detail else ""))
        failures.append(label)

def warn(label, detail=""):
    print(f"  {WARN} {label}" + (f" -- {detail}" if detail else ""))
    warnings.append(label)

def direct(sql):
    r = execute_sql(sql)
    if not r["success"]:
        print(f"  {WARN} Direct query failed: {r.get('error','')[:100]}")
        return []
    return r["rows"]

def agent(question, session="audit"):
    return run(question, session_id=session, skip_insight=True)

def fval(rows, key):
    """Extract float from first row."""
    if not rows: return None
    v = rows[0].get(key)
    return round(float(v), 2) if v is not None else None

def close(a, b, pct=0.01):
    """True if a and b are within pct of each other."""
    if a is None or b is None: return False
    if b == 0 and a == 0: return True
    if b == 0: return False
    return abs(a - b) / abs(b) < pct

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("SECTION 1: ALL METRICS — ground truth vs agent")
print(SEP)

# ── GMV (all time) ───────────────────────────────────────────────────────────
print("\n  [GMV all time]")
gt = fval(direct("""
    SELECT COALESCE(SUM(oi.item_price),0) AS gmv
    FROM orders o JOIN order_items oi ON o.order_id=oi.order_id
    WHERE o.order_status IN ('Delivered','Shipped')
"""), "gmv")
r = agent("total GMV this year", "m1")
if r.get("clarification_needed"):
    warn("GMV this year returned clarification instead of SQL", r.get("note",""))
else:
    ag = fval(r["rows"], "gmv") if r["rows"] else None
    gt2 = fval(direct("""
        SELECT COALESCE(SUM(oi.item_price),0) AS gmv
        FROM orders o JOIN order_items oi ON o.order_id=oi.order_id
        WHERE o.order_status IN ('Delivered','Shipped')
        AND date_trunc('year',o.order_date)=date_trunc('year',current_date)
    """), "gmv")
    check(f"GMV this year: agent={ag} vs direct={gt2}", close(ag, gt2),
          f"agent={ag}, direct={gt2}")
    print(f"  {INFO} SQL: {r['sql'][:120]}")

# ── ORDER COUNT ──────────────────────────────────────────────────────────────
print("\n  [Order count this year]")
gt_oc = fval(direct("""
    SELECT COUNT(DISTINCT o.order_id) AS order_count
    FROM orders o
    WHERE o.order_status != 'Cancelled'
    AND date_trunc('year',o.order_date)=date_trunc('year',current_date)
"""), "order_count")
r = agent("total orders this year", "m2")
if r.get("clarification_needed"):
    warn("Order count returned clarification", r.get("note",""))
else:
    ag = fval(r["rows"], "order_count") if r["rows"] else None
    check(f"Order count this year: agent={ag} vs direct={gt_oc}",
          close(ag, gt_oc), f"agent={ag}, direct={gt_oc}")

# ── AVG ORDER VALUE ──────────────────────────────────────────────────────────
print("\n  [Average order value]")
gt_aov = fval(direct("""
    SELECT COALESCE(SUM(oi.item_price)/NULLIF(COUNT(DISTINCT o.order_id),0),0) AS avg_order_value
    FROM orders o JOIN order_items oi ON o.order_id=oi.order_id
    WHERE o.order_status IN ('Delivered','Shipped')
"""), "avg_order_value")
r = agent("average order value", "m3")
if r.get("clarification_needed"):
    warn("AOV returned clarification", r.get("note",""))
else:
    ag = fval(r["rows"], "avg_order_value") if r["rows"] else None
    check(f"AOV: agent={ag} vs direct={gt_aov}",
          close(ag, gt_aov), f"agent={ag}, direct={gt_aov}")

# ── RETURN RATE ──────────────────────────────────────────────────────────────
print("\n  [Return rate]")
gt_rr = fval(direct("""
    SELECT COUNT(ret.return_id)*100.0/NULLIF(COUNT(oi.order_item_id),0) AS return_rate
    FROM order_items oi
    LEFT JOIN returns ret ON ret.order_item_id=oi.order_item_id
"""), "return_rate")
r = agent("return rate", "m4")
if r.get("clarification_needed"):
    warn("Return rate returned clarification", r.get("note",""))
else:
    ag = fval(r["rows"], "return_rate") if r["rows"] else None
    check(f"Return rate: agent={ag} vs direct={gt_rr}",
          close(ag, gt_rr), f"agent={ag}, direct={gt_rr}")

# ── ACTIVE SELLERS ───────────────────────────────────────────────────────────
print("\n  [Active sellers]")
gt_as = fval(direct("""
    SELECT COUNT(DISTINCT oi.seller_id) AS active_sellers
    FROM orders o JOIN order_items oi ON o.order_id=oi.order_id
    WHERE o.order_status='Delivered'
"""), "active_sellers")
r = agent("active sellers this year", "m5")
if r.get("clarification_needed"):
    warn("Active sellers returned clarification", r.get("note",""))
else:
    ag = fval(r["rows"], "active_sellers") if r["rows"] else None
    gt2 = fval(direct("""
        SELECT COUNT(DISTINCT oi.seller_id) AS active_sellers
        FROM orders o JOIN order_items oi ON o.order_id=oi.order_id
        WHERE o.order_status='Delivered'
        AND date_trunc('year',o.order_date)=date_trunc('year',current_date)
    """), "active_sellers")
    check(f"Active sellers this year: agent={ag} vs direct={gt2}",
          close(ag, gt2), f"agent={ag}, direct={gt2}")

# ── CUSTOMER COUNT ───────────────────────────────────────────────────────────
print("\n  [Customer count]")
r = agent("unique customers this year", "m6")
if r.get("clarification_needed"):
    warn("Customer count returned clarification", r.get("note",""))
else:
    ag = fval(r["rows"], "customer_count") if r["rows"] else None
    gt2 = fval(direct("""
        SELECT COUNT(DISTINCT o.customer_id) AS customer_count
        FROM orders o
        WHERE o.order_status!='Cancelled'
        AND date_trunc('year',o.order_date)=date_trunc('year',current_date)
    """), "customer_count")
    check(f"Customer count this year: agent={ag} vs direct={gt2}",
          close(ag, gt2), f"agent={ag}, direct={gt2}")

# ── NET REVENUE ──────────────────────────────────────────────────────────────
print("\n  [Net revenue]")
r = agent("net revenue this year", "m7")
if r.get("clarification_needed"):
    warn("Net revenue returned clarification", r.get("note",""))
else:
    ag = fval(r["rows"], "revenue_net") if r["rows"] else None
    gt2 = fval(direct("""
        SELECT COALESCE(SUM(oi.item_price),0) - COALESCE(SUM(ret.refund_amount),0) AS revenue_net
        FROM orders o
        JOIN order_items oi ON o.order_id=oi.order_id
        LEFT JOIN returns ret ON ret.order_item_id=oi.order_item_id
        WHERE o.order_status IN ('Delivered','Shipped')
        AND date_trunc('year',o.order_date)=date_trunc('year',current_date)
    """), "revenue_net")
    check(f"Net revenue this year: agent={ag} vs direct={gt2}",
          close(ag, gt2), f"agent={ag}, direct={gt2}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("SECTION 2: ALL DIMENSIONS — group-by correctness")
print(SEP)

dims = [
    ("Top 5 cities by revenue",       "city",          "gmv",             "customers",  "city"),
    ("Top 5 states by revenue",        "state",         "gmv",             "customers",  "state"),
    ("GMV by category",                "category",      "gmv",             "products",   "category"),
    ("GMV by brand",                   "brand",         "gmv",             "products",   "brand"),
    ("revenue by seller",              "seller_name",   "gmv",             "sellers",    "seller_name"),
    ("orders by channel",              "order_channel", "order_count",     "orders",     "order_channel"),
    ("orders by payment",              "payment_mode",  "order_count",     "orders",     "payment_mode"),
    ("GMV by gender",                  "gender",        "gmv",             "customers",  "gender"),
    ("orders by status",               "order_status",  "order_count",     "orders",     "order_status"),
]

for query, dim_col, metric_col, dim_table, col_name in dims:
    print(f"\n  [{query}]")
    r = agent(query, f"dim_{dim_col}")
    if r.get("clarification_needed"):
        warn(f"'{query}' returned clarification", r.get("note",""))
        continue
    check(f"  SQL valid",                r["validation"]["valid"])
    check(f"  Has rows",                 r["row_count"] > 0, f"got {r['row_count']}")
    if r["rows"]:
        first = r["rows"][0]
        check(f"  Has '{dim_col}' column",  dim_col in first or col_name in first,
              f"keys: {list(first.keys())}")
        check(f"  Has '{metric_col}' column", metric_col in first,
              f"keys: {list(first.keys())}")
        print(f"  {INFO} Sample: {first}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("SECTION 3: ALL TIME FILTERS — date range correctness")
print(SEP)

time_tests = [
    ("GMV today",           "today",        "date_trunc('day', o.order_date) = current_date"),
    ("GMV yesterday",       "yesterday",    "o.order_date >= current_date - interval '1 day' AND o.order_date < current_date"),
    ("GMV last 7 days",     "last_7_days",  "o.order_date >= current_date - interval '7 days'"),
    ("GMV last 30 days",    "last_30_days", "o.order_date >= current_date - interval '30 days'"),
    ("GMV this month",      "this_month",   "date_trunc('month', o.order_date) = date_trunc('month', current_date)"),
    ("GMV last month",      "last_month",   "date_trunc('month', o.order_date) = date_trunc('month', current_date - interval '1 month')"),
    ("GMV last quarter",    "last_quarter", "o.order_date >= date_trunc('quarter', current_date - interval '3 months')"),
    ("GMV this year",       "this_year",    "date_trunc('year', o.order_date) = date_trunc('year', current_date)"),
]

for query, tf_key, date_condition in time_tests:
    print(f"\n  [{query}]")
    i = parse(query)
    check(f"  intent time_filter={tf_key}", i["time_filter"] == tf_key,
          f"got: {i['time_filter']}")

    r = agent(query, f"tf_{tf_key}")
    if r.get("clarification_needed"):
        warn(f"'{query}' returned clarification")
        continue
    check(f"  SQL valid",       r["validation"]["valid"])
    check(f"  SQL has date filter", any(kw in r["sql"].lower() for kw in
          ["date_trunc", "current_date", "interval"]),
          f"SQL: {r['sql'][:100]}")

    # Cross-check value
    gt2 = fval(direct(f"""
        SELECT COALESCE(SUM(oi.item_price),0) AS gmv
        FROM orders o JOIN order_items oi ON o.order_id=oi.order_id
        WHERE o.order_status IN ('Delivered','Shipped')
        AND {date_condition}
    """), "gmv")
    ag = fval(r["rows"], "gmv") if r["rows"] else None
    check(f"  Value matches direct ({gt2})", close(ag, gt2),
          f"agent={ag}, direct={gt2}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("SECTION 4: JOIN CORRECTNESS — verify no wrong joins")
print(SEP)

join_tests = [
    # (query, must_contain_tables, must_NOT_skip_bridge)
    ("GMV by seller",
     ["order_items", "sellers"],
     "sellers must come via order_items"),
    ("GMV last month",
     ["order_items"],
     "GMV always needs order_items"),
    ("Average order value by category",
     ["order_items", "products"],
     "products must come via order_items"),
    ("Top 5 cities by revenue",
     ["order_items", "customers"],
     "customers joined via orders"),
    ("return rate",
     ["order_items", "returns"],
     "returns joined via order_items"),
]

for query, required_tables, rule in join_tests:
    print(f"\n  [{query}]")
    r = agent(query, f"join_{query[:10]}")
    if r.get("clarification_needed"):
        warn(f"'{query}' returned clarification")
        continue
    sql_lower = r["sql"].lower()
    for tbl in required_tables:
        check(f"  SQL contains '{tbl}'", tbl in sql_lower,
              f"rule: {rule}")
    check(f"  SQL valid (no illegal joins)", r["validation"]["valid"],
          str(r["validation"].get("errors",[])))

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("SECTION 5: EDGE CASES & SAFETY")
print(SEP)

# Ambiguous — must never return SQL
ambiguous = [
    "show me data",
    "top products",
    "Compare this month vs last month",
    "what is the trend",
    "bottom sellers",
    "show me something",
]
print("\n  [Ambiguous queries — must return clarification, never SQL]")
for q in ambiguous:
    r = agent(q, "edge_amb")
    check(f"  '{q}' -> clarification",
          r.get("clarification_needed") is True and r["sql"] == "",
          f"sql='{r['sql'][:50]}', clarification={r.get('clarification_needed')}")

# Safety — DDL must be blocked
print("\n  [Safety — dangerous SQL blocked]")
from core.safety import enforce
dangerous = [
    "DROP TABLE orders",
    "DELETE FROM orders WHERE 1=1",
    "TRUNCATE order_items",
    "INSERT INTO orders VALUES (1,2,3)",
    "UPDATE orders SET order_status='x'",
    "SELECT * FROM orders; DROP TABLE orders",
]
for sql in dangerous:
    s = enforce(sql)
    check(f"  Blocked: '{sql[:40]}'", not s["safe"])

# LIMIT enforcement
print("\n  [LIMIT enforcement]")
from core.safety import enforce, MAX_ROWS
s = enforce("SELECT o.order_id FROM orders o")
check(f"  LIMIT injected when missing", f"LIMIT {MAX_ROWS}" in s["sql"])
s = enforce(f"SELECT o.order_id FROM orders o LIMIT {MAX_ROWS * 10}")
check(f"  LIMIT clamped to {MAX_ROWS}", f"LIMIT {MAX_ROWS}" in s["sql"])

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("SECTION 6: MEMORY / FOLLOW-UP CORRECTNESS")
print(SEP)

follow_up_tests = [
    # (turn1, turn2, check_field, expected_value)
    ("GMV last month",              "same for Mumbai",    "city",        "Mumbai"),
    ("GMV last month",              "same for Bangalore", "city",        "Bangalore"),
    ("Top 5 cities by revenue",     "same for last month","time_filter", "last_month"),
    ("return rate last 7 days",     "same for Delhi",     "city",        "Delhi"),
    ("average order value by category", "same but this year", "time_filter", "this_year"),
]

for t1, t2, check_field, expected in follow_up_tests:
    print(f"\n  ['{t1}' -> '{t2}']")
    run(t1, session_id=f"fu_{t1[:8]}", skip_insight=True)
    r = agent(t2, f"fu_{t1[:8]}")

    check(f"  is_followup=True",     r["intent"].get("is_followup") is True)
    check(f"  SQL valid",            r["validation"]["valid"],
          str(r["validation"].get("errors",[])))

    if check_field == "city":
        got = r["intent"].get("filters", {}).get("city")
        check(f"  city={expected}",  got == expected, f"got: {got}")
        if r["rows"] is not None:
            # Cross-check value against direct query WITH the same time filter
            inherited_time = r["intent"].get("time_filter", "")
            time_clause = ""
            if inherited_time == "last_month":
                time_clause = "AND date_trunc('month',o.order_date)=date_trunc('month',current_date - interval '1 month')"
            elif inherited_time == "this_month":
                time_clause = "AND date_trunc('month',o.order_date)=date_trunc('month',current_date)"
            elif inherited_time == "this_year":
                time_clause = "AND date_trunc('year',o.order_date)=date_trunc('year',current_date)"
            gt_rows = direct(f"""
                SELECT COALESCE(SUM(oi.item_price),0) AS gmv
                FROM orders o
                JOIN order_items oi ON o.order_id=oi.order_id
                JOIN customers c ON o.customer_id=c.customer_id
                WHERE o.order_status IN ('Delivered','Shipped')
                AND c.city='{expected}'
                {time_clause}
            """)
            gt_val = fval(gt_rows, "gmv")
            ag_val = fval(r["rows"], "gmv") if r["rows"] else None
            if ag_val is not None:
                check(f"  {expected} GMV matches direct ({gt_val}) [time={inherited_time or 'all'}]",
                      close(ag_val, gt_val), f"agent={ag_val}, direct={gt_val}")
    elif check_field == "time_filter":
        got = r["intent"].get("time_filter")
        check(f"  time_filter={expected}", got == expected, f"got: {got}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("SECTION 7: METRIC FORMULA CORRECTNESS")
print(SEP)

# Verify the exact formula used matches YAML definition
print("\n  [GMV formula: SUM(oi.item_price) with Delivered/Shipped filter]")
r = agent("GMV this year", "formula_1")
if not r.get("clarification_needed"):
    sql = r["sql"].upper()
    check("  Uses SUM(OI.ITEM_PRICE)",          "SUM(OI.ITEM_PRICE)" in sql)
    check("  Filters Delivered",                "DELIVERED" in sql)
    check("  Filters Shipped",                  "SHIPPED" in sql)
    check("  Does NOT use total_amount",        "TOTAL_AMOUNT" not in sql,
          "total_amount is wrong column for GMV")
    check("  Does NOT use final_payable",       "FINAL_PAYABLE" not in sql,
          "final_payable is wrong column for GMV")

print("\n  [AOV formula: SUM/NULLIF(COUNT(DISTINCT order_id))]")
r = agent("average order value by category", "formula_2")
if not r.get("clarification_needed"):
    sql = r["sql"].upper()
    check("  Uses NULLIF for division safety",  "NULLIF" in sql)
    check("  Uses COUNT(DISTINCT",              "COUNT(DISTINCT" in sql)
    check("  Uses SUM(OI.ITEM_PRICE)",          "SUM(OI.ITEM_PRICE)" in sql)

print("\n  [Return rate formula: COUNT(ret)/COUNT(oi)]")
r = agent("return rate last 30 days", "formula_3")
if not r.get("clarification_needed"):
    sql = r["sql"].upper()
    check("  Joins returns table",              "RETURNS" in sql)
    check("  Joins order_items",                "ORDER_ITEMS" in sql)
    check("  Uses COUNT",                       "COUNT" in sql)
    check("  Uses NULLIF for division safety",  "NULLIF" in sql)

print("\n  [Net revenue: SUM(item_price) - SUM(refund_amount)]")
r = agent("net revenue this year", "formula_4")
if not r.get("clarification_needed"):
    sql = r["sql"].upper()
    check("  Joins returns table",              "RETURNS" in sql)
    check("  Uses SUM(OI.ITEM_PRICE)",          "SUM(OI.ITEM_PRICE)" in sql or "ITEM_PRICE" in sql)
    check("  References refund_amount",         "REFUND_AMOUNT" in sql)

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("SECTION 8: CONFIDENCE SCORE SANITY")
print(SEP)

conf_tests = [
    ("GMV this year",                "should be HIGH or MEDIUM", ["HIGH","MEDIUM"]),
    ("Top 5 cities by revenue",      "should be HIGH",           ["HIGH"]),
    ("top products",                 "should be LOW (clarification)", ["LOW"]),
    ("show me data",                 "should be LOW (clarification)", ["LOW"]),
]
for q, desc, expected_levels in conf_tests:
    r = agent(q, f"conf_{q[:8]}")
    conf = r.get("confidence","")
    check(f"  '{q}' confidence {desc}",
          conf in expected_levels, f"got: {conf}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
total = len(failures) + len(warnings)
if failures:
    print(f"FAILED: {len(failures)} checks failed:")
    for f in failures:
        print(f"  FAIL: {f}")
if warnings:
    print(f"WARNINGS: {len(warnings)}:")
    for w in warnings:
        print(f"  WARN: {w}")
if not failures:
    print(f"ALL CHECKS PASSED  ({len(warnings)} warnings)")
print(SEP)
