# test_pipeline.py
"""
Tests the full NL2SQL pipeline.
Run from querymind/ directory: python test_pipeline.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

import json

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
INFO = "\033[94m[INFO]\033[0m"
WARN = "\033[93m[WARN]\033[0m"
SEP  = "-" * 70

failures = []

def check(label, condition, detail=""):
    if condition:
        print(f"  {PASS} {label}")
    else:
        print(f"  {FAIL} {label}" + (f" — {detail}" if detail else ""))
        failures.append(label)

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 1: semantic_engine
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("MODULE 1: semantic_engine")
print(SEP)

from core.semantic_engine import resolve_metric, enforce_metric

tests = [
    ("GMV last month",                    "gmv"),
    ("show me gross merchandise value",   "gmv"),
    ("what is the gross revenue today",   "gmv"),
    ("top 5 cities by revenue",           "gmv"),
    ("average order value by category",   "avg_order_value"),
    ("AOV this month",                    "avg_order_value"),
    ("return rate last 30 days",          "return_rate"),
    ("how many orders last week",         "order_count"),
    ("total orders this year",            "order_count"),
    ("active sellers this month",         "active_sellers"),
    ("unique customers last month",       "customer_count"),
    ("net revenue after returns",         "revenue_net"),
]

for query, expected_metric in tests:
    result = resolve_metric(query)
    got = result["metric"] if result else None
    check(f'resolve_metric("{query}") -> {expected_metric}', got == expected_metric,
          f"got: {got}")

# enforce_metric produces non-empty string for known metric
m = resolve_metric("GMV last month")
instruction = enforce_metric(m)
check("enforce_metric returns non-empty instruction", bool(instruction) and "gmv" in instruction.lower())
check("enforce_metric contains aggregation", "SUM" in instruction)
check("enforce_metric contains filter",      "Delivered" in instruction)

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 2: intent_parser
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("MODULE 2: intent_parser")
print(SEP)

from core.intent_parser import parse

# Test 1: GMV last month
i = parse("GMV last month")
check("GMV last month → metric=gmv",          i["metric"] and i["metric"]["metric"] == "gmv")
check("GMV last month → time_filter=last_month", i["time_filter"] == "last_month")
check("GMV last month → operation=aggregate", i["operation"] == "aggregate")
check("GMV last month → no dimension",        i["dimension"] is None)

# Test 2: Top 5 cities by revenue
i = parse("Top 5 cities by revenue")
check("Top 5 cities → metric=gmv",            i["metric"] and i["metric"]["metric"] == "gmv")
check("Top 5 cities → dimension=city",        i["dimension"] and i["dimension"]["column"] == "city")
check("Top 5 cities → operation=top_n",       i["operation"] == "top_n")
check("Top 5 cities → limit=5",               i["limit"] == 5)

# Test 3: Compare this month vs last month
i = parse("Compare this month vs last month")
check("Compare → operation=compare",          i["operation"] == "compare")
check("Compare → time_filter detected",       i["time_filter"] is not None)

# Test 4: Average order value by category
i = parse("Average order value by category")
check("AOV by category → metric=avg_order_value", i["metric"] and i["metric"]["metric"] == "avg_order_value")
check("AOV by category → dimension=category",     i["dimension"] and i["dimension"]["column"] == "category")

# Test 5: Follow-up with city filter
i = parse("Same as before but for Pune", last_context={
    "metric": "gmv", "time_filter": "last_month", "dimension": None, "filters": {}
})
check("Follow-up → is_followup=True",         i["is_followup"] is True)
check("Follow-up → inherits metric=gmv",      i["metric"] and i["metric"]["metric"] == "gmv")
check("Follow-up → inherits time_filter",     i["time_filter"] == "last_month")
check("Follow-up → city filter=Pune",         i["filters"].get("city") == "Pune")

# Test 6: Trend query
i = parse("Show GMV trend over time")
check("Trend → operation=trend",              i["operation"] == "trend")

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 3: planner
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("MODULE 3: planner")
print(SEP)

from core.planner import build, plan_to_prompt

# Test 1: GMV last month
intent = parse("GMV last month")
plan = build(intent)
check("GMV plan → primary_table=orders",      plan["primary_table"] == "orders")
check("GMV plan → joins order_items",         any(j["table"] == "order_items" for j in plan["joins"]))
check("GMV plan → join condition correct",    any("order_id" in j["condition"] for j in plan["joins"]))
check("GMV plan → where has status filter",   any("Delivered" in w for w in plan["where"]))
check("GMV plan → where has time filter",     any("month" in w for w in plan["where"]))
check("GMV plan → select has SUM",            any("SUM" in s for s in plan["select"]))

# Test 2: Top 5 cities by revenue
intent = parse("Top 5 cities by revenue")
plan = build(intent)
check("Cities plan → joins customers",        any(j["table"] == "customers" for j in plan["joins"]))
check("Cities plan → joins order_items",      any(j["table"] == "order_items" for j in plan["joins"]))
check("Cities plan → group_by has city",      any("city" in g for g in plan["group_by"]))
check("Cities plan → limit=5",               plan["limit"] == 5)

# Test 3: Compare this month vs last month
intent = parse("Compare this month vs last month")
plan = build(intent)
check("Compare plan → operation=compare",     plan["operation"] == "compare")
check("Compare plan → compare_periods set",   plan.get("compare_periods") is not None)

# Test 4: AOV by category
intent = parse("Average order value by category")
plan = build(intent)
check("AOV plan → joins order_items",         any(j["table"] == "order_items" for j in plan["joins"]))
check("AOV plan → joins products",            any(j["table"] == "products" for j in plan["joins"]))
check("AOV plan → group_by has category",     any("category" in g for g in plan["group_by"]))

# Verify join path is correct (orders → order_items → products, NOT orders → products directly)
if any(j["table"] == "products" for j in plan["joins"]):
    join_tables = [j["table"] for j in plan["joins"]]
    oi_idx = join_tables.index("order_items") if "order_items" in join_tables else -1
    p_idx  = join_tables.index("products")    if "products"    in join_tables else -1
    check("AOV plan → order_items before products in join chain", oi_idx < p_idx,
          f"join order: {join_tables}")

# plan_to_prompt produces non-empty string
prompt_text = plan_to_prompt(plan)
check("plan_to_prompt → non-empty",           bool(prompt_text))
check("plan_to_prompt → contains STRUCTURED QUERY PLAN", "STRUCTURED QUERY PLAN" in prompt_text)

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 4: sql_rule_engine
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("MODULE 4: sql_rule_engine")
print(SEP)

from core.sql_rule_engine import validate

# Valid SQL
valid_sql = """
SELECT c.city, COALESCE(SUM(oi.item_price), 0) AS gmv
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_status IN ('Delivered', 'Shipped')
GROUP BY c.city
ORDER BY gmv DESC
LIMIT 5
"""
r = validate(valid_sql)
check("Valid SQL passes rule engine",         r["valid"], str(r["errors"]))

# SELECT * blocked
r = validate("SELECT * FROM orders o")
check("SELECT * is blocked",                  not r["valid"])

# Direct sellers join from orders (no order_items) — must be blocked
bad_seller_sql = """
SELECT s.seller_name, COUNT(*) as cnt
FROM orders o
JOIN sellers s ON o.seller_id = s.seller_id
GROUP BY s.seller_name
"""
r = validate(bad_seller_sql)
check("Direct orders→sellers join blocked",   not r["valid"], str(r["errors"]))

# Direct warehouses join from orders (no shipments) — must be blocked
bad_wh_sql = """
SELECT w.warehouse_city, COUNT(*) as cnt
FROM orders o
JOIN warehouses w ON o.warehouse_id = w.warehouse_id
GROUP BY w.warehouse_city
"""
r = validate(bad_wh_sql)
check("Direct orders→warehouses join blocked", not r["valid"], str(r["errors"]))

# Correct sellers join via order_items — must pass
good_seller_sql = """
SELECT s.seller_name, COALESCE(SUM(oi.item_price), 0) AS revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN sellers s ON oi.seller_id = s.seller_id
WHERE o.order_status IN ('Delivered', 'Shipped')
GROUP BY s.seller_name
ORDER BY revenue DESC
LIMIT 10
"""
r = validate(good_seller_sql)
check("Correct sellers via order_items passes", r["valid"], str(r["errors"]))

# Empty SQL blocked
r = validate("")
check("Empty SQL blocked",                    not r["valid"])

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 5: metrics_engine
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("MODULE 5: metrics_engine")
print(SEP)

from core.metrics_engine import compute

# Empty rows
s = compute([])
check("Empty rows → row_count=0",             s["row_count"] == 0)
check("Empty rows → total=None",              s["total"] is None)

# Single aggregate row (GMV)
s = compute([{"gmv": 1500000.0}])
check("Single row → total=1500000",           s["total"] == 1500000.0)
check("Single row → row_count=1",             s["row_count"] == 1)

# Multi-row with label (top cities)
rows = [
    {"city": "Mumbai",    "gmv": 500000.0},
    {"city": "Delhi",     "gmv": 300000.0},
    {"city": "Bangalore", "gmv": 200000.0},
    {"city": "Pune",      "gmv": 50000.0},
    {"city": "Chennai",   "gmv": 30000.0},
]
s = compute(rows)
check("Multi-row → total correct",            s["total"] == 1080000.0)
check("Multi-row → top_contributors has 3",   len(s["top_contributors"]) == 3)
check("Multi-row → top contributor is Mumbai",s["top_contributors"][0]["label"] == "Mumbai")
check("Multi-row → share_pct sums ~100",      sum(c["share_pct"] for c in s["top_contributors"]) <= 100.1)

# Growth % (exactly 2 rows)
s = compute([{"period": "last_month", "gmv": 1000000.0}, {"period": "this_month", "gmv": 1200000.0}])
check("2-row growth → growth_pct=20.0",       s["growth_pct"] == 20.0, f"got: {s['growth_pct']}")

# Anomaly detection
rows_anomaly = [
    {"city": "Mumbai", "gmv": 9000000.0},
    {"city": "Delhi",  "gmv": 100000.0},
    {"city": "Pune",   "gmv": 90000.0},
    {"city": "Surat",  "gmv": 80000.0},
]
s = compute(rows_anomaly)
check("Anomaly detected for outlier",         s["anomaly"] is not None, f"got: {s['anomaly']}")

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 6: sql_generator (prompt building only — no API call)
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("MODULE 6: sql_generator (prompt structure)")
print(SEP)

from core.sql_generator import build_prompt

intent = parse("GMV last month")
plan   = build(intent)
prompt_block = plan_to_prompt(plan)
metric_instr = enforce_metric(intent["metric"])
full_ctx = f"{metric_instr}\n\n{prompt_block}"

messages = build_prompt("GMV last month", full_ctx, [], "")
check("build_prompt returns 2 messages",      len(messages) == 2)
check("system message present",               messages[0]["role"] == "system")
check("user message present",                 messages[1]["role"] == "user")
check("metric instruction in user message",   "gmv" in messages[1]["content"].lower())
check("plan in user message",                 "STRUCTURED QUERY PLAN" in messages[1]["content"])
check("SUM in user message",                  "SUM" in messages[1]["content"])
check("Delivered filter in user message",     "Delivered" in messages[1]["content"])
check("last_month filter in user message",    "month" in messages[1]["content"].lower())

# ═══════════════════════════════════════════════════════════════════════════════
# INTEGRATION TEST: Full pipeline (live API + DB calls)
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("INTEGRATION: Full pipeline (live Groq + Supabase)")
print(SEP)

from core.agent import run

TEST_QUERIES = [
    {
        "q": "GMV last month",
        "checks": {
            "metric": "gmv",
            "sql_has": ["SUM", "item_price", "order_items", "Delivered"],
            "sql_not": ["SELECT *"],
            "time_filter": "last_month",
        }
    },
    {
        "q": "Top 5 cities by revenue",
        "checks": {
            "metric": "gmv",
            "sql_has": ["SUM", "city", "customers", "order_items", "LIMIT"],
            "sql_not": ["SELECT *"],
            "dimension": "city",
        }
    },
    {
        "q": "Compare this month vs last month",
        "checks": {
            "sql_has": ["month", "order_date"],
            "sql_not": ["SELECT *"],
            "operation": "compare",
        }
    },
    {
        "q": "Average order value by category",
        "checks": {
            "metric": "avg_order_value",
            "sql_has": ["category", "order_items", "products"],
            "sql_not": ["SELECT *"],
            "dimension": "category",
        }
    },
]

for test in TEST_QUERIES:
    q = test["q"]
    checks = test["checks"]
    print(f"\n  Query: \"{q}\"")

    try:
        result = run(q, session_id=f"test_{q[:10].replace(' ','_')}", skip_insight=False)

        sql = result["sql"].upper()
        sql_raw = result["sql"]
        validation = result["validation"]
        intent_out = result.get("intent", {})
        metric_out  = result.get("metric")
        stats_out   = result.get("stats", {})

        # SQL was generated
        check("  SQL generated",               bool(sql_raw))

        # Validation passed
        check("  Validation passed",           validation["valid"],
              str(validation.get("errors", [])))

        # SQL contains expected keywords
        for kw in checks.get("sql_has", []):
            check(f"  SQL contains '{kw}'",    kw.upper() in sql, f"SQL: {sql_raw[:200]}")

        # SQL does NOT contain forbidden patterns
        for kw in checks.get("sql_not", []):
            check(f"  SQL does NOT contain '{kw}'", kw.upper() not in sql)

        # Metric check
        if "metric" in checks:
            got_metric = metric_out["metric"] if metric_out else None
            check(f"  Metric resolved = {checks['metric']}", got_metric == checks["metric"],
                  f"got: {got_metric}")

        # Dimension check
        if "dimension" in checks:
            dim = intent_out.get("dimension")
            got_dim = dim["column"] if dim else None
            check(f"  Dimension = {checks['dimension']}", got_dim == checks["dimension"],
                  f"got: {got_dim}")

        # Operation check
        if "operation" in checks:
            got_op = intent_out.get("operation")
            check(f"  Operation = {checks['operation']}", got_op == checks["operation"],
                  f"got: {got_op}")

        # Rows returned (or at least no DB error)
        if result.get("rows") is not None:
            row_count = result["row_count"]
            print(f"  {INFO} Rows returned: {row_count}")
            if row_count > 0:
                print(f"  {INFO} Sample row: {json.dumps(result['rows'][0], default=str)}")

        # Stats
        if stats_out and stats_out.get("total") is not None:
            print(f"  {INFO} Total: {stats_out['total']}")
        if stats_out and stats_out.get("top_contributors"):
            print(f"  {INFO} Top contributor: {stats_out['top_contributors'][0]}")
        if stats_out and stats_out.get("growth_pct") is not None:
            print(f"  {INFO} Growth: {stats_out['growth_pct']}%")

        # Insight quality
        insight = result.get("insight", "")
        check("  Insight generated",           bool(insight))
        if insight:
            is_generic = insight.lower().startswith("the data shows") or \
                         insight.lower().startswith("based on the data")
            check("  Insight is not generic",  not is_generic, f"insight: {insight[:100]}")
            print(f"  {INFO} Insight: {insight[:200]}")

        # Print full SQL for manual review
        print(f"  {INFO} SQL:\n    {sql_raw.replace(chr(10), chr(10)+'    ')}")

    except Exception as e:
        check(f"  Pipeline did not crash", False, str(e))
        import traceback
        traceback.print_exc()

# ═══════════════════════════════════════════════════════════════════════════════
# FOLLOW-UP MEMORY TEST
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("INTEGRATION: Follow-up memory test")
print(SEP)

SESSION = "memory_test"

print("\n  Turn 1: 'GMV last month'")
r1 = run("GMV last month", session_id=SESSION, skip_insight=True)
check("  Turn 1 SQL valid",                   r1["validation"]["valid"])
check("  Turn 1 metric=gmv",                  r1["metric"] and r1["metric"]["metric"] == "gmv")

print("\n  Turn 2: 'Same as before but for Pune'")
r2 = run("Same as before but for Pune", session_id=SESSION, skip_insight=True)
check("  Turn 2 is_followup=True",            r2["intent"].get("is_followup") is True)
check("  Turn 2 inherits metric=gmv",         r2["metric"] and r2["metric"]["metric"] == "gmv")
check("  Turn 2 SQL contains Pune",           "Pune" in r2["sql"])
check("  Turn 2 SQL valid",                   r2["validation"]["valid"])

# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
if failures:
    print(f"\033[91mFAILED: {len(failures)} checks failed:\033[0m")
    for f in failures:
        print(f"  - {f}")
else:
    print(f"\033[92mALL CHECKS PASSED\033[0m")
print(SEP)
