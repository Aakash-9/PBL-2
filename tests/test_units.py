# test_units.py — unit tests only, no API calls
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dotenv import load_dotenv
load_dotenv()

PASS = "[PASS]"
FAIL = "[FAIL]"
INFO = "[INFO]"
SEP  = "-" * 70
failures = []

def check(label, condition, detail=""):
    if condition:
        print(f"  {PASS} {label}")
    else:
        print(f"  {FAIL} {label}" + (f" -- {detail}" if detail else ""))
        failures.append(label)

# ── MODULE 1: semantic_engine ────────────────────────────────────────────────
print(f"\n{SEP}\nMODULE 1: semantic_engine\n{SEP}")
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
for query, expected in tests:
    result = resolve_metric(query)
    got = result["metric"] if result else None
    check(f'resolve_metric("{query}") -> {expected}', got == expected, f"got: {got}")

m = resolve_metric("GMV last month")
instr = enforce_metric(m)
check("enforce_metric non-empty",         bool(instr))
check("enforce_metric has SUM",           "SUM" in instr)
check("enforce_metric has Delivered",     "Delivered" in instr)

# ── MODULE 2: intent_parser ──────────────────────────────────────────────────
print(f"\n{SEP}\nMODULE 2: intent_parser\n{SEP}")
from core.intent_parser import parse

i = parse("GMV last month")
check("GMV last month -> metric=gmv",          i["metric"] and i["metric"]["metric"] == "gmv")
check("GMV last month -> time_filter=last_month", i["time_filter"] == "last_month")
check("GMV last month -> operation=aggregate", i["operation"] == "aggregate")
check("GMV last month -> no dimension",        i["dimension"] is None)

i = parse("Top 5 cities by revenue")
check("Top 5 cities -> metric=gmv",            i["metric"] and i["metric"]["metric"] == "gmv")
check("Top 5 cities -> dimension=city",        i["dimension"] and i["dimension"]["column"] == "city")
check("Top 5 cities -> operation=top_n",       i["operation"] == "top_n")
check("Top 5 cities -> limit=5",               i["limit"] == 5)

i = parse("Compare this month vs last month")
check("Compare -> operation=compare",          i["operation"] == "compare")
check("Compare -> time_filter detected",       i["time_filter"] is not None)

i = parse("Average order value by category")
check("AOV by category -> metric=avg_order_value", i["metric"] and i["metric"]["metric"] == "avg_order_value")
check("AOV by category -> dimension=category",     i["dimension"] and i["dimension"]["column"] == "category")

i = parse("Same as before but for Pune", last_context={
    "metric": "gmv", "time_filter": "last_month", "dimension": None, "filters": {}
})
check("Follow-up -> is_followup=True",         i["is_followup"] is True)
check("Follow-up -> inherits metric=gmv",      i["metric"] and i["metric"]["metric"] == "gmv")
check("Follow-up -> inherits time_filter",     i["time_filter"] == "last_month")
check("Follow-up -> city filter=Pune",         i["filters"].get("city") == "Pune")

i = parse("Show GMV trend over time")
check("Trend -> operation=trend",              i["operation"] == "trend")

# ── MODULE 3: planner ────────────────────────────────────────────────────────
print(f"\n{SEP}\nMODULE 3: planner\n{SEP}")
from core.planner import build, plan_to_prompt

intent = parse("GMV last month")
plan = build(intent)
check("GMV plan -> primary_table=orders",      plan["primary_table"] == "orders")
check("GMV plan -> joins order_items",         any(j["table"] == "order_items" for j in plan["joins"]))
check("GMV plan -> join condition correct",    any("order_id" in j["condition"] for j in plan["joins"]))
check("GMV plan -> where has status filter",   any("Delivered" in w for w in plan["where"]))
check("GMV plan -> where has time filter",     any("month" in w for w in plan["where"]))
check("GMV plan -> select has SUM",            any("SUM" in s for s in plan["select"]))

intent = parse("Top 5 cities by revenue")
plan = build(intent)
check("Cities plan -> joins customers",        any(j["table"] == "customers" for j in plan["joins"]))
check("Cities plan -> joins order_items",      any(j["table"] == "order_items" for j in plan["joins"]))
check("Cities plan -> group_by has city",      any("city" in g for g in plan["group_by"]))
check("Cities plan -> limit=5",               plan["limit"] == 5)

intent = parse("Compare this month vs last month")
plan = build(intent)
check("Compare plan -> operation=compare",     plan["operation"] == "compare")
check("Compare plan -> compare_periods set",   plan.get("compare_periods") is not None)
check("Compare plan -> this_month period",     "this_month" in (plan.get("compare_periods") or {}))
check("Compare plan -> last_month period",     "last_month" in (plan.get("compare_periods") or {}))

intent = parse("Average order value by category")
plan = build(intent)
check("AOV plan -> joins order_items",         any(j["table"] == "order_items" for j in plan["joins"]))
check("AOV plan -> joins products",            any(j["table"] == "products" for j in plan["joins"]))
check("AOV plan -> group_by has category",     any("category" in g for g in plan["group_by"]))
join_tables = [j["table"] for j in plan["joins"]]
oi_idx = join_tables.index("order_items") if "order_items" in join_tables else -1
p_idx  = join_tables.index("products")    if "products"    in join_tables else -1
check("AOV plan -> order_items before products", oi_idx != -1 and p_idx != -1 and oi_idx < p_idx,
      f"join order: {join_tables}")

prompt_text = plan_to_prompt(plan)
check("plan_to_prompt non-empty",              bool(prompt_text))
check("plan_to_prompt has STRUCTURED QUERY PLAN", "STRUCTURED QUERY PLAN" in prompt_text)

# ── MODULE 4: sql_rule_engine ────────────────────────────────────────────────
print(f"\n{SEP}\nMODULE 4: sql_rule_engine\n{SEP}")
from core.sql_rule_engine import validate

valid_sql = """
SELECT c.city, COALESCE(SUM(oi.item_price), 0) AS gmv
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_status IN ('Delivered', 'Shipped')
GROUP BY c.city ORDER BY gmv DESC LIMIT 5
"""
r = validate(valid_sql)
check("Valid SQL passes",                      r["valid"], str(r["errors"]))

r = validate("SELECT * FROM orders o")
check("SELECT * blocked",                      not r["valid"])

bad_seller = """
SELECT s.seller_name, COUNT(*) as cnt
FROM orders o JOIN sellers s ON o.seller_id = s.seller_id
GROUP BY s.seller_name
"""
r = validate(bad_seller)
check("Direct orders->sellers blocked",        not r["valid"], str(r["errors"]))

bad_wh = """
SELECT w.warehouse_city, COUNT(*) as cnt
FROM orders o JOIN warehouses w ON o.warehouse_id = w.warehouse_id
GROUP BY w.warehouse_city
"""
r = validate(bad_wh)
check("Direct orders->warehouses blocked",     not r["valid"], str(r["errors"]))

good_seller = """
SELECT s.seller_name, COALESCE(SUM(oi.item_price), 0) AS revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN sellers s ON oi.seller_id = s.seller_id
WHERE o.order_status IN ('Delivered', 'Shipped')
GROUP BY s.seller_name ORDER BY revenue DESC LIMIT 10
"""
r = validate(good_seller)
check("Correct sellers via order_items passes", r["valid"], str(r["errors"]))

r = validate("")
check("Empty SQL blocked",                     not r["valid"])

# ── MODULE 5: metrics_engine ─────────────────────────────────────────────────
print(f"\n{SEP}\nMODULE 5: metrics_engine\n{SEP}")
from core.metrics_engine import compute

s = compute([])
check("Empty -> row_count=0",                  s["row_count"] == 0)
check("Empty -> total=None",                   s["total"] is None)

s = compute([{"gmv": 1500000.0}])
check("Single row -> total=1500000",           s["total"] == 1500000.0)
check("Single row -> row_count=1",             s["row_count"] == 1)

rows = [
    {"city": "Mumbai",    "gmv": 500000.0},
    {"city": "Delhi",     "gmv": 300000.0},
    {"city": "Bangalore", "gmv": 200000.0},
    {"city": "Pune",      "gmv": 50000.0},
    {"city": "Chennai",   "gmv": 30000.0},
]
s = compute(rows)
check("Multi-row -> total=1080000",            s["total"] == 1080000.0)
check("Multi-row -> top_contributors=3",       len(s["top_contributors"]) == 3)
check("Multi-row -> top is Mumbai",            s["top_contributors"][0]["label"] == "Mumbai")
check("Multi-row -> share_pct <= 100",         sum(c["share_pct"] for c in s["top_contributors"]) <= 100.1)

s = compute([{"period": "last_month", "gmv": 1000000.0}, {"period": "this_month", "gmv": 1200000.0}])
check("2-row growth -> 20.0%",                 s["growth_pct"] == 20.0, f"got: {s['growth_pct']}")

rows_anomaly = [
    {"city": "Mumbai", "gmv": 9000000.0},
    {"city": "Delhi",  "gmv": 100000.0},
    {"city": "Pune",   "gmv": 90000.0},
    {"city": "Surat",  "gmv": 80000.0},
]
s = compute(rows_anomaly)
check("Anomaly detected",                      s["anomaly"] is not None, f"got: {s['anomaly']}")

# ── MODULE 6: sql_generator prompt structure ─────────────────────────────────
print(f"\n{SEP}\nMODULE 6: sql_generator (prompt structure)\n{SEP}")
from core.sql_generator import build_prompt
from core.semantic_engine import enforce_metric

intent = parse("GMV last month")
plan   = build(intent)
full_ctx = f"{enforce_metric(intent['metric'])}\n\n{plan_to_prompt(plan)}"
msgs = build_prompt("GMV last month", full_ctx, [], "")
check("2 messages",                            len(msgs) == 2)
check("system role",                           msgs[0]["role"] == "system")
check("user role",                             msgs[1]["role"] == "user")
check("metric in user msg",                    "gmv" in msgs[1]["content"].lower())
check("STRUCTURED QUERY PLAN in user msg",     "STRUCTURED QUERY PLAN" in msgs[1]["content"])
check("SUM in user msg",                       "SUM" in msgs[1]["content"])
check("Delivered in user msg",                 "Delivered" in msgs[1]["content"])
check("month filter in user msg",              "month" in msgs[1]["content"].lower())

# ── SUMMARY ──────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
if failures:
    print(f"FAILED: {len(failures)} checks failed:")
    for f in failures:
        print(f"  - {f}")
else:
    print("ALL UNIT CHECKS PASSED")
print(SEP)
