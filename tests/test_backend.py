# test_backend.py
"""
Backend HTTP tests. Hits the live FastAPI server on localhost:8000.

Usage:
  Step 1 — Start the server in one terminal:
      cd querymind && python main.py

  Step 2 — Run this script in another terminal:
      python test_backend.py
"""
import requests
import json
import sys
import time

BASE = "http://localhost:8000"
SEP  = "-" * 65
PASS = "[PASS]"
FAIL = "[FAIL]"
INFO = "[INFO]"
failures = []

def check(label, condition, detail=""):
    if condition:
        print(f"  {PASS} {label}")
    else:
        print(f"  {FAIL} {label}" + (f"\n         {detail}" if detail else ""))
        failures.append(label)

def post(path, body):
    try:
        r = requests.post(f"{BASE}{path}", json=body, timeout=60)
        return r.status_code, r.json()
    except requests.exceptions.ConnectionError:
        print(f"\n  ERROR: Cannot connect to {BASE}")
        print("  Make sure the server is running: python main.py")
        sys.exit(1)
    except Exception as e:
        return 500, {"error": str(e)}

def get(path):
    try:
        r = requests.get(f"{BASE}{path}", timeout=30)
        return r.status_code, r.json()
    except requests.exceptions.ConnectionError:
        print(f"\n  ERROR: Cannot connect to {BASE}")
        print("  Make sure the server is running: python main.py")
        sys.exit(1)
    except Exception as e:
        return 500, {"error": str(e)}

def delete(path):
    try:
        r = requests.delete(f"{BASE}{path}", timeout=10)
        return r.status_code, r.json()
    except Exception as e:
        return 500, {"error": str(e)}

# ── Check server is up ───────────────────────────────────────────────────────
print(f"\n{SEP}")
print("CHECKING SERVER")
print(SEP)
status, data = get("/health")
check("Server is running", status == 200, f"status={status}")
check("Health returns ok", data.get("status") == "ok", str(data))
print(f"  {INFO} Version: {data.get('version')}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("ENDPOINT: GET /api/schema")
print(SEP)

status, data = get("/api/schema")
check("Status 200",                    status == 200)
check("Has 'tables' key",              "tables" in data)
check("Has 'table_count' key",         "table_count" in data)
check("table_count > 0",               data.get("table_count", 0) > 0,
      f"got {data.get('table_count')}")
check("Has orders table",              any(t["name"] == "orders" for t in data.get("tables", [])))
check("Has order_items table",         any(t["name"] == "order_items" for t in data.get("tables", [])))
check("Has customers table",           any(t["name"] == "customers" for t in data.get("tables", [])))
print(f"  {INFO} Tables found: {[t['name'] for t in data.get('tables', [])]}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("ENDPOINT: GET /api/schema/{table}/sample")
print(SEP)

status, data = get("/api/schema/orders/sample?limit=5")
check("Status 200",                    status == 200)
check("Has rows",                      len(data.get("rows", [])) > 0)
check("Row has order_id",              "order_id" in (data.get("rows") or [{}])[0])
print(f"  {INFO} Sample row: {json.dumps(list((data.get('rows') or [{}])[0].items())[:4], default=str)}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("ENDPOINT: POST /api/query — core NL2SQL tests")
print(SEP)

query_tests = [
    {
        "name":     "GMV this year",
        "body":     {"question": "GMV this year", "session_id": "http_test_1"},
        "checks": {
            "has_sql":          True,
            "sql_contains":     ["SUM", "item_price", "order_items"],
            "sql_not_contains": ["SELECT *"],
            "valid":            True,
            "has_rows":         True,
            "metric":           "gmv",
            "confidence_not":   "LOW",
            "no_clarification": True,
        }
    },
    {
        "name":     "Top 5 cities by revenue",
        "body":     {"question": "Top 5 cities by revenue", "session_id": "http_test_2"},
        "checks": {
            "has_sql":          True,
            "sql_contains":     ["city", "customers", "order_items", "LIMIT"],
            "sql_not_contains": ["SELECT *"],
            "valid":            True,
            "row_count":        5,
            "metric":           "gmv",
            "confidence":       "HIGH",
            "no_clarification": True,
        }
    },
    {
        "name":     "Average order value by category",
        "body":     {"question": "Average order value by category", "session_id": "http_test_3"},
        "checks": {
            "has_sql":          True,
            "sql_contains":     ["category", "products", "order_items", "NULLIF"],
            "sql_not_contains": ["SELECT *"],
            "valid":            True,
            "has_rows":         True,
            "metric":           "avg_order_value",
            "no_clarification": True,
        }
    },
    {
        "name":     "Compare GMV this month vs last month",
        "body":     {"question": "Compare GMV this month vs last month", "session_id": "http_test_4"},
        "checks": {
            "has_sql":          True,
            "sql_contains":     ["month", "order_date"],
            "valid":            True,
            "row_count":        2,
            "no_clarification": True,
        }
    },
    {
        "name":     "Ambiguous: top products (must clarify)",
        "body":     {"question": "top products", "session_id": "http_test_5"},
        "checks": {
            "has_sql":          False,
            "clarification":    True,
            "confidence":       "LOW",
        }
    },
    {
        "name":     "Ambiguous: show me data (must clarify)",
        "body":     {"question": "show me data", "session_id": "http_test_6"},
        "checks": {
            "has_sql":          False,
            "clarification":    True,
            "confidence":       "LOW",
        }
    },
    {
        "name":     "Ambiguous: Compare this month vs last month (no metric)",
        "body":     {"question": "Compare this month vs last month", "session_id": "http_test_7"},
        "checks": {
            "has_sql":          False,
            "clarification":    True,
            "confidence":       "LOW",
        }
    },
    {
        "name":     "Return rate last 30 days",
        "body":     {"question": "return rate last 30 days", "session_id": "http_test_8"},
        "checks": {
            "has_sql":          True,
            "sql_contains":     ["returns", "order_items"],
            "valid":            True,
            "metric":           "return_rate",
            "no_clarification": True,
        }
    },
    {
        "name":     "skip_insight=True (faster response)",
        "body":     {"question": "GMV this year", "session_id": "http_test_9", "skip_insight": True},
        "checks": {
            "has_sql":          True,
            "valid":            True,
            "no_clarification": True,
        }
    },
]

for test in query_tests:
    print(f"\n  [{test['name']}]")
    t0 = time.time()
    status, data = post("/api/query", test["body"])
    elapsed = round(time.time() - t0, 1)

    check(f"  Status 200",              status == 200, f"got {status}, body={str(data)[:100]}")
    if status != 200:
        continue

    c = test["checks"]
    sql = data.get("sql", "")
    sql_upper = sql.upper()

    if c.get("has_sql"):
        check(f"  SQL generated",       bool(sql), f"sql='{sql[:60]}'")
    else:
        check(f"  No SQL (clarification expected)", not bool(sql), f"sql='{sql[:60]}'")

    if c.get("clarification"):
        check(f"  clarification_needed=True",
              data.get("clarification_needed") is True,
              f"got: {data.get('clarification_needed')}")
        check(f"  note is non-empty",   bool(data.get("note")))
        print(f"  {INFO} note: {data.get('note','')[:80]}")

    if c.get("no_clarification"):
        check(f"  clarification_needed=False",
              data.get("clarification_needed") is False,
              f"got: {data.get('clarification_needed')}")

    if c.get("valid") and sql:
        check(f"  validation.valid=True",
              data.get("validation", {}).get("valid") is True,
              str(data.get("validation", {}).get("errors", [])))

    for kw in c.get("sql_contains", []):
        check(f"  SQL has '{kw}'",      kw.upper() in sql_upper)

    for kw in c.get("sql_not_contains", []):
        check(f"  SQL no '{kw}'",       kw.upper() not in sql_upper)

    if "row_count" in c:
        check(f"  row_count={c['row_count']}",
              data.get("row_count") == c["row_count"],
              f"got {data.get('row_count')}")

    if c.get("has_rows"):
        check(f"  has rows",            data.get("row_count", 0) > 0,
              f"got {data.get('row_count')}")

    if "metric" in c:
        got_metric = (data.get("metric") or {}).get("metric")
        check(f"  metric={c['metric']}", got_metric == c["metric"],
              f"got {got_metric}")

    if "confidence" in c:
        check(f"  confidence={c['confidence']}",
              data.get("confidence") == c["confidence"],
              f"got {data.get('confidence')}")

    if "confidence_not" in c:
        check(f"  confidence != {c['confidence_not']}",
              data.get("confidence") != c["confidence_not"],
              f"got {data.get('confidence')}")

    print(f"  {INFO} confidence={data.get('confidence')} | rows={data.get('row_count')} | {elapsed}s")
    if data.get("insight"):
        print(f"  {INFO} insight: {data['insight'][:100]}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("ENDPOINT: POST /api/query — follow-up memory test")
print(SEP)

SESSION = "http_memory_test"

print("\n  Turn 1: GMV this year")
status, r1 = post("/api/query", {"question": "GMV this year", "session_id": SESSION, "skip_insight": True})
check("  Turn 1 status 200",           status == 200)
check("  Turn 1 SQL valid",            r1.get("validation", {}).get("valid") is True)
check("  Turn 1 metric=gmv",           (r1.get("metric") or {}).get("metric") == "gmv")

print("\n  Turn 2: same for Delhi")
status, r2 = post("/api/query", {"question": "same for Delhi", "session_id": SESSION, "skip_insight": True})
check("  Turn 2 status 200",           status == 200)
check("  Turn 2 is_followup=True",     r2.get("intent", {}).get("is_followup") is True)
check("  Turn 2 inherits metric=gmv",  (r2.get("metric") or {}).get("metric") == "gmv")
check("  Turn 2 SQL has Delhi",        "Delhi" in r2.get("sql", ""))
check("  Turn 2 SQL valid",            r2.get("validation", {}).get("valid") is True)
print(f"  {INFO} SQL: {r2.get('sql','')[:120]}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("ENDPOINT: GET /api/session/{id}")
print(SEP)

status, data = get(f"/api/session/{SESSION}")
check("Status 200",                    status == 200)
check("Has history",                   len(data.get("history", [])) >= 2,
      f"got {len(data.get('history', []))} turns")
check("turn_count >= 2",               data.get("turn_count", 0) >= 2)
check("First turn has query",          bool(data["history"][0].get("query")) if data.get("history") else False)
check("First turn has sql",            bool(data["history"][0].get("sql")) if data.get("history") else False)
print(f"  {INFO} Session has {data.get('turn_count')} turns")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("ENDPOINT: DELETE /api/session/{id}")
print(SEP)

status, data = delete(f"/api/session/{SESSION}")
check("Status 200",                    status == 200)
check("Cleared session id returned",   data.get("cleared") == SESSION)

# Verify it's actually cleared
status, data = get(f"/api/session/{SESSION}")
check("Session is empty after clear",  data.get("turn_count", 0) == 0)

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("ENDPOINT: GET /api/alerts")
print(SEP)

status, data = get("/api/alerts")
check("Status 200",                    status == 200)
check("Has 'alerts' key",              "alerts" in data)
check("Has 'count' key",               "count" in data)
print(f"  {INFO} Active alerts: {data.get('count', 0)}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("ENDPOINT: POST /api/visualize")
print(SEP)

viz_body = {
    "selections": [
        {"table": "orders", "columns": ["order_date", "order_status"]},
        {"table": "order_items", "columns": ["item_price"]}
    ],
    "limit": 100
}
status, data = post("/api/visualize", viz_body)
check("Status 200",                    status == 200)
check("Has rows",                      data.get("count", 0) > 0, f"count={data.get('count')}")
check("Has recommendation",            "recommendation" in data)
check("Has best chart type",           data.get("recommendation", {}).get("best") is not None)
print(f"  {INFO} Rows: {data.get('count')} | Chart: {data.get('recommendation',{}).get('best',{}).get('type')}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("ENDPOINT: POST /api/recommend")
print(SEP)

rec_body = {
    "columns": [
        {"name": "city", "dtype": "text"},
        {"name": "gmv",  "dtype": "numeric"}
    ],
    "sample_data": [
        {"city": "Delhi",  "gmv": 1609405},
        {"city": "Mumbai", "gmv": 1601976}
    ]
}
status, data = post("/api/recommend", rec_body)
check("Status 200",                    status == 200)
check("Has rule_based",                "rule_based" in data)
check("Has best",                      "best" in data)
check("Best chart type is bar",        data.get("best") == "bar",
      f"got {data.get('best')}")
print(f"  {INFO} Recommended: {data.get('best')}")

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("RESPONSE STRUCTURE AUDIT")
print(SEP)

print("\n  [Checking all required fields in /api/query response]")
status, data = post("/api/query", {"question": "GMV this year", "session_id": "struct_test", "skip_insight": True})
required_fields = ["sql", "reasoning", "confidence", "insight", "rows",
                   "row_count", "chunks_used", "validation", "metric",
                   "stats", "intent", "plan", "session_id",
                   "clarification_needed", "note"]
for field in required_fields:
    check(f"  Has '{field}' field",    field in data, f"keys: {list(data.keys())}")

check("  confidence is HIGH/MEDIUM/LOW",
      data.get("confidence") in ["HIGH", "MEDIUM", "LOW"])
check("  validation has 'valid' key",
      "valid" in data.get("validation", {}))
check("  validation has 'errors' key",
      "errors" in data.get("validation", {}))
check("  stats has 'row_count'",
      "row_count" in data.get("stats", {}))

# ════════════════════════════════════════════════════════════════════════════
print(f"\n{SEP}")
if failures:
    print(f"FAILED: {len(failures)} checks failed:")
    for f in failures:
        print(f"  - {f}")
else:
    print("ALL BACKEND CHECKS PASSED")
print(SEP)
