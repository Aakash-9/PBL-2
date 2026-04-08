import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dotenv import load_dotenv; load_dotenv()
from core.data_validator import verify
from core.supabase_client import execute_sql
from core.agent import run

SEP  = "-" * 65
PASS = "[PASS]"
FAIL = "[FAIL]"
INFO = "[INFO]"
failures = []

def check(label, condition, detail=""):
    if condition:
        print(f"  {PASS} {label}")
    else:
        print(f"  {FAIL} {label}" + (f" -- {detail}" if detail else ""))
        failures.append(label)

# ── Unit test data_validator directly ───────────────────────────────────────
print(f"\n{SEP}\nDATA VALIDATOR: direct unit tests\n{SEP}")

# Test 1: correct GMV result should verify
gmv_rows = execute_sql("""
    SELECT COALESCE(SUM(oi.item_price),0) AS gmv
    FROM orders o JOIN order_items oi ON o.order_id=oi.order_id
    WHERE o.order_status IN ('Delivered','Shipped')
    AND date_trunc('year',o.order_date)=date_trunc('year',current_date)
""")["rows"]

intent_gmv = {
    "metric": {"metric":"gmv","aggregation":"SUM(oi.item_price)",
               "filter":"order_status IN ('Delivered', 'Shipped')","tables":["orders","order_items"]},
    "time_filter": "this_year", "dimension": None, "filters": {}, "operation": "aggregate"
}
dv = verify(gmv_rows, intent_gmv, "")
check("Correct GMV result verifies",       dv["verified"] and not dv["mismatch"],
      f"agent={dv['agent_value']} verified={dv['verified_value']}")
check("delta_pct < 0.01%",                 (dv["delta_pct"] or 0) < 0.01,
      f"delta={dv['delta_pct']}")
print(f"  {INFO} GMV this year: agent={dv['agent_value']} verified={dv['verified_value']}")

# Test 2: wrong result should be caught
wrong_rows = [{"gmv": 999999999.0}]
dv2 = verify(wrong_rows, intent_gmv, "")
check("Wrong GMV result caught as mismatch", dv2["mismatch"],
      f"agent={dv2['agent_value']} verified={dv2['verified_value']}")
check("Mismatch note is non-empty",          bool(dv2["note"]))
print(f"  {INFO} Mismatch note: {dv2['note'][:80]}")

# Test 3: top cities verification
cities_rows = execute_sql("""
    SELECT c.city, COALESCE(SUM(oi.item_price),0) AS gmv
    FROM orders o JOIN order_items oi ON o.order_id=oi.order_id
    JOIN customers c ON o.customer_id=c.customer_id
    WHERE o.order_status IN ('Delivered','Shipped')
    GROUP BY c.city ORDER BY gmv DESC LIMIT 5
""")["rows"]

intent_cities = {
    "metric": {"metric":"gmv","aggregation":"SUM(oi.item_price)",
               "filter":"order_status IN ('Delivered', 'Shipped')","tables":["orders","order_items"]},
    "time_filter": None,
    "dimension": {"table":"customers","column":"city"},
    "filters": {}, "operation": "top_n"
}
dv3 = verify(cities_rows, intent_cities, "")
check("Correct top cities result verifies",  dv3["verified"] and not dv3["mismatch"],
      f"agent_top={dv3['agent_value']} verified_top={dv3['verified_value']}")
print(f"  {INFO} Top city: agent={dv3['agent_value']} verified={dv3['verified_value']}")

# Test 4: compare operation is skipped (not verifiable simply)
intent_compare = {"metric": None, "time_filter": None, "dimension": None,
                  "filters": {}, "operation": "compare"}
dv4 = verify([{"period":"this_month","gmv":0}], intent_compare, "")
check("Compare operation skipped gracefully", dv4["skipped"])

# Test 5: no metric skipped gracefully
intent_no_metric = {"metric": None, "time_filter": "this_year", "dimension": None,
                    "filters": {}, "operation": "aggregate"}
dv5 = verify([{"order_count": 504}], intent_no_metric, "")
check("No metric skipped gracefully",         dv5["skipped"])

# ── Integration: agent pipeline with data_validation field ──────────────────
print(f"\n{SEP}\nINTEGRATION: agent returns data_validation field\n{SEP}")

queries = [
    ("GMV this year",              True,  False),
    ("Top 5 cities by revenue",    True,  False),
    ("return rate last 30 days",   True,  False),
    ("active sellers this year",   True,  False),
]

for q, expect_verified, expect_mismatch in queries:
    print(f"\n  [{q}]")
    r = run(q, session_id=f"dv_{q[:8]}", skip_insight=True)
    dv = r.get("data_validation", {})

    check("  data_validation field present",   "data_validation" in r)
    check("  has verified field",              "verified" in dv)
    check("  has mismatch field",              "mismatch" in dv)
    check("  no mismatch on correct query",    not dv.get("mismatch", True),
          f"note: {dv.get('note','')}")

    if not dv.get("skipped"):
        print(f"  {INFO} agent={dv.get('agent_value')} verified={dv.get('verified_value')} delta={dv.get('delta_pct')}%")
    else:
        print(f"  {INFO} skipped: {dv.get('skip_reason','')}")

    print(f"  {INFO} confidence: {r['confidence']}")

# ── Verify mismatch triggers confidence=LOW ──────────────────────────────────
print(f"\n{SEP}\nMISMATCH HANDLING\n{SEP}")

# Manually inject a mismatch scenario by patching verify temporarily
from unittest.mock import patch

mock_dv = {"verified": False, "skipped": False, "mismatch": True,
           "agent_value": 999, "verified_value": 100,
           "delta_pct": 890.0, "note": "Data mismatch: agent=999, verified=100"}

with patch("core.agent.data_verify", return_value=mock_dv):
    r_bad = run("GMV this year", session_id="mismatch_test", skip_insight=True)

check("Mismatch -> confidence=LOW",        r_bad["confidence"] == "LOW",
      f"got: {r_bad['confidence']}")
check("Mismatch -> note contains mismatch info",
      "mismatch" in r_bad.get("note","").lower() or "999" in r_bad.get("note",""),
      f"note: {r_bad.get('note','')}")
check("Mismatch -> insight suppressed",    r_bad.get("insight","") == "")

print(f"\n{SEP}")
if failures:
    print(f"FAILED: {len(failures)} checks:")
    for f in failures:
        print(f"  - {f}")
else:
    print("ALL DATA VALIDATION CHECKS PASSED")
print(SEP)
