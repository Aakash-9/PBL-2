# test_hardening.py
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from dotenv import load_dotenv
load_dotenv()

SEP = "-" * 70
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

# ── safety.py ────────────────────────────────────────────────────────────────
print(f"\n{SEP}\nMODULE: safety\n{SEP}")
from core.safety import enforce

check("DROP TABLE blocked",        not enforce("DROP TABLE orders")["safe"])
check("TRUNCATE blocked",          not enforce("TRUNCATE orders")["safe"])
check("DELETE blocked",            not enforce("DELETE FROM orders WHERE 1=1")["safe"])
check("INSERT blocked",            not enforce("INSERT INTO orders VALUES (1)")["safe"])
check("UPDATE blocked",            not enforce("UPDATE orders SET status='x' WHERE 1=1")["safe"])
check("Empty SQL blocked",         not enforce("")["safe"])

s = enforce("SELECT o.order_id FROM orders o")
check("LIMIT injected when missing",  "LIMIT 1000" in s["sql"] and s["safe"])

s2 = enforce("SELECT o.order_id FROM orders o LIMIT 9999")
check("LIMIT 9999 clamped to 1000",   "LIMIT 1000" in s2["sql"])

s3 = enforce("SELECT o.order_id FROM orders o LIMIT 500")
check("LIMIT 500 preserved",          "LIMIT 500" in s3["sql"])

s4 = enforce("SELECT o.order_id FROM orders o LIMIT 5")
check("LIMIT 5 preserved",            "LIMIT 5" in s4["sql"])

s5 = enforce("SELECT o.order_id FROM orders o;")
check("Trailing semicolon stripped",  not s5["sql"].endswith(";") and s5["safe"])

# ── intent_parser ambiguity ──────────────────────────────────────────────────
print(f"\n{SEP}\nMODULE: intent_parser ambiguity\n{SEP}")
from core.intent_parser import parse

i = parse("show me some data")
check("Vague query -> ambiguity=too_vague",          i["ambiguity"] == "too_vague")

i = parse("top products")
check("top products -> ambiguity=top_products_no_metric", i["ambiguity"] == "top_products_no_metric")

i = parse("Compare this month vs last month")
check("Compare no metric -> ambiguity=compare_no_metric", i["ambiguity"] == "compare_no_metric")

i = parse("GMV last month")
check("GMV last month -> no ambiguity",              i["ambiguity"] is None)

i = parse("Top 5 cities by revenue")
check("Top 5 cities by revenue -> no ambiguity",     i["ambiguity"] is None)

i = parse("Average order value by category")
check("AOV by category -> no ambiguity",             i["ambiguity"] is None)

# Follow-up should not be flagged as ambiguous
i = parse("same as before but for Pune", last_context={
    "metric": "gmv", "time_filter": "last_month", "dimension": None, "filters": {}
})
check("Follow-up -> no ambiguity",                   i["ambiguity"] is None)

# ── agent: clarification responses ──────────────────────────────────────────
print(f"\n{SEP}\nMODULE: agent clarification (no LLM calls)\n{SEP}")
from core.agent import _clarification_response, _compute_confidence

r = _clarification_response("s1", {"ambiguity": "compare_no_metric", "operation": "compare"}, "test msg")
check("Clarification response has clarification_needed=True", r["clarification_needed"] is True)
check("Clarification response has empty SQL",                  r["sql"] == "")
check("Clarification response has LOW confidence",             r["confidence"] == "LOW")
check("Clarification response has note",                       r["note"] == "test msg")
check("Clarification response has empty rows",                 r["rows"] == [])

# ── confidence scoring ───────────────────────────────────────────────────────
print(f"\n{SEP}\nMODULE: confidence scoring\n{SEP}")

valid   = {"valid": True,  "errors": [], "warnings": []}
invalid = {"valid": False, "errors": ["err"], "warnings": []}

check("Invalid SQL -> LOW",
    _compute_confidence(invalid, 1, 10, {"time_filter": "last_month", "operation": "aggregate"}) == "LOW")

check("Valid, 1 attempt, rows, time_filter -> HIGH",
    _compute_confidence(valid, 1, 10, {"time_filter": "last_month", "operation": "aggregate"}) == "HIGH")

check("Valid, 2 attempts -> MEDIUM",
    _compute_confidence(valid, 2, 10, {"time_filter": "last_month", "operation": "aggregate"}) == "MEDIUM")

check("Valid, 0 rows -> MEDIUM",
    _compute_confidence(valid, 1, 0, {"time_filter": "last_month", "operation": "aggregate"}) == "MEDIUM")

check("Valid, no time_filter, aggregate -> MEDIUM",
    _compute_confidence(valid, 1, 10, {"time_filter": None, "operation": "aggregate"}) == "MEDIUM")

check("Valid, no time_filter, top_n -> HIGH",
    _compute_confidence(valid, 1, 10, {"time_filter": None, "operation": "top_n"}) == "HIGH")

# ── logger ───────────────────────────────────────────────────────────────────
print(f"\n{SEP}\nMODULE: logger\n{SEP}")
from core.logger import log as agent_log
import os

log_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", "agent_logs.txt")
size_before = os.path.getsize(log_path) if os.path.exists(log_path) else 0

agent_log(
    session_id="test_session",
    question="GMV last month",
    intent={"metric": "gmv", "time_filter": "last_month", "dimension": None, "operation": "aggregate"},
    plan={"primary_table": "orders"},
    sql="SELECT SUM(oi.item_price) AS gmv FROM orders o JOIN order_items oi ON o.order_id=oi.order_id",
    validation={"valid": True, "errors": [], "warnings": []},
    exec_result={"count": 1, "success": True},
    confidence="HIGH",
    insight="GMV was X",
    note="",
)

size_after = os.path.getsize(log_path) if os.path.exists(log_path) else 0
check("Log file written",          size_after > size_before)
check("Log file exists",           os.path.exists(log_path))

# Verify log is valid JSON
import json
with open(log_path, encoding="utf-8") as f:
    lines = [l.strip() for l in f.readlines() if l.strip()]
last_line = lines[-1] if lines else ""
try:
    parsed = json.loads(last_line)
    check("Last log entry is valid JSON",     True)
    check("Log has session_id field",         "session_id" in parsed)
    check("Log has question field",           "question" in parsed)
    check("Log has sql field",                "sql" in parsed)
    check("Log has confidence field",         "confidence" in parsed)
    check("Log has valid field",              "valid" in parsed)
    check("Log has rows field",               "rows" in parsed)
    check("Log confidence=HIGH",              parsed["confidence"] == "HIGH")
except json.JSONDecodeError as e:
    check("Last log entry is valid JSON", False, str(e))

# ── integration: agent fail-safe responses (live) ───────────────────────────
print(f"\n{SEP}\nINTEGRATION: agent fail-safes (live)\n{SEP}")
from core.agent import run

print("\n  Case 1: 'Compare this month vs last month' (no metric)")
r = run("Compare this month vs last month", session_id="fs1", skip_insight=True)
check("  Returns clarification",       r.get("clarification_needed") is True)
check("  No SQL generated",            r["sql"] == "")
check("  Confidence=LOW",              r["confidence"] == "LOW")
check("  Note is helpful message",     "compare" in r.get("note","").lower() or "metric" in r.get("note","").lower())
print(f"  {INFO} note: {r.get('note','')}")

print("\n  Case 2: 'top products' (ambiguous)")
r2 = run("top products", session_id="fs2", skip_insight=True)
check("  Returns clarification",       r2.get("clarification_needed") is True)
check("  No SQL generated",            r2["sql"] == "")
check("  Note mentions revenue/quantity", any(w in r2.get("note","").lower() for w in ["revenue","quantity","margin"]))
print(f"  {INFO} note: {r2.get('note','')}")

print("\n  Case 3: 'show me some data' (too vague)")
r3 = run("show me some data", session_id="fs3", skip_insight=True)
check("  Returns clarification",       r3.get("clarification_needed") is True)
check("  No SQL generated",            r3["sql"] == "")
print(f"  {INFO} note: {r3.get('note','')}")

print("\n  Case 4: 'GMV last month' (valid — should NOT trigger clarification)")
r4 = run("GMV last month", session_id="fs4", skip_insight=True)
check("  No clarification needed",     r4.get("clarification_needed") is False)
check("  SQL generated",               bool(r4["sql"]))
check("  Validation passed",           r4["validation"]["valid"])
check("  Confidence not LOW",          r4["confidence"] != "LOW")
print(f"  {INFO} confidence: {r4['confidence']}")
print(f"  {INFO} note: {r4.get('note','')}")

print("\n  Case 5: 'Top 5 cities by revenue' (valid — HIGH confidence expected)")
r5 = run("Top 5 cities by revenue", session_id="fs5", skip_insight=True)
check("  No clarification needed",     r5.get("clarification_needed") is False)
check("  SQL generated",               bool(r5["sql"]))
check("  Validation passed",           r5["validation"]["valid"])
check("  Confidence is HIGH",          r5["confidence"] == "HIGH")
check("  note is empty (clean data)",  r5.get("note","") == "" or "No data" in r5.get("note",""))
print(f"  {INFO} confidence: {r5['confidence']}")

# ── summary ──────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
if failures:
    print(f"FAILED: {len(failures)} checks failed:")
    for f in failures:
        print(f"  - {f}")
else:
    print("ALL HARDENING CHECKS PASSED")
print(SEP)
