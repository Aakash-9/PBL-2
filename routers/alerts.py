# routers/alerts.py
from fastapi import APIRouter, BackgroundTasks
from datetime import datetime
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from core.supabase_client import execute_sql

router = APIRouter()

_alerts = []
_last_checked = None

# Each check runs a real query and generates an alert if the result is noteworthy
WATCH_QUERIES = [
    {
        "id": "gmv_today",
        "name": "Today's GMV",
        "metric": "gmv",
        "sql": """
            SELECT COALESCE(SUM(oi.item_price), 0) AS value
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.order_date >= current_date
              AND o.order_status IN ('Delivered', 'Shipped')
        """,
        "baseline_sql": """
            SELECT COALESCE(SUM(oi.item_price), 0) / 7 AS value
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.order_date >= current_date - interval '7 days'
              AND o.order_date < current_date
              AND o.order_status IN ('Delivered', 'Shipped')
        """,
        "direction": "below",
        "threshold_pct": 0.6,
        "suggested_query": "What is the GMV breakdown by category today?",
    },
    {
        "id": "returns_today",
        "name": "Return spike today",
        "metric": "returns",
        "sql": "SELECT COUNT(*) AS value FROM returns WHERE return_date >= current_date",
        "baseline_sql": """
            SELECT COALESCE(COUNT(*), 0) / 7 AS value
            FROM returns
            WHERE return_date >= current_date - interval '7 days'
              AND return_date < current_date
        """,
        "direction": "above",
        "threshold_pct": 1.5,
        "suggested_query": "Which categories had the most returns today?",
    },
    {
        "id": "cancelled_orders",
        "name": "High cancellation rate",
        "metric": "cancellations",
        "sql": """
            SELECT ROUND(
                COUNT(*) FILTER (WHERE order_status = 'Cancelled') * 100.0
                / NULLIF(COUNT(*), 0), 1
            ) AS value
            FROM orders
            WHERE order_date >= current_date - interval '7 days'
        """,
        "baseline_sql": None,  # absolute threshold
        "absolute_threshold": 15,  # alert if cancellation rate > 15%
        "direction": "above",
        "threshold_pct": 1.0,
        "suggested_query": "What are the top reasons for order cancellations this week?",
    },
    {
        "id": "low_inventory",
        "name": "Low stock items",
        "metric": "inventory",
        "sql": "SELECT COUNT(*) AS value FROM inventory WHERE available_qty < 10 AND available_qty >= 0",
        "baseline_sql": None,
        "absolute_threshold": 5,  # alert if more than 5 items are low stock
        "direction": "above",
        "threshold_pct": 1.0,
        "suggested_query": "Which products are running low on inventory?",
    },
    {
        "id": "pending_settlements",
        "name": "Pending seller settlements",
        "metric": "settlements",
        "sql": """
            SELECT COUNT(*) AS value
            FROM seller_settlements
            WHERE settlement_status = 'Pending'
              AND settlement_date <= current_date - interval '3 days'
        """,
        "baseline_sql": None,
        "absolute_threshold": 10,
        "direction": "above",
        "threshold_pct": 1.0,
        "suggested_query": "Which sellers have pending settlements older than 3 days?",
    },
]


async def _run_checks():
    global _alerts, _last_checked
    new_alerts = []

    for watch in WATCH_QUERIES:
        try:
            result = execute_sql(watch["sql"].strip())
            if not result["success"] or not result["rows"]:
                continue

            value = float(result["rows"][0].get("value", 0) or 0)

            # Determine baseline
            if watch.get("baseline_sql"):
                b_result = execute_sql(watch["baseline_sql"].strip())
                if not b_result["success"] or not b_result["rows"]:
                    continue
                baseline = float(b_result["rows"][0].get("value", 0) or 0)
                if baseline == 0:
                    continue  # can't compare against zero baseline
            elif watch.get("absolute_threshold") is not None:
                baseline = float(watch["absolute_threshold"])
            else:
                continue

            triggered = False
            if watch["direction"] == "below" and value < baseline * watch["threshold_pct"]:
                triggered = True
            elif watch["direction"] == "above" and value > baseline * watch["threshold_pct"]:
                triggered = True

            if triggered:
                pct_diff = abs(value - baseline) / max(baseline, 1)
                new_alerts.append({
                    "id": f"{watch['id']}_{datetime.now().strftime('%Y%m%d%H')}",
                    "name": watch["name"],
                    "metric": watch["metric"],
                    "value": round(value, 1),
                    "baseline": round(baseline, 1),
                    "direction": watch["direction"],
                    "ts": datetime.now().isoformat(),
                    "suggested_query": watch["suggested_query"],
                    "severity": "high" if pct_diff > 0.3 else "medium",
                })
        except Exception:
            continue

    # Merge: keep existing dismissed alerts out, add new ones deduped by id prefix
    existing_ids = {a["id"].rsplit("_", 1)[0] for a in _alerts}
    for alert in new_alerts:
        prefix = alert["id"].rsplit("_", 1)[0]
        if prefix not in existing_ids:
            _alerts.insert(0, alert)

    _alerts = _alerts[:50]
    _last_checked = datetime.now().isoformat()


@router.get("/alerts")
async def get_alerts(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_checks)
    return {"alerts": _alerts, "count": len(_alerts), "last_checked": _last_checked}


@router.delete("/alerts/{alert_id}")
async def dismiss_alert(alert_id: str):
    global _alerts
    _alerts = [a for a in _alerts if a["id"] != alert_id]
    return {"dismissed": alert_id}
