# routers/alerts.py
from fastapi import APIRouter, BackgroundTasks
from datetime import datetime
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from core.supabase_client import execute_sql

router = APIRouter()

# In-memory alert store (swap for Redis in production)
_alerts = []
_baselines = {}

WATCH_QUERIES = [
    {
        "id": "gmv_daily",
        "name": "Daily GMV",
        "metric": "gmv",
        "sql": "SELECT COALESCE(SUM(oi.item_price),0) as value FROM orders o JOIN order_items oi ON o.order_id=oi.order_id WHERE o.order_date >= current_date - interval '1 day' AND o.order_status IN ('Delivered','Shipped')",
        "threshold_pct": 0.75,
        "direction": "below",
    },
    {
        "id": "return_rate_daily",
        "name": "Return spike",
        "metric": "returns",
        "sql": "SELECT COUNT(*) as value FROM returns WHERE return_date >= current_date - interval '1 day'",
        "threshold_pct": 1.5,
        "direction": "above",
    },
    {
        "id": "low_inventory",
        "name": "Low inventory items",
        "metric": "inventory",
        "sql": "SELECT COUNT(*) as value FROM inventory WHERE available_qty < 10",
        "threshold_pct": 1.0,
        "direction": "above",
    },
]


async def _run_checks():
    global _alerts, _baselines
    for watch in WATCH_QUERIES:
        result = execute_sql(watch["sql"])
        if not result["success"] or not result["rows"]:
            continue
        value = result["rows"][0].get("value", 0) or 0
        baseline = _baselines.get(watch["id"], value)
        _baselines[watch["id"]] = (baseline * 6 + value) / 7  # rolling avg

        triggered = False
        if watch["direction"] == "below" and value < baseline * watch["threshold_pct"]:
            triggered = True
        elif watch["direction"] == "above" and value > baseline * watch["threshold_pct"]:
            triggered = True

        if triggered:
            _alerts.insert(0, {
                "id": f"{watch['id']}_{datetime.now().isoformat()}",
                "name": watch["name"],
                "metric": watch["metric"],
                "value": value,
                "baseline": baseline,
                "direction": watch["direction"],
                "ts": datetime.now().isoformat(),
                "suggested_query": f"Explain why {watch['name'].lower()} changed significantly today",
                "severity": "high" if abs(value - baseline) / max(baseline, 1) > 0.3 else "medium",
            })

    # Keep only last 50 alerts
    _alerts = _alerts[:50]


@router.get("/alerts")
async def get_alerts(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_checks)
    return {"alerts": _alerts, "count": len(_alerts)}


@router.delete("/alerts/{alert_id}")
async def dismiss_alert(alert_id: str):
    global _alerts
    _alerts = [a for a in _alerts if a["id"] != alert_id]
    return {"dismissed": alert_id}
