# core/semantic_engine.py
import yaml
import os

_YAML_PATH = os.path.join(os.path.dirname(__file__), "..", "yaml", "metric_glossary.yaml")


def _load() -> dict:
    with open(_YAML_PATH) as f:
        return yaml.safe_load(f) or {}


_METRICS = _load()

# Build alias map: "gross merchandise value" → "gmv", "net revenue" → "revenue_net", etc.
_ALIASES = {
    "quantity sold": "quantity_sold",
    "units sold": "quantity_sold",
    "total quantity": "quantity_sold",
    "quantity": "quantity_sold",
    "units": "quantity_sold",
    "items sold": "quantity_sold",
    "gmv": "gmv",
    "gross merchandise value": "gmv",
    "gross revenue": "gmv",
    "total revenue": "gmv",
    "revenue": "gmv",
    "net revenue": "revenue_net",
    "revenue net": "revenue_net",
    "revenue after returns": "revenue_net",
    "order count": "order_count",
    "number of orders": "order_count",
    "total orders": "order_count",
    "how many orders": "order_count",
    "count of orders": "order_count",
    "return rate": "return_rate",
    "returns": "return_rate",
    "aov": "avg_order_value",
    "average order value": "avg_order_value",
    "avg order value": "avg_order_value",
    "active sellers": "active_sellers",
    "customer count": "customer_count",
    "unique customers": "customer_count",
    "number of customers": "customer_count",
    "low inventory": "inventory_low",
    "stock low": "inventory_low",
    "delivery time": "delivery_time_avg",
    "avg delivery": "delivery_time_avg",
    "average delivery": "delivery_time_avg",
}


def resolve_metric(user_query: str) -> dict | None:
    """
    Returns metric definition from glossary if query references a known metric.
    Checks aliases first (exact phrases), then metric key substrings.
    """
    q = user_query.lower()

    # Check aliases (longest match first to avoid partial hits)
    for phrase in sorted(_ALIASES, key=len, reverse=True):
        if phrase in q:
            key = _ALIASES[phrase]
            if key in _METRICS:
                return _build(key, _METRICS[key])

    # Fallback: direct key match
    for key, defn in _METRICS.items():
        if key.replace("_", " ") in q or key in q:
            return _build(key, defn)

    return None


def _build(key: str, defn: dict) -> dict:
    fl = defn.get("formula_logic", {})
    return {
        "metric": key,
        "description": defn.get("description", ""),
        "aggregation": fl.get("aggregation", ""),
        "filter": fl.get("filter", ""),
        "tables": defn.get("tables", []),
    }


def enforce_metric(metric: dict | None) -> str:
    """Returns a strict instruction block for the LLM prompt, or empty string."""
    if not metric:
        return ""
    return (
        f"MANDATORY METRIC RULE — use EXACTLY this, no approximation:\n"
        f"  Metric     : {metric['metric']}\n"
        f"  Aggregation: {metric['aggregation']}\n"
        f"  Filter     : {metric['filter']}\n"
        f"  Tables     : {', '.join(metric['tables'])}\n"
    )
