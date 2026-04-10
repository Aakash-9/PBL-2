# core/metrics_engine.py
"""
Pre-computes structured stats from raw query result rows.
Passes numbers to insight LLM instead of raw data — eliminates hallucinated math.
"""
from typing import Any


def compute(rows: list[dict], metric_info: dict | None = None) -> dict:
    """
    Returns structured stats dict:
    {
        "total": float | None,
        "row_count": int,
        "top_contributors": [...],   # top 3 rows by numeric value
        "growth_pct": float | None,  # only if exactly 2 rows (period comparison)
        "anomaly": str | None,       # flags extreme outliers
        "numeric_col": str | None,
        "label_col": str | None,
    }
    """
    if not rows:
        return {"row_count": 0, "total": None, "top_contributors": [],
                "growth_pct": None, "anomaly": None, "numeric_col": None, "label_col": None}

    numeric_col, label_col = _detect_cols(rows[0])

    total = None
    top_contributors = []
    growth_pct = None
    anomaly = None

    if numeric_col:
        values = [_to_float(r.get(numeric_col)) for r in rows]
        values = [v for v in values if v is not None]

        if values:
            total = round(sum(values), 2)
            sorted_rows = []

            # Top contributors only meaningful when there are multiple labelled rows
            if label_col and len(rows) > 1:
                sorted_rows = sorted(rows, key=lambda r: _to_float(r.get(numeric_col)) or 0, reverse=True)
                top_contributors = [
                    {
                        "label": r.get(label_col, f"row_{i}"),
                        "value": round(_to_float(r.get(numeric_col)) or 0, 2),
                        "share_pct": round((_to_float(r.get(numeric_col)) or 0) / total * 100, 1) if total else 0,
                    }
                    for i, r in enumerate(sorted_rows[:3])
                ]

            # Growth % — only meaningful for exactly 2 rows (period-over-period)
            if len(values) == 2:
                prev, curr = values[0], values[1]
                if prev and prev != 0:
                    growth_pct = round((curr - prev) / abs(prev) * 100, 1)

            # Anomaly: top value is >3x the average of the rest
            if len(values) > 2:
                top_val = max(values)
                rest_avg = (sum(values) - top_val) / (len(values) - 1)
                if rest_avg > 0 and top_val > rest_avg * 3:
                    top_label = sorted_rows[0].get(label_col, "unknown") if sorted_rows and label_col else "unknown"
                    anomaly = f"{top_label} is an outlier — {round(top_val / rest_avg, 1)}x above average"

    return {
        "row_count": len(rows),
        "total": total,
        "top_contributors": top_contributors,
        "growth_pct": growth_pct,
        "anomaly": anomaly,
        "numeric_col": numeric_col,
        "label_col": label_col,
        "_raw_rows": rows,  # passed to insight_engine for compare prompt
    }


def _detect_cols(row: dict) -> tuple[str | None, str | None]:
    """Heuristically picks the primary numeric and label columns."""
    # Known metric columns in priority order — pick the most meaningful one
    _METRIC_PRIORITY = [
        "gmv", "revenue", "revenue_lost_inr", "total_revenue_lost", "refund_amount",
        "avg_order_value", "aov", "order_count", "quantity_sold",
        "active_sellers", "customer_count", "revenue_net",
    ]
    # Rate/percentage columns — treat as secondary, not primary
    _RATE_COLS = {"return_rate", "return_rate_pct", "growth_pct", "share_pct", "rate", "pct"}

    numeric_col = None
    label_col = None
    fallback_numeric = None  # first numeric col if no priority match

    for k, v in row.items():
        if isinstance(v, str) and label_col is None:
            label_col = k
        elif isinstance(v, (int, float)):
            k_lower = k.lower()
            # Skip rate/percentage columns as primary metric
            if any(rate in k_lower for rate in _RATE_COLS):
                if fallback_numeric is None:
                    fallback_numeric = k
                continue
            # Check priority list
            if numeric_col is None:
                for priority in _METRIC_PRIORITY:
                    if priority in k_lower:
                        numeric_col = k
                        break
            # First non-rate numeric as fallback
            if fallback_numeric is None:
                fallback_numeric = k

    # If no priority match found, use first numeric col
    if numeric_col is None:
        numeric_col = fallback_numeric

    return numeric_col, label_col


def _to_float(val: Any) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
