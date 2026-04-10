# core/viz_recommender.py
"""
Dtype-aware visualization recommendation engine.
Scores chart types based on selected column types and cardinality.
Works like Power BI's auto-chart suggestion, but explainable.
"""
from typing import List, Dict, Any


NUMERIC_TYPES = {"integer", "bigint", "numeric", "real", "double precision", "float", "decimal", "money"}
DATE_TYPES    = {"date", "timestamp", "timestamp without time zone", "timestamp with time zone", "time"}
TEXT_TYPES    = {"character varying", "varchar", "text", "char", "character", "boolean", "uuid"}


def _classify(dtype: str) -> str:
    dtype = dtype.lower()
    if dtype in NUMERIC_TYPES or dtype == "numeric": return "numeric"
    if dtype in DATE_TYPES or dtype == "date":       return "date"
    return "categorical"


def recommend(columns: List[Dict], sample_data: List[Dict] = None) -> Dict[str, Any]:
    """
    columns: [{"name": "order_date", "dtype": "date"}, ...]
    Returns ordered list of chart recommendations with reasons.
    """
    if not columns:
        return {"recommendations": [], "best": None}

    classified = [{"name": c["name"], "dtype": c["dtype"], "kind": _classify(c["dtype"])} for c in columns]
    kinds = [c["kind"] for c in classified]

    numeric_cols = [c for c in classified if c["kind"] == "numeric"]
    date_cols    = [c for c in classified if c["kind"] == "date"]
    cat_cols     = [c for c in classified if c["kind"] == "categorical"]

    n_num = len(numeric_cols)
    n_date = len(date_cols)
    n_cat = len(cat_cols)
    total = len(columns)

    # Estimate cardinality from sample
    cardinality = {}
    if sample_data:
        for col in classified:
            vals = [r.get(col["name"]) for r in sample_data if r.get(col["name"]) is not None]
            cardinality[col["name"]] = len(set(str(v) for v in vals))

    recommendations = []

    # ── Rule 1: 1 date + 1 numeric → line chart (time series)
    if n_date >= 1 and n_num >= 1:
        recommendations.append({
            "type": "line",
            "score": 95,
            "x": date_cols[0]["name"],
            "y": numeric_cols[0]["name"],
            "color_by": cat_cols[0]["name"] if cat_cols else None,
            "reason": f"Time series: {date_cols[0]['name']} on X-axis, {numeric_cols[0]['name']} as trend line.",
            "icon": "📈",
        })
        if n_num > 1:
            recommendations.append({
                "type": "area",
                "score": 85,
                "x": date_cols[0]["name"],
                "y": numeric_cols[0]["name"],
                "reason": "Area chart works well to show volume over time.",
                "icon": "🏔",
            })

    # ── Rule 2: 1 categorical + 1 numeric → bar chart
    if n_cat >= 1 and n_num >= 1:
        cat = cat_cols[0]["name"]
        car = cardinality.get(cat, 10)
        if car <= 20:
            recommendations.append({
                "type": "bar",
                "score": 90,
                "x": cat,
                "y": numeric_cols[0]["name"],
                "color_by": None,
                "reason": f"Bar chart comparing {numeric_cols[0]['name']} across {cat} categories.",
                "icon": "📊",
            })
        else:
            recommendations.append({
                "type": "bar",
                "score": 70,
                "x": cat,
                "y": numeric_cols[0]["name"],
                "reason": f"High cardinality ({car} values) — consider filtering top N.",
                "icon": "📊",
            })

    # ── Rule 3: 1 categorical + small cardinality → pie / donut
    if n_cat >= 1 and n_num == 1:
        cat = cat_cols[0]["name"]
        car = cardinality.get(cat, 10)
        if car <= 8:
            recommendations.append({
                "type": "pie",
                "score": 78,
                "x": cat,
                "y": numeric_cols[0]["name"],
                "reason": f"Pie chart shows share of {numeric_cols[0]['name']} by {cat} (only {car} slices).",
                "icon": "🥧",
            })

    # ── Rule 4: 2 numeric columns → scatter plot
    if n_num >= 2:
        recommendations.append({
            "type": "scatter",
            "score": 82,
            "x": numeric_cols[0]["name"],
            "y": numeric_cols[1]["name"],
            "color_by": cat_cols[0]["name"] if cat_cols else None,
            "reason": f"Scatter plot reveals correlation between {numeric_cols[0]['name']} and {numeric_cols[1]['name']}.",
            "icon": "🔵",
        })

    # ── Rule 5: 2 categorical + 1 numeric → heatmap
    if n_cat >= 2 and n_num >= 1:
        recommendations.append({
            "type": "heatmap",
            "score": 80,
            "x": cat_cols[0]["name"],
            "y": cat_cols[1]["name"],
            "value": numeric_cols[0]["name"] if numeric_cols else None,
            "reason": f"Heatmap shows {numeric_cols[0]['name'] if numeric_cols else 'count'} density across {cat_cols[0]['name']} × {cat_cols[1]['name']}.",
            "icon": "🌡",
        })

    # ── Rule 6: multiple numerics → combo (bar + line)
    if n_num >= 2 and (n_date >= 1 or n_cat >= 1):
        recommendations.append({
            "type": "combo",
            "score": 75,
            "x": date_cols[0]["name"] if date_cols else cat_cols[0]["name"],
            "y": numeric_cols[0]["name"],
            "y2": numeric_cols[1]["name"],
            "reason": f"Combo chart overlays {numeric_cols[0]['name']} (bars) and {numeric_cols[1]['name']} (line) for dual-axis comparison.",
            "icon": "📉",
        })

    # ── Fallback: table
    recommendations.append({
        "type": "table",
        "score": 50,
        "reason": "Raw tabular view — always available.",
        "icon": "📋",
    })

    # Sort by score descending
    recommendations.sort(key=lambda r: r["score"], reverse=True)
    best = recommendations[0] if recommendations else None

    return {
        "recommendations": recommendations,
        "best": best,
        "column_summary": {
            "numeric": [c["name"] for c in numeric_cols],
            "date": [c["name"] for c in date_cols],
            "categorical": [c["name"] for c in cat_cols],
        },
    }
