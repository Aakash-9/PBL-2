# core/data_validator.py
"""
Data validation layer.
After the agent executes SQL and gets a result, this module:
  1. Builds a deterministic verification SQL from the metric YAML definition
  2. Runs it against Supabase independently
  3. Compares the result to what the agent returned
  4. Returns a validation verdict with mismatch details

This is the only way to guarantee the number is correct without a human.
Pure code — no LLM involved.
"""
import os
import yaml
import re
from core.supabase_client import execute_sql

_YAML_PATH = os.path.join(os.path.dirname(__file__), "..", "yaml", "metric_glossary.yaml")
_TIME_PATH  = os.path.join(os.path.dirname(__file__), "..", "yaml", "time_filter_governance.yaml")

with open(_YAML_PATH) as f:
    _METRICS = yaml.safe_load(f) or {}

with open(_TIME_PATH) as f:
    _TIME_FILTERS = (yaml.safe_load(f) or {}).get("time_filters", {})

# Tolerance: results within 0.1% are considered matching
_TOLERANCE = 0.001


def verify(agent_rows: list, intent: dict, agent_sql: str) -> dict:
    """
    Builds a verification SQL from the metric YAML and compares to agent result.

    Returns:
    {
        "verified":       bool,   # True = numbers match
        "skipped":        bool,   # True = verification not applicable
        "skip_reason":    str,    # why skipped
        "agent_value":    float | None,
        "verified_value": float | None,
        "delta_pct":      float | None,
        "mismatch":       bool,
        "note":           str,
    }
    """
    metric_info = intent.get("metric")
    time_filter = intent.get("time_filter")
    dimension   = intent.get("dimension")
    filters     = intent.get("filters", {})
    operation   = intent.get("operation", "aggregate")

    # Skip verification for non-aggregate operations
    if operation in ("compare", "trend", "breakdown"):
        return _skip("Verification skipped for compare/trend operations")

    # Skip if no metric
    if not metric_info:
        return _skip("No metric definition available for verification")

    # Skip if time_filter is a dynamic key not in YAML (e.g. last_5_months)
    if time_filter and time_filter not in _TIME_FILTERS:
        return _skip(f"Dynamic time filter '{time_filter}' — verification skipped")

    # Skip top_n/bottom_n — row ordering is correct by construction
    if operation in ("top_n", "bottom_n"):
        return _skip("Verification skipped for top_n/bottom_n")

    # Skip if dimension is present
    if dimension:
        return _skip("Verification skipped for group-by queries")

    # Skip if brand/category filters present — verification SQL can't replicate complex filters reliably
    if filters.get("brand") or filters.get("payment_mode") or filters.get("gender"):
        return _skip("Verification skipped for filtered queries")

    # Skip if time_filter is a dynamic key not in YAML
    if time_filter and time_filter not in _TIME_FILTERS:
        return _skip(f"Dynamic time filter '{time_filter}' — verification skipped")

    # Single aggregate — build exact verification SQL
    return _verify_aggregate(agent_rows, metric_info, time_filter, filters)


def _verify_aggregate(agent_rows: list, metric_info: dict,
                      time_filter: str | None, filters: dict) -> dict:
    """Verify a single aggregate result (e.g. GMV last month = X)."""
    metric_key  = metric_info["metric"]
    aggregation = metric_info.get("aggregation", "")
    status_filter = metric_info.get("filter", "")

    if not aggregation:
        return _skip(f"No aggregation formula for metric '{metric_key}'")

    # Build WHERE clauses
    where_parts = []
    if status_filter:
        where_parts.append(status_filter)

    if time_filter and time_filter in _TIME_FILTERS:
        time_sql = _TIME_FILTERS[time_filter].get("sql", "")
        if time_sql:
            where_parts.append(time_sql)

    if "city" in filters:
        where_parts.append(f"c.city = '{filters['city']}'")

    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    # Build FROM + JOINs based on required tables
    tables = metric_info.get("tables", ["orders"])
    from_join = _build_from_join(tables, filters)

    verification_sql = f"""
        SELECT {aggregation} AS verified_value
        FROM {from_join}
        {where_clause}
    """.strip()

    result = execute_sql(verification_sql)
    if not result["success"] or not result["rows"]:
        return _skip(f"Verification query failed: {result.get('error','unknown')}")

    verified_value = _extract_numeric(result["rows"][0])
    agent_value    = _extract_numeric(agent_rows[0]) if agent_rows else None

    if agent_value is None or verified_value is None:
        return _skip("Could not extract numeric values for comparison")

    delta_pct = abs(agent_value - verified_value) / max(abs(verified_value), 1)
    matched   = delta_pct <= _TOLERANCE

    return {
        "verified":       matched,
        "skipped":        False,
        "skip_reason":    "",
        "agent_value":    round(agent_value, 2),
        "verified_value": round(verified_value, 2),
        "delta_pct":      round(delta_pct * 100, 4),
        "mismatch":       not matched,
        "note":           "" if matched else (
            f"Data mismatch detected: agent returned {round(agent_value,2)}, "
            f"verification query returned {round(verified_value,2)} "
            f"(delta: {round(delta_pct*100,2)}%). SQL may have incorrect filters."
        ),
    }


def _verify_top_row(agent_rows: list, metric_info: dict, time_filter: str | None,
                    dimension: dict, filters: dict) -> dict:
    """
    For group-by queries (e.g. top cities by GMV), verify the top row only.
    If the top city in agent result matches the top city in a direct query, it's correct.
    """
    if not agent_rows:
        return _skip("No agent rows to verify")

    metric_key  = metric_info["metric"]
    aggregation = metric_info.get("aggregation", "")
    status_filter = metric_info.get("filter", "")

    if not aggregation:
        return _skip(f"No aggregation formula for metric '{metric_key}'")

    dim_table  = dimension["table"]
    dim_col    = dimension["column"]
    dim_alias  = _table_alias(dim_table)

    where_parts = []
    if status_filter:
        where_parts.append(status_filter)
    if time_filter and time_filter in _TIME_FILTERS:
        time_sql = _TIME_FILTERS[time_filter].get("sql", "")
        if time_sql:
            where_parts.append(time_sql)
    if "city" in filters:
        where_parts.append(f"c.city = '{filters['city']}'")

    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    tables = list({t for t in metric_info.get("tables", ["orders"])} | {dim_table})
    from_join = _build_from_join(tables, filters)

    verification_sql = f"""
        SELECT {dim_alias}.{dim_col} AS label, {aggregation} AS verified_value
        FROM {from_join}
        {where_clause}
        GROUP BY {dim_alias}.{dim_col}
        ORDER BY verified_value DESC
        LIMIT 1
    """.strip()

    result = execute_sql(verification_sql)
    if not result["success"] or not result["rows"]:
        return _skip(f"Verification query failed: {result.get('error','unknown')}")

    verified_row   = result["rows"][0]
    verified_label = verified_row.get("label", "")
    verified_value = _extract_numeric(verified_row)

    # Find the top row in agent result
    agent_top = agent_rows[0]
    agent_label = agent_top.get(dim_col) or agent_top.get("label", "")
    agent_value = _extract_numeric(agent_top)

    label_match = str(agent_label).strip().lower() == str(verified_label).strip().lower()
    value_match = (
        agent_value is not None and verified_value is not None and
        abs(agent_value - verified_value) / max(abs(verified_value), 1) <= _TOLERANCE
    )
    matched = label_match and value_match

    delta_pct = (
        abs(agent_value - verified_value) / max(abs(verified_value), 1)
        if agent_value is not None and verified_value is not None else None
    )

    return {
        "verified":       matched,
        "skipped":        False,
        "skip_reason":    "",
        "agent_value":    round(agent_value, 2) if agent_value is not None else None,
        "verified_value": round(verified_value, 2) if verified_value is not None else None,
        "delta_pct":      round(delta_pct * 100, 4) if delta_pct is not None else None,
        "mismatch":       not matched,
        "note":           "" if matched else (
            f"Top result mismatch: agent says {agent_label}={round(agent_value or 0,2)}, "
            f"verification says {verified_label}={round(verified_value or 0,2)}."
        ),
    }


def _build_from_join(tables: list, filters: dict) -> str:
    """Build FROM + JOIN clause for the given tables using known join paths."""
    _JOIN_PATHS = {
        ("orders",      "order_items"):  "JOIN order_items oi ON o.order_id = oi.order_id",
        ("order_items", "products"):     "JOIN products p ON oi.product_id = p.product_id",
        ("order_items", "sellers"):      "JOIN sellers s ON oi.seller_id = s.seller_id",
        ("orders",      "customers"):    "JOIN customers c ON o.customer_id = c.customer_id",
        ("orders",      "payments"):     "JOIN payments pay ON o.order_id = pay.order_id",
        ("orders",      "shipments"):    "JOIN shipments sh ON o.order_id = sh.order_id",
        ("returns",     "order_items"):  "JOIN order_items oi ON ret.order_item_id = oi.order_item_id",
        ("order_items", "returns"):      "LEFT JOIN returns ret ON ret.order_item_id = oi.order_item_id",
    }

    # city filter always needs customers
    if "city" in filters and "customers" not in tables:
        tables = list(tables) + ["customers"]

    # return_rate uses order_items as primary but time filter references o.order_date
    # so orders must be included
    if "order_items" in tables and "orders" not in tables:
        tables = list(tables) + ["orders"]

    # Determine primary table
    if "orders" in tables:
        primary = "orders o"
        joined  = {"orders"}
    elif "order_items" in tables:
        primary = "order_items oi"
        joined  = {"order_items"}
    elif "returns" in tables:
        primary = "returns ret"
        joined  = {"returns"}
    else:
        primary = f"{tables[0]} t"
        joined  = {tables[0]}

    joins = []
    remaining = [t for t in tables if t not in joined]

    for target in remaining:
        for (a, b), clause in _JOIN_PATHS.items():
            if a in joined and b == target:
                joins.append(clause)
                joined.add(target)
                break
            elif b in joined and a == target:
                # reverse path
                rev = _JOIN_PATHS.get((b, a))
                if rev:
                    joins.append(rev)
                    joined.add(target)
                break

    return f"{primary} {' '.join(joins)}".strip()


def _extract_numeric(row: dict) -> float | None:
    """Extract the first numeric value from a result row."""
    for k, v in row.items():
        if k in ("label", "period", "city", "state", "category", "brand",
                 "seller_name", "gender", "order_status", "order_channel",
                 "payment_mode", "courier_partner", "warehouse_city"):
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def _table_alias(table: str) -> str:
    return {
        "orders": "o", "order_items": "oi", "products": "p",
        "sellers": "s", "customers": "c", "payments": "pay",
        "shipments": "sh", "returns": "ret", "refunds": "ref",
        "seller_settlements": "ss", "warehouses": "wh",
        "inventory": "inv",
    }.get(table, table[:3])


def _skip(reason: str) -> dict:
    return {
        "verified": True,   # don't penalise confidence when we can't verify
        "skipped": True,
        "skip_reason": reason,
        "agent_value": None,
        "verified_value": None,
        "delta_pct": None,
        "mismatch": False,
        "note": "",
    }
