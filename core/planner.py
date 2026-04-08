# core/planner.py
"""
Converts parsed intent into a structured SQL plan.
LLM receives a plan to translate, not a raw question to interpret.
All join paths come from join_path_specification.yaml — never invented.
"""
import os
import yaml

_YAML_PATH = os.path.join(os.path.dirname(__file__), "..", "yaml", "join_path_specification.yaml")
_TIME_YAML  = os.path.join(os.path.dirname(__file__), "..", "yaml", "time_filter_governance.yaml")

# Load join graph once
with open(_YAML_PATH) as f:
    _JOIN_SPEC = yaml.safe_load(f) or {}

with open(_TIME_YAML) as f:
    _TIME_SPEC = yaml.safe_load(f) or {}

# Adjacency map: table → {neighbor_table: condition}
_JOIN_GRAPH: dict[str, dict[str, str]] = {}
for _path in _JOIN_SPEC.get("core_paths", {}).values():
    a, b, cond = _path["from"], _path["to"], _path["condition"]
    _JOIN_GRAPH.setdefault(a, {})[b] = cond
    _JOIN_GRAPH.setdefault(b, {})[a] = cond  # bidirectional


def _bfs_join_path(start: str, targets: set[str]) -> list[dict]:
    """
    BFS over the join graph to find the minimal join sequence
    from `start` to reach all `targets`.
    Returns list of {"table": str, "condition": str} in join order.
    """
    if not targets:
        return []

    visited = {start}
    queue = [(start, [])]
    joins = []
    remaining = set(targets)

    while queue and remaining:
        node, path = queue.pop(0)
        for neighbor, cond in _JOIN_GRAPH.get(node, {}).items():
            if neighbor not in visited:
                visited.add(neighbor)
                new_path = path + [{"table": neighbor, "condition": cond}]
                if neighbor in remaining:
                    joins.extend(new_path)
                    remaining.discard(neighbor)
                    queue.append((neighbor, new_path))
                else:
                    queue.append((neighbor, new_path))

    return joins


def build(intent: dict) -> dict:
    """
    intent: output of intent_parser.parse()

    Returns plan:
    {
        "primary_table":  str,
        "joins":          [{"table": str, "condition": str}],
        "select":         [str],          # column expressions
        "group_by":       [str],
        "where":          [str],          # SQL WHERE clauses
        "order_by":       str,
        "limit":          int,
        "aggregation":    str,
        "operation":      str,
    }
    """
    metric    = intent.get("metric")
    dimension = intent.get("dimension")
    time_filter      = intent.get("time_filter")
    dynamic_interval = intent.get("dynamic_interval")  # e.g. "5 months"
    operation = intent.get("operation", "aggregate")
    limit     = intent.get("limit")
    filters   = intent.get("filters", {})

    # ── Determine required tables ────────────────────────────────────────────
    required_tables: set[str] = set()
    primary_table = "orders"

    if metric:
        for t in metric.get("tables", []):
            required_tables.add(t)
        if metric["tables"]:
            primary_table = metric["tables"][0]

    if dimension:
        required_tables.add(dimension["table"])

    # city filter needs customers
    if "city" in filters:
        required_tables.add("customers")
    # category/brand filter needs products
    if "category" in filters or "brand" in filters:
        required_tables.add("products")
        if "order_items" not in required_tables:
            required_tables.add("order_items")

    required_tables.discard(primary_table)

    # ── Build join path via BFS ──────────────────────────────────────────────
    joins = _bfs_join_path(primary_table, required_tables)

    # ── SELECT expressions ───────────────────────────────────────────────────
    select = []
    group_by = []

    if dimension:
        alias = _alias(dimension["table"])
        col_expr = f'{alias}.{dimension["column"]}'
        select.append(col_expr)
        group_by.append(col_expr)

    if metric:
        agg = metric["aggregation"]
        select.append(f"{agg} AS {metric['metric']}")
    else:
        select.append("COUNT(DISTINCT o.order_id) AS order_count")

    # ── WHERE clauses ────────────────────────────────────────────────────────
    where = []

    if metric and metric.get("filter"):
        where.append(metric["filter"])

    # Time filter — from YAML if known key, else build from dynamic_interval
    if time_filter:
        time_sql = _TIME_SPEC.get("time_filters", {}).get(time_filter, {}).get("sql", "")
        if time_sql:
            where.append(time_sql)
        elif dynamic_interval:
            # Synthetic key like "last_5_months" — build SQL directly
            # For month-based intervals, use date_trunc for consistency with data validator
            if "month" in dynamic_interval:
                where.append(f"o.order_date >= date_trunc('month', current_date - interval '{dynamic_interval}')")
            else:
                where.append(f"o.order_date >= current_date - interval '{dynamic_interval}'")

    if "city" in filters:
        where.append(f"c.city = '{filters['city']}'")
    if "category" in filters:
        where.append(f"p.category ILIKE '{filters['category']}'")
    if "brand" in filters:
        where.append(f"p.brand ILIKE '{filters['brand']}'")
    if "min_amount" in filters:
        where.append(f"oi.item_price >= {filters['min_amount']}")
    if "payment_mode" in filters:
        payment = filters["payment_mode"]
        if isinstance(payment, list):
            payment_list = "', '".join(payment)
            where.append(f"o.payment_mode IN ('{payment_list}')")
        else:
            where.append(f"o.payment_mode = '{payment}'")
    if "gender" in filters:
        where.append(f"c.gender = '{filters['gender']}'")
    if "order_status" in filters:
        where.append(f"o.order_status = '{filters['order_status']}'")  
    # ── ORDER BY + LIMIT ─────────────────────────────────────────────────────
    metric_col = metric["metric"] if metric else "order_count"
    order_by = f"{metric_col} DESC"

    if operation in ("compare", "growth"):
        # Always build compare_periods for compare/growth — time_filter alone is not enough
        where_this  = ["date_trunc('month', o.order_date) = date_trunc('month', current_date)"]
        where_last  = ["date_trunc('month', o.order_date) = date_trunc('month', current_date - interval '1 month')"]
        if metric and metric.get("filter"):
            where_this.append(metric["filter"])
            where_last.append(metric["filter"])
        return {
            "primary_table": primary_table,
            "joins": joins,
            "select": select,
            "group_by": group_by,
            "where": where,
            "order_by": order_by,
            "limit": limit or 1000,
            "aggregation": metric["aggregation"] if metric else "COUNT(*)",
            "operation": "compare",
            "compare_periods": {
                "this_month": where_this,
                "last_month": where_last,
            },
        }

    return {
        "primary_table": primary_table,
        "joins": joins,
        "select": select,
        "group_by": group_by,
        "where": where,
        "order_by": order_by,
        "limit": limit or 1000,
        "aggregation": metric["aggregation"] if metric else "COUNT(*)",
        "operation": operation,
        "compare_periods": None,
    }


def plan_to_prompt(plan: dict) -> str:
    """Serialises the plan into a compact instruction block for the LLM prompt."""
    joins_text = "\n".join(
        f"  JOIN {j['table']} ON {j['condition']}" for j in plan["joins"]
    ) or "  (no joins needed)"

    where_text = "\n  AND ".join(plan["where"]) if plan["where"] else "(no filters)"

    group_text = ", ".join(plan["group_by"]) if plan["group_by"] else "(no grouping)"

    select_text = ",\n  ".join(plan["select"])

    compare_text = ""
    if plan.get("compare_periods"):
        cp = plan["compare_periods"]
        compare_text = (
            f"\nCOMPARISON PERIODS:\n"
            f"  this_month WHERE: {' AND '.join(cp['this_month'])}\n"
            f"  last_month WHERE: {' AND '.join(cp['last_month'])}"
        )

    return (
        f"STRUCTURED QUERY PLAN (translate this EXACTLY into SQL):\n"
        f"  PRIMARY TABLE : {plan['primary_table']} (alias: {_alias(plan['primary_table'])})\n"
        f"  JOINS         :\n{joins_text}\n"
        f"  SELECT        :\n  {select_text}\n"
        f"  WHERE         : {where_text}\n"
        f"  GROUP BY      : {group_text}\n"
        f"  ORDER BY      : {plan['order_by']}\n"
        f"  LIMIT         : {plan['limit']}\n"
        f"  OPERATION     : {plan['operation']}"
        f"{compare_text}"
    )


def _alias(table: str) -> str:
    _ALIASES = {
        "orders": "o", "order_items": "oi", "products": "p",
        "sellers": "s", "customers": "c", "payments": "pay",
        "shipments": "sh", "returns": "ret", "refunds": "ref",
        "seller_settlements": "ss", "warehouses": "wh",
        "inventory": "inv", "inventory_movements": "im",
    }
    return _ALIASES.get(table, table[:3])
