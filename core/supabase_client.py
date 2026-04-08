# core/supabase_client.py
"""
Supabase integration:
- Execute validated SQL queries
- Introspect schema (tables + columns + dtypes)
- Used by both NL2SQL and the visual dashboard
"""
import os
from supabase import create_client, Client
from typing import Optional

_client: Optional[Client] = None

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")  # service key for schema access


def get_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _client


def execute_sql(sql: str) -> dict:
    """Run a SQL query via Supabase RPC (requires exec_sql function in DB)."""
    try:
        client = get_client()
        sql = sql.strip().rstrip(";")
        # Uses a Postgres function: CREATE OR REPLACE FUNCTION exec_sql(query text)...
        result = client.rpc("execute_query", {"query": sql}).execute()
        rows = result.data if result.data else []
        return {"success": True, "rows": rows, "count": len(rows)}
    except Exception as e:
        return {"success": False, "error": str(e), "rows": [], "count": 0}


def get_schema() -> list:
    """
    Returns all tables with their columns and data types.
    Uses information_schema via Supabase RPC.
    """
    sql = """
    SELECT
        t.table_name,
        c.column_name,
        c.data_type,
        c.is_nullable,
        c.column_default
    FROM information_schema.tables t
    JOIN information_schema.columns c
        ON t.table_name = c.table_name
        AND t.table_schema = c.table_schema
    WHERE t.table_schema = 'public'
        AND t.table_type = 'BASE TABLE'
    ORDER BY t.table_name, c.ordinal_position
    """
    result = execute_sql(sql)
    if not result["success"]:
        return []

    # Group by table
    tables = {}
    for row in result["rows"]:
        tname = row["table_name"]
        if tname not in tables:
            tables[tname] = {"name": tname, "columns": []}
        tables[tname]["columns"].append({
            "name": row["column_name"],
            "dtype": row["data_type"],
            "nullable": row["is_nullable"] == "YES",
        })
    return list(tables.values())


def get_column_sample(table: str, columns: list, limit: int = 100) -> dict:
    """Fetch sample data for selected columns."""
    col_list = ", ".join(f'"{c}"' for c in columns)
    sql = f'SELECT {col_list} FROM "{table}" LIMIT {limit}'
    result = execute_sql(sql)
    return result


def get_multi_table_data(selections: list, limit: int = 500) -> dict:
    """
    selections = [{"table": "orders", "columns": ["order_date", "order_status"]}, ...]
    Builds a query joining tables via known join paths, returns flat rows.
    """
    if len(selections) == 1:
        s = selections[0]
        return get_column_sample(s["table"], s["columns"], limit)

    # Build JOIN query using known join paths
    join_paths = {
        ("orders", "order_items"):      "o.order_id = oi.order_id",
        ("order_items", "products"):    "oi.product_id = p.product_id",
        ("order_items", "sellers"):     "oi.seller_id = s.seller_id",
        ("orders", "customers"):        "o.customer_id = c.customer_id",
        ("orders", "payments"):         "o.order_id = pay.order_id",
        ("orders", "shipments"):        "o.order_id = sh.order_id",
        ("returns", "order_items"):     "ret.order_item_id = oi.order_item_id",
    }

    aliases = {
        "orders": "o", "order_items": "oi", "products": "p",
        "sellers": "s", "customers": "c", "payments": "pay",
        "shipments": "sh", "returns": "ret", "refunds": "ref",
    }

    # Select columns
    select_parts = []
    for sel in selections:
        alias = aliases.get(sel["table"], sel["table"][:3])
        for col in sel["columns"]:
            select_parts.append(f'{alias}."{col}" AS "{sel["table"]}__{col}"')

    primary = selections[0]
    primary_alias = aliases.get(primary["table"], "t1")
    from_clause = f'"{primary["table"]}" {primary_alias}'

    join_clauses = []
    joined = {primary["table"]}
    for sel in selections[1:]:
        t2 = sel["table"]
        alias2 = aliases.get(t2, t2[:3])
        cond = None
        for (a, b), c in join_paths.items():
            if (a in joined and b == t2) or (b in joined and a == t2):
                cond = c
                break
        if cond:
            join_clauses.append(f'JOIN "{t2}" {alias2} ON {cond}')
            joined.add(t2)

    sql = f"SELECT {', '.join(select_parts)} FROM {from_clause} {' '.join(join_clauses)} LIMIT {limit}"
    result = execute_sql(sql)
    return result
