# core/safety.py
"""
SQL safety and performance guards.
Pure code — no LLM, no external dependencies.
Applied to every SQL before execution.
"""
import re

MAX_ROWS = 1000

# Patterns that must never appear in generated SQL
_BLOCKED_PATTERNS = [
    (r"\bdrop\s+table\b",        "DROP TABLE is not allowed"),
    (r"\btruncate\b",            "TRUNCATE is not allowed"),
    (r"\bdelete\s+from\b",       "DELETE is not allowed"),
    (r"\binsert\s+into\b",       "INSERT is not allowed"),
    (r"\bupdate\s+\w+\s+set\b",  "UPDATE is not allowed"),
    (r"\balter\s+table\b",       "ALTER TABLE is not allowed"),
    (r"\bcreate\s+table\b",      "CREATE TABLE is not allowed"),
    (r";\s*\w",                  "Multiple statements are not allowed"),
]

# Known tables — queries referencing none of these are likely full-scan attempts
_KNOWN_TABLES = {
    "orders", "order_items", "products", "sellers", "customers",
    "payments", "shipments", "returns", "refunds", "seller_settlements",
    "warehouses", "inventory", "inventory_movements",
}


def enforce(sql: str) -> dict:
    """
    Returns {"safe": bool, "sql": str, "errors": list[str]}
    - Blocks dangerous DDL/DML
    - Enforces LIMIT cap
    - Strips trailing semicolons
    """
    if not sql or not sql.strip():
        return {"safe": False, "sql": sql, "errors": ["Empty SQL"]}

    errors = []
    sql_lower = sql.lower()

    # Block dangerous patterns
    for pattern, msg in _BLOCKED_PATTERNS:
        if re.search(pattern, sql_lower):
            errors.append(msg)

    if errors:
        return {"safe": False, "sql": sql, "errors": errors}

    # Strip trailing semicolons
    sql = sql.strip().rstrip(";").strip()

    # Enforce LIMIT — if no LIMIT present, inject it
    if not re.search(r"\blimit\s+\d+", sql_lower):
        sql = f"{sql}\nLIMIT {MAX_ROWS}"
    else:
        # If LIMIT exceeds MAX_ROWS, clamp it
        sql = re.sub(
            r"\blimit\s+(\d+)",
            lambda m: f"LIMIT {min(int(m.group(1)), MAX_ROWS)}",
            sql,
            flags=re.IGNORECASE,
        )

    return {"safe": True, "sql": sql, "errors": []}
