# core/sql_rule_engine.py
"""
Validates generated SQL against the join graph from join_path_specification.yaml.
Stronger than string-matching: detects illegal direct joins by parsing JOIN ... ON clauses.
"""
import re
import os
import yaml

_YAML_PATH = os.path.join(os.path.dirname(__file__), "..", "yaml", "join_path_specification.yaml")

with open(_YAML_PATH) as f:
    _SPEC = yaml.safe_load(f) or {}

# Valid join conditions (normalised, whitespace-collapsed)
_VALID_CONDITIONS: set[str] = set()
for _p in _SPEC.get("core_paths", {}).values():
    _VALID_CONDITIONS.add(re.sub(r"\s+", " ", _p["condition"].strip().lower()))

# Known schema columns per table
_SCHEMA: dict[str, set[str]] = {
    "customers":           {"customer_id","signup_date","city","state","gender","age_group","loyalty_tier","preferred_payment_mode","risk_score","is_active"},
    "inventory":           {"inventory_id","product_id","seller_id","warehouse_id","available_qty","reserved_qty","damaged_qty","last_updated"},
    "inventory_movements": {"movement_id","inventory_id","movement_type","quantity","reference_id","movement_date"},
    "order_items":         {"order_item_id","order_id","product_id","seller_id","quantity","item_price","item_status"},
    "orders":              {"order_id","customer_id","order_date","order_status","payment_mode","total_amount","discount_amount","final_payable","order_channel"},
    "payments":            {"payment_id","order_id","payment_method","payment_status","payment_date","paid_amount","gateway"},
    "products":            {"product_id","seller_id","brand","category","sub_category","size","color","mrp","selling_price","season","is_returnable"},
    "refunds":             {"refund_id","return_id","refund_method","refund_status","refund_date","refunded_amount"},
    "returns":             {"return_id","order_item_id","return_date","return_reason","return_type","return_status","pickup_date","refund_amount"},
    "seller_settlements":  {"settlement_id","seller_id","order_item_id","gross_amount","commission_amount","net_payable","settlement_date","settlement_status"},
    "sellers":             {"seller_id","seller_name","seller_type","onboarding_date","seller_rating","seller_region","commission_rate","risk_flag","is_active"},
    "shipments":           {"shipment_id","order_id","warehouse_id","courier_partner","shipped_date","promised_delivery_date","actual_delivery_date","delivery_status"},
    "warehouses":          {"warehouse_id","warehouse_city","warehouse_state","warehouse_type","is_active"},
}

_ALIAS_TO_TABLE = {
    "o": "orders", "oi": "order_items", "p": "products", "s": "sellers",
    "c": "customers", "pay": "payments", "sh": "shipments", "ret": "returns",
    "ref": "refunds", "ss": "seller_settlements", "wh": "warehouses",
    "inv": "inventory", "im": "inventory_movements",
}

_FORBIDDEN_DIRECT = [
    ("orders",  "sellers",           "order_items"),
    ("orders",  "warehouses",        "shipments"),
    ("orders",  "refunds",           "returns"),
    ("orders",  "seller_settlements","order_items"),
]

# Invalid payment_mode values (DB only has COD and Prepaid)
_INVALID_PAYMENTS = ["'online'", "'upi'", "'card'", "'wallet'", "'netbanking'",
                     "'debit card'", "'credit card'", "'gpay'", "'phonepe'", "'paytm'"]


def validate(sql: str) -> dict:
    errors: list[str] = []
    warnings: list[str] = []

    if not sql or not sql.strip():
        return {"valid": False, "errors": ["Empty SQL"], "warnings": []}

    sql_lower = sql.lower()

    # Rule 1: No SELECT *
    if re.search(r"select\s+\*", sql_lower):
        errors.append("SELECT * is forbidden — use explicit columns")

    # Rule 2: Extract tables in query
    table_pattern = re.compile(r"(?:from|join)\s+\"?(\w+)\"?\s+(\w+)", re.IGNORECASE)
    tables_in_query: dict[str, str] = {}
    for match in table_pattern.finditer(sql):
        tables_in_query[match.group(2).lower()] = match.group(1).lower()

    # Rule 3: LEFT JOIN enforcement for returns
    if "returns" in sql_lower:
        # Find any JOIN returns that is NOT preceded by LEFT
        for m in re.finditer(r"(\w+\s+)?join\s+returns\b", sql_lower):
            prefix = (m.group(1) or "").strip()
            if prefix != "left":
                errors.append(
                    "returns table MUST use LEFT JOIN — INNER JOIN makes return_rate always 100%"
                )
                break

    # Rule 4: Invalid payment_mode values
    for bp in _INVALID_PAYMENTS:
        if bp in sql_lower:
            errors.append(f"Invalid payment_mode {bp} — DB only has 'COD' and 'Prepaid'")

    # Rule 5: Forbidden direct joins
    present_tables = set(tables_in_query.values())
    for left, right, bridge in _FORBIDDEN_DIRECT:
        if left in present_tables and right in present_tables and bridge not in present_tables:
            errors.append(f"Illegal direct join: {left} -> {right} requires {bridge} as bridge")

    # Rule 6: Validate JOIN ON conditions
    join_on_pattern = re.compile(
        r"join\s+\S+\s+\w+\s+on\s+(.+?)(?=\s+(?:join|where|group|order|limit|$))",
        re.IGNORECASE | re.DOTALL
    )
    for match in join_on_pattern.finditer(sql):
        condition = re.sub(r"\s+", " ", match.group(1).strip().lower().split("\n")[0])
        normalised = _normalise_condition(condition, tables_in_query)
        if normalised and normalised not in _VALID_CONDITIONS:
            if not _is_plausible_condition(condition, tables_in_query):
                errors.append(f"Invalid JOIN condition: '{condition}' — not in approved join paths")

    # Rule 7: Unknown column references
    col_ref_pattern = re.compile(r"\b(\w+)\.(\w+)\b")
    for match in col_ref_pattern.finditer(sql):
        alias = match.group(1).lower()
        col   = match.group(2).lower()
        table = tables_in_query.get(alias) or _ALIAS_TO_TABLE.get(alias)
        if table and table in _SCHEMA:
            if col not in _SCHEMA[table] and col not in {"*"}:
                if not re.match(r"^\d+$", col):
                    warnings.append(f"Unknown column: {alias}.{col} (table: {table})")

    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}


def _normalise_condition(cond: str, alias_map: dict[str, str]) -> str:
    parts = cond.split("=")
    if len(parts) != 2:
        return cond
    left  = parts[0].strip()
    right = parts[1].strip()

    def resolve(ref: str) -> str:
        if "." in ref:
            a, c = ref.split(".", 1)
            table = alias_map.get(a) or _ALIAS_TO_TABLE.get(a, a)
            return f"{table}.{c}"
        return ref

    return f"{resolve(left)} = {resolve(right)}"


def _is_plausible_condition(cond: str, alias_map: dict[str, str]) -> bool:
    parts = cond.split("=")
    if len(parts) != 2:
        return False
    for part in parts:
        part = part.strip()
        if "." in part:
            a, c = part.split(".", 1)
            table = alias_map.get(a) or _ALIAS_TO_TABLE.get(a)
            if table and table in _SCHEMA and c in _SCHEMA[table]:
                continue
        return False
    return True
