# core/intent_parser.py
"""
Extracts structured intent from a natural language query.
Deterministic: regex + keyword maps, no LLM.
Uses session last_context to resolve follow-up references.
"""
import re
from core.semantic_engine import resolve_metric

# ── Time filter keyword map (matches time_filter_governance.yaml keys) ──────
_TIME_MAP = [
    (r"\btoday\b",                              "today"),
    (r"\byesterday\b",                          "yesterday"),
    (r"\blast\s*7\s*days?\b",                   "last_7_days"),
    (r"\blast\s*week\b",                        "last_7_days"),
    (r"\blast\s*30\s*days?\b",                  "last_30_days"),
    (r"\blast\s*90\s*days?\b",                  "last_90_days"),
    (r"\blast\s*180\s*days?\b",                 "last_180_days"),
    (r"\blast\s*3\s*months?\b",                 "last_3_months"),
    (r"\blast\s*6\s*months?\b",                 "last_6_months"),
    (r"\blast\s*(?:half\s*year|half-year)\b",   "last_6_months"),
    (r"\blast\s*month\b",                       "last_month"),
    (r"\bthis\s*month\b",                       "this_month"),
    (r"\blast\s*quarter\b",                     "last_quarter"),
    (r"\blast\s*year\b",                        "last_year"),
    (r"\bthis\s*year\b",                        "this_year"),
]

# Known product categories for filter extraction
_CATEGORIES = {
    "footwear", "accessories", "dresses", "topwear", "bottomwear",
    "clothing", "apparel", "shoes", "bags", "jewellery", "jewelry",
}

# Known brands for filter extraction (lowercase)
_BRANDS = {
    "here&now", "here and now", "roadster", "hrx", "mango", "h&m",
    "nike", "adidas", "puma", "levis", "levi's", "zara", "only",
    "vero moda", "w", "biba", "global desi", "aurelia",
}

# ── Dimension keyword map → (table, column) ──────────────────────────────────
_DIMENSION_MAP = [
    (r"\bby\s+cit(?:y|ies)\b",             ("customers", "city")),
    (r"\bcities\b",                          ("customers", "city")),
    (r"\bby\s+state\b",                     ("customers", "state")),
    (r"\bby\s+categor(?:y|ies)\b",          ("products",  "category")),
    (r"\bby\s+brand\b",                     ("products",  "brand")),
    (r"\bby\s+seller\b",                    ("sellers",   "seller_name")),
    (r"\bby\s+channel\b",                   ("orders",    "order_channel")),
    (r"\bby\s+payment\b",                   ("orders",    "payment_mode")),
    (r"\bby\s+product\b",                   ("products",  "sub_category")),
    (r"\bby\s+region\b",                    ("sellers",   "seller_region")),
    (r"\bby\s+gender\b",                    ("customers", "gender")),
    (r"\bby\s+loyalty\b",                   ("customers", "loyalty_tier")),
    (r"\bby\s+warehouse\b",                 ("warehouses","warehouse_city")),
    (r"\bby\s+courier\b",                   ("shipments", "courier_partner")),
    (r"\bby\s+status\b",                    ("orders",    "order_status")),
]

# ── Operation detection ───────────────────────────────────────────────────────
_OP_MAP = [
    (r"\bcompare\b|\bvs\.?\b|\bversus\b",   "compare"),
    (r"\btrend\b|\bover\s+time\b",          "trend"),
    (r"\btop\s*\d+\b",                      "top_n"),
    (r"\btop\b",                             "top_n"),
    (r"\bbottom\s*\d+\b",                   "bottom_n"),
    (r"\bbottom\b",                          "bottom_n"),
    (r"\bgrowth\b|\bchange\b",              "growth"),
    (r"\bbreakdown\b|\bdistribution\b",     "breakdown"),
]

_FOLLOW_UP_SIGNALS = [
    r"\bsame\s+as\s+before\b",
    r"\bsame\s+but\b",
    r"\bsame\s+for\b",
    r"\bnow\s+for\b",
    r"\bwhat\s+about\b",
    r"\band\s+for\b",
]


def parse(question: str, last_context: dict | None = None) -> dict:
    q = question.lower()
    last = last_context or {}

    # ── Is this a follow-up? ─────────────────────────────────────────────────
    is_followup = any(re.search(p, q) for p in _FOLLOW_UP_SIGNALS)

    # ── Metric ───────────────────────────────────────────────────────────────
    metric = resolve_metric(question)
    # "sales" / "sale" are common synonyms for GMV
    if metric is None and re.search(r"\bsales?\b", q):
        from core.semantic_engine import _METRICS, _build
        metric = _build("gmv", _METRICS["gmv"])
    if metric is None and is_followup and last.get("metric"):
        from core.semantic_engine import _METRICS, _build
        key = last["metric"]
        if key in _METRICS:
            metric = _build(key, _METRICS[key])

    # ── Time filter ──────────────────────────────────────────────────────────
    time_filter    = None
    dynamic_interval = None  # e.g. "5 months" for arbitrary N

    for pattern, key in _TIME_MAP:
        if re.search(pattern, q):
            time_filter = key
            break

    # Dynamic N-month / N-day not in fixed map (e.g. "last 5 months", "last 8 months")
    if time_filter is None:
        m_mo = re.search(r"\blast\s*(\d+)\s*months?\b", q)
        m_dy = re.search(r"\blast\s*(\d+)\s*days?\b", q)
        # Specific year detection (e.g. "2024", "in 2023")
        m_yr = re.search(r"\b(20\d{2})\b", q)
        if m_mo:
            n = int(m_mo.group(1))
            dynamic_interval = f"{n} months"
            time_filter = f"last_{n}_months"
        elif m_dy:
            n = int(m_dy.group(1))
            dynamic_interval = f"{n} days"
            time_filter = f"last_{n}_days"
        elif m_yr:
            year = m_yr.group(1)
            time_filter = f"year_{year}"
            dynamic_interval = f"year {year}"

    if time_filter is None and is_followup and last.get("time_filter"):
        time_filter = last["time_filter"]

    # ── Dimension ────────────────────────────────────────────────────────────
    dimension = None
    for pattern, (table, col) in _DIMENSION_MAP:
        if re.search(pattern, q):
            dimension = {"table": table, "column": col}
            break
    if dimension is None and is_followup and last.get("dimension"):
        dimension = last["dimension"]

    # ── Operation ────────────────────────────────────────────────────────────
    operation = "aggregate"
    for pattern, op in _OP_MAP:
        if re.search(pattern, q):
            operation = op
            break

    # ── Limit ────────────────────────────────────────────────────────────────
    limit = None
    m = re.search(r"\b(?:top|bottom)\s*(\d+)\b", q)
    if m:
        limit = int(m.group(1))

    # ── Filters ──────────────────────────────────────────────────────────────
    filters = {}

    # City filter — exclude time words that follow 'in' or 'for'
    _TIME_WORDS = {"last", "this", "today", "yesterday", "next", "past",
                   "month", "year", "week", "quarter", "day", "days",
                   "months", "years", "weeks", "current", "previous"}
    _KNOWN_CITIES = {"mumbai", "delhi", "bangalore", "pune", "hyderabad",
                     "chennai", "kolkata", "ahmedabad", "jaipur", "lucknow",
                     "surat", "kanpur", "nagpur", "indore", "thane", "bhopal",
                     "visakhapatnam", "pimpri", "patna", "vadodara", "ghaziabad"}
    
    city_match = re.search(r"\b(?:for|in)\s+([A-Za-z][a-z]+)\b", question, re.IGNORECASE)
    if city_match:
        city = city_match.group(1)
        city_lower = city.lower()
        # Only accept if it's a known city AND not a time word
        if city_lower in _KNOWN_CITIES and city_lower not in _TIME_WORDS:
            filters["city"] = city.capitalize()

    # Category filter (e.g. "footwear", "accessories")
    for cat in _CATEGORIES:
        if re.search(r"\b" + re.escape(cat) + r"\b", q):
            filters["category"] = cat.capitalize()
            break

    # Brand filter
    for brand in sorted(_BRANDS, key=len, reverse=True):
        if re.search(r"\b" + re.escape(brand) + r"\b", q):
            filters["brand"] = brand.title()
            break

    # Minimum order amount filter (e.g. "above 3000", "minimum 3000", "more than 3000")
    min_match = re.search(
        r"(?:above|minimum|min|more\s+than|greater\s+than|over)\s+(?:rs\.?|inr|rupees?)?\s*(\d+)",
        q
    )
    if min_match:
        filters["min_amount"] = int(min_match.group(1))

    # ── Dual metric detection ("by quantity and revenue separately") ──────────
    dual_metrics = None
    dual_match = re.search(
        r"by\s+(\w+(?:\s+\w+)?)\s+and\s+(\w+(?:\s+\w+)?)\s+separately", q
    )
    if dual_match:
        dual_metrics = [dual_match.group(1).strip(), dual_match.group(2).strip()]

    # ── Ambiguity detection ──────────────────────────────────────────────────
    ambiguity = None
    if operation in ("top_n", "bottom_n") and metric is None and not is_followup:
        ambiguity = "top_products_no_metric"
    elif operation in ("compare", "growth") and metric is None and not is_followup:
        ambiguity = "compare_no_metric"
    elif metric is None and dimension is None and time_filter is None and not is_followup:
        ambiguity = "too_vague"

    return {
        "metric":           metric,
        "dimension":        dimension,
        "time_filter":      time_filter,
        "dynamic_interval": dynamic_interval,
        "operation":        operation,
        "limit":            limit,
        "is_followup":      is_followup,
        "filters":          filters,
        "ambiguity":        ambiguity,
        "dual_metrics":     dual_metrics,
    }
