# core/intent_parser_enhanced.py
import re
from core.intent_parser import parse

_GREETING_PATTERNS = [
    r"^\s*(hi|hello|hey|howdy|good\s*(morning|afternoon|evening)|what'?s\s*up|sup)\b",
    r"^\s*(who are you|what (can|do) you do|help me)\b",
]

_OFFTOPIC_PATTERNS = [
    r"\b(weather|cricket|movie|song|recipe|cook|sport|football|politics|news|joke|poem|story|capital of|president|prime minister|actor|actress)\b",
    r"^\s*(what is \d|calculate|solve|translate|write a|tell me a)\b",
]

_BUSINESS_KEYWORDS = [
    "order", "sale", "revenue", "gmv", "product", "customer", "seller", "brand",
    "category", "return", "payment", "city", "state", "footwear", "clothing",
    "topwear", "bottomwear", "accessories", "dresses", "cod", "prepaid",
    "shipment", "delivery", "quantity", "price", "discount", "refund",
    "month", "year", "quarter", "week", "today", "last", "this", "top", "bottom",
]


def _is_business_query(question: str) -> bool:
    q = question.lower()
    return any(kw in q for kw in _BUSINESS_KEYWORDS)


def parse_enhanced(question: str, last_context: dict | None = None) -> dict:
    for pattern in _GREETING_PATTERNS:
        if re.search(pattern, question.strip(), re.IGNORECASE):
            return {
                "metric": None, "dimension": None, "time_filter": None,
                "dynamic_interval": None, "operation": "greeting",
                "limit": None, "is_followup": False, "filters": {},
                "ambiguity": None, "dual_metrics": None,
            }

    # Off-topic: matches non-business pattern AND has no business keywords
    for pattern in _OFFTOPIC_PATTERNS:
        if re.search(pattern, question.strip(), re.IGNORECASE) and not _is_business_query(question):
            return {
                "metric": None, "dimension": None, "time_filter": None,
                "dynamic_interval": None, "operation": "offtopic",
                "limit": None, "is_followup": False, "filters": {},
                "ambiguity": None, "dual_metrics": None,
            }

    return parse(question, last_context)
