# core/intent_parser_enhanced.py
import re
from core.intent_parser import parse

_GREETING_PATTERNS = [
    r"^\s*(hi|hello|hey|howdy|good\s*(morning|afternoon|evening)|what'?s\s*up|sup)\b",
    r"^\s*(who are you|what (can|do) you do|help me)\b",
]


def parse_enhanced(question: str, last_context: dict | None = None) -> dict:
    for pattern in _GREETING_PATTERNS:
        if re.search(pattern, question.strip(), re.IGNORECASE):
            return {
                "metric": None, "dimension": None, "time_filter": None,
                "dynamic_interval": None, "operation": "greeting",
                "limit": None, "is_followup": False, "filters": {},
                "ambiguity": None, "dual_metrics": None,
            }
    return parse(question, last_context)
