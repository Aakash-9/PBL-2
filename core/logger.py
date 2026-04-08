# core/logger.py
"""
Minimal structured logger. One JSON line per query to logs/agent_logs.txt.
No external dependencies — stdlib logging only.
"""
import logging
import json
import os
from datetime import datetime

_LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "logs", "agent_logs.txt")
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)

_handler = logging.FileHandler(_LOG_PATH, encoding="utf-8")
_handler.setFormatter(logging.Formatter("%(message)s"))

_logger = logging.getLogger("querymind")
_logger.setLevel(logging.INFO)
_logger.addHandler(_handler)
_logger.propagate = False


def log(session_id: str, question: str, intent: dict, plan: dict,
        sql: str, validation: dict, exec_result: dict,
        confidence: str, insight: str, note: str = "") -> None:
    record = {
        "ts":           datetime.utcnow().isoformat() + "Z",
        "session_id":   session_id,
        "question":     question,
        "metric":       (intent.get("metric") or {}).get("metric") if isinstance(intent.get("metric"), dict) else intent.get("metric"),
        "time_filter":  intent.get("time_filter"),
        "dimension":    intent.get("dimension"),
        "operation":    intent.get("operation"),
        "sql":          sql,
        "valid":        validation.get("valid"),
        "errors":       validation.get("errors", []),
        "warnings":     validation.get("warnings", []),
        "rows":         exec_result.get("count", 0),
        "db_success":   exec_result.get("success", False),
        "confidence":   confidence,
        "insight_len":  len(insight),
        "note":         note,
    }
    _logger.info(json.dumps(record, default=str))
