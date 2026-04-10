# core/agent.py
"""
Pipeline orchestrator. Single entry point for the full NL2SQL flow.
query.py calls run() — all logic lives here, not in the router.

Flow:
  parse intent → ambiguity check →
  build plan → retrieve RAG context →
  generate SQL → safety check → rule_engine validate → sql_critic →
  [retry up to MAX_RETRIES] →
  execute → compute metrics → generate insight →
  confidence score → log → return clean response
"""
from core.intent_parser   import parse as parse_intent
from core.planner         import build as build_plan, plan_to_prompt
from core.rag_engine      import retrieve_context
from core.sql_generator   import generate_sql
from core.semantic_engine import enforce_metric
from core.sql_rule_engine import validate as rule_validate
from core.sql_critic      import critique
from core.supabase_client import execute_sql
from core.metrics_engine  import compute as compute_metrics
from core.insight_engine  import generate as generate_insight
from core.session_store   import get as get_session, update as update_session
from core.safety          import enforce as safety_enforce
from core.logger          import log as agent_log
from core.data_validator  import verify as data_verify

MAX_RETRIES = 2

# ── Clarification messages per ambiguity type ────────────────────────────────
_CLARIFICATIONS = {
    "top_products_no_metric": (
        "I need to know what metric to rank by. Try:\n"
        "• 'Top 5 products by revenue this month'\n"
        "• 'Top 10 products by quantity sold'\n"
        "• 'Top 5 products by return rate'\n\n"
        "What would you like to measure?"
    ),
    "compare_no_metric": (
        "I need to know what to compare. Try:\n"
        "• 'Compare GMV this month vs last month'\n"
        "• 'Compare order count this week vs last week'\n"
        "• 'Compare return rate this quarter vs last quarter'\n\n"
        "What metric should I compare?"
    ),
    "too_vague": (
        "I need more details to answer that. Try asking:\n"
        "• 'What is the GMV last month?'\n"
        "• 'Top 5 cities by revenue'\n"
        "• 'Return rate this week'\n"
        "• 'Average order value by category'\n\n"
        "Or tell me what you want to know about your business."
    ),
}


def _clarification_response(session_id: str, intent: dict, message: str) -> dict:
    """Returns a safe, user-friendly clarification response. No SQL generated."""
    agent_log(
        session_id=session_id,
        question="",
        intent=intent,
        plan={},
        sql="",
        validation={"valid": False, "errors": ["Clarification needed"], "warnings": []},
        exec_result={"count": 0, "success": False},
        confidence="LOW",
        insight="",
        note=f"clarification:{intent.get('ambiguity')}",
    )
    return {
        "sql":                  "",
        "reasoning":            "",
        "confidence":           "LOW",
        "insight":              "",
        "rows":                 [],
        "row_count":            0,
        "chunks_used":          [],
        "validation":           {"valid": False, "errors": ["Clarification needed"], "warnings": []},
        "metric":               None,
        "stats":                {},
        "intent":               {k: v for k, v in intent.items() if k != "metric"},
        "plan":                 {},
        "session_id":           session_id,
        "clarification_needed": True,
        "note":                 message,
    }


def _compute_confidence(validation: dict, attempts: int,
                        row_count: int, intent: dict) -> str:
    if not validation.get("valid"):
        return "LOW"
    if attempts > 1:
        return "MEDIUM"
    if row_count == 0:
        return "MEDIUM"
    # HIGH if any time filter or filters are present (query is specific enough)
    if intent.get("time_filter") or intent.get("filters"):
        return "HIGH"
    if intent.get("operation") in ("top_n", "bottom_n", "compare"):
        return "HIGH"
    if intent.get("dimension"):
        return "HIGH"
    return "MEDIUM"


def _run_single(question: str, intent: dict, session: dict,
                session_id: str, skip_insight: bool) -> dict:
    """Core single-query pipeline. Extracted so _run_dual can call it twice."""
    plan = build_plan(intent)
    rag  = retrieve_context(question)

    metric_instruction = enforce_metric(intent.get("metric"))
    plan_instruction   = plan_to_prompt(plan)
    full_context = f"{plan_instruction}\n\nSUPPORTING RULES:\n{rag['context_text']}"
    if metric_instruction:
        full_context = f"{metric_instruction}\n\n{full_context}"

    gen        = {"sql": "", "reasoning": "", "confidence": "LOW", "raw": ""}
    validation = {"valid": False, "errors": ["Not attempted"], "warnings": []}
    attempts   = 0

    for attempt in range(MAX_RETRIES + 1):
        attempts = attempt + 1
        gen = generate_sql(question, full_context, session["history"], metric_instruction="")

        if gen.get("error"):
            return {
                "sql": "", "reasoning": "", "confidence": "LOW",
                "insight": gen["error"], "rows": [], "row_count": 0,
                "chunks_used": rag["chunks_used"],
                "validation": {"valid": False, "errors": [gen["error"]], "warnings": []},
                "metric": None, "stats": {}, "data_validation": {"mismatch": False},
                "intent": intent, "plan": {}, "session_id": session_id,
                "clarification_needed": False, "note": gen["error"],
            }

        safety = safety_enforce(gen["sql"])
        if not safety["safe"]:
            validation = {"valid": False, "errors": safety["errors"], "warnings": []}
            if attempt < MAX_RETRIES:
                full_context = f"SAFETY VIOLATION: {safety['errors']}\n\n{full_context}"
            continue
        gen["sql"] = safety["sql"]

        validation = rule_validate(gen["sql"])
        if not validation["valid"]:
            if attempt == MAX_RETRIES:
                break
            full_context = "PREVIOUS SQL FAILED RULES:\n" + "\n".join(validation["errors"]) + "\n\n" + full_context
            continue

        critic    = critique(gen["sql"])
        candidate = critic["fixed_sql"] or gen["sql"]
        safety2   = safety_enforce(candidate)
        candidate = safety2["sql"] if safety2["safe"] else gen["sql"]
        recheck   = rule_validate(candidate)
        if recheck["valid"]:
            gen["sql"] = candidate
            validation = recheck
            break
        if attempt < MAX_RETRIES:
            full_context = f"CRITIC REJECTION: {critic['reason']}\n\n{full_context}"

    exec_result = {"rows": [], "count": 0, "success": False, "error": None}
    if validation["valid"] and gen["sql"]:
        exec_result = execute_sql(gen["sql"])

    stats = compute_metrics(exec_result["rows"], intent.get("metric"))

    dv = {"verified": True, "skipped": True, "mismatch": False, "note": ""}
    if validation["valid"] and exec_result["success"] and exec_result["rows"]:
        dv = data_verify(exec_result["rows"], intent, gen["sql"])
        if dv["mismatch"]:
            correction = (
                f"DATA VALIDATION FAILED: your SQL returned {dv['agent_value']} "
                f"but verification returned {dv['verified_value']}. Fix the SQL."
            )
            gen_r = generate_sql(question, correction + "\n\n" + full_context,
                                 session["history"], metric_instruction="")
            s_r = safety_enforce(gen_r["sql"])
            if s_r["safe"]:
                v_r = rule_validate(s_r["sql"])
                if v_r["valid"]:
                    ex_r = execute_sql(s_r["sql"])
                    if ex_r["success"]:
                        dv_r = data_verify(ex_r["rows"], intent, s_r["sql"])
                        if not dv_r["mismatch"]:
                            gen["sql"] = s_r["sql"]; exec_result = ex_r
                            validation = v_r; stats = compute_metrics(exec_result["rows"], intent.get("metric"))
                            dv = dv_r; attempts += 1

    confidence = _compute_confidence(validation, attempts, exec_result["count"], intent)
    if dv["mismatch"]:
        confidence = "LOW"

    note = ""
    if not validation["valid"]:
        note = "Could not generate a valid query. Please rephrase or add more detail."
    elif dv["mismatch"]:
        note = dv["note"]
    elif exec_result["count"] == 0:
        note = "No data found for this query — try adjusting filters or time range."
    elif validation.get("warnings"):
        note = f"Warning: {validation['warnings'][0]}"

    insight = ""
    if not skip_insight and exec_result.get("rows") and not dv["mismatch"]:
        insight = generate_insight(question, stats)

    return {
        "sql": gen["sql"], "reasoning": gen["reasoning"], "confidence": confidence,
        "insight": insight, "rows": exec_result["rows"], "row_count": exec_result["count"],
        "chunks_used": rag["chunks_used"], "validation": validation,
        "metric": intent.get("metric"), "stats": stats, "data_validation": dv,
        "intent": {k: v for k, v in intent.items() if k != "metric"},
        "plan": {k: v for k, v in plan.items() if k != "compare_periods"},
        "session_id": session_id, "clarification_needed": False, "note": note,
    }


def _run_dual(question: str, session_id: str, intent: dict, skip_insight: bool) -> dict:
    """
    Handles queries like 'top 5 brands by quantity and revenue separately'.
    Runs two sub-queries, one per metric, and returns combined results.
    """
    from core.semantic_engine import _METRICS, _build
    session = get_session(session_id)
    dual = intent["dual_metrics"]  # e.g. ["quantity", "revenue"]

    # Map each term to a metric key
    _TERM_MAP = {
        "quantity": "quantity_sold", "units": "quantity_sold", "qty": "quantity_sold",
        "revenue": "gmv", "sales": "gmv", "gmv": "gmv", "sale": "gmv",
        "orders": "order_count", "order count": "order_count",
        "aov": "avg_order_value", "average order value": "avg_order_value",
    }

    results = []
    for term in dual:
        metric_key = _TERM_MAP.get(term.lower())
        if not metric_key or metric_key not in _METRICS:
            continue
        sub_intent = {**intent, "metric": _build(metric_key, _METRICS[metric_key]),
                      "dual_metrics": None}
        sub_q = f"{intent.get('operation','top')} {intent.get('limit',5)} brands by {term} {intent.get('time_filter','') or ''}".strip()
        r = _run_single(sub_q, sub_intent, session, session_id, skip_insight)
        results.append({"label": term, "result": r})

    if not results:
        return _clarification_response(session_id, intent,
            "Could not identify the metrics in your query. Try: 'Top 5 brands by revenue last year' or 'Top 5 brands by quantity last year'.")

    # Combine: primary result is first, secondary appended
    primary = results[0]["result"]
    combined_rows = []
    combined_sql  = []
    for r in results:
        label = r["label"].upper()
        for row in r["result"]["rows"]:
            combined_rows.append({"metric": label, **row})
        combined_sql.append(f"-- Top by {r['label']}\n{r['result']['sql']}")

    # Build combined insight
    insight = ""
    if not skip_insight and results:
        parts = []
        for r in results:
            if r["result"]["rows"]:
                top = r["result"]["rows"][0]
                top_val = list(top.values())[-1]
                top_label = list(top.values())[0]
                parts.append(f"{r['label']}: {top_label} leads with {top_val}")
        if parts:
            insight = "Here are the results broken down separately. " + " | ".join(parts) + "."

    # Log and save session
    structured_context = {
        "metric": "dual", "dimension": intent.get("dimension"),
        "time_filter": intent.get("time_filter"), "filters": intent.get("filters", {}),
        "operation": intent.get("operation"),
    }
    update_session(session_id, question, "\n\n".join(combined_sql), combined_rows,
                   "dual metric query", primary["chunks_used"], structured_context)

    return {
        "sql":                  "\n\n".join(combined_sql),
        "reasoning":            f"Dual query: {' and '.join(dual)}",
        "confidence":           min((r["result"]["confidence"] for r in results),
                                    key=lambda c: {"HIGH":2,"MEDIUM":1,"LOW":0}[c]),
        "insight":              insight,
        "rows":                 combined_rows,
        "row_count":            len(combined_rows),
        "chunks_used":          primary["chunks_used"],
        "validation":           {"valid": True, "errors": [], "warnings": []},
        "metric":               None,
        "stats":                {},
        "data_validation":      {"verified": True, "skipped": True, "mismatch": False, "note": ""},
        "intent":               {k: v for k, v in intent.items() if k != "metric"},
        "plan":                 {},
        "session_id":           session_id,
        "clarification_needed": False,
        "note":                 "",
        "dual_results":         [{"label": r["label"], "rows": r["result"]["rows"],
                                  "sql": r["result"]["sql"]} for r in results],
    }


def run(question: str, session_id: str = "default", skip_insight: bool = False) -> dict:
    session = get_session(session_id)
    last_context = session.get("last_context", {})

    # ── Step 1: Parse intent with advanced understanding ────────────────────
    from core.intent_parser_enhanced import parse_enhanced
    intent = parse_enhanced(question, last_context)

    # ── Step 1b: Handle greetings ────────────────────────────────────────
    if intent.get("operation") == "greeting":
        # Use LLM for natural, context-aware greeting response
        try:
            from core.insight_engine import _client, _call_with_retry
            resp = _call_with_retry(lambda: _client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{
                    "role": "system",
                    "content": "You are QueryMind, a friendly AI analytics assistant for an Indian ecommerce company. Respond naturally to greetings in 1-2 sentences. Always mention you can help with business data questions. Be warm and conversational."
                }, {
                    "role": "user",
                    "content": question
                }],
                temperature=0.7,
                max_tokens=80,
            ))
            greeting_msg = resp.choices[0].message.content.strip()
        except Exception:
            greeting_msg = "Hey! I'm QueryMind, your AI analytics assistant. Ask me anything about your business data!"

        return {
            "sql": "", "reasoning": "", "confidence": "HIGH",
            "insight": greeting_msg,
            "rows": [], "row_count": 0, "chunks_used": [],
            "validation": {"valid": True, "errors": [], "warnings": []},
            "metric": None, "stats": {}, "data_validation": {"mismatch": False},
            "intent": intent, "plan": {}, "session_id": session_id,
            "clarification_needed": False, "note": "",
        }

    # ── Step 2: Ambiguity fail-safe — never guess ────────────────────────────
    ambiguity = intent.get("ambiguity")
    if ambiguity and ambiguity in _CLARIFICATIONS:
        return _clarification_response(session_id, intent, _CLARIFICATIONS[ambiguity])

    # ── Step 3: Build plan ───────────────────────────────────────────────────
    plan = build_plan(intent)

    # ── Step 4: RAG context ──────────────────────────────────────────────────
    rag = retrieve_context(question)

    # ── Step 5: Build full LLM context ──────────────────────────────────────
    metric_instruction = enforce_metric(intent.get("metric"))
    plan_instruction   = plan_to_prompt(plan)
    full_context = f"{plan_instruction}\n\nSUPPORTING RULES:\n{rag['context_text']}"
    if metric_instruction:
        full_context = f"{metric_instruction}\n\n{full_context}"

    # ── Step 6: Generate + safety + validate + retry ─────────────────────────
    gen        = {"sql": "", "reasoning": "", "confidence": "LOW", "raw": ""}
    validation = {"valid": False, "errors": ["Not attempted"], "warnings": []}
    attempts   = 0

    for attempt in range(MAX_RETRIES + 1):
        attempts = attempt + 1
        gen = generate_sql(question, full_context, session["history"], metric_instruction="", intent=intent)

        # If SQL generator returned an error (e.g. rate limit), return gracefully
        if gen.get("error"):
            return {
                "sql": "", "reasoning": "", "confidence": "LOW",
                "insight": gen["error"],
                "rows": [], "row_count": 0, "chunks_used": rag["chunks_used"],
                "validation": {"valid": False, "errors": [gen["error"]], "warnings": []},
                "metric": None, "stats": {}, "data_validation": {"mismatch": False},
                "intent": intent, "plan": {}, "session_id": session_id,
                "clarification_needed": False, "note": gen["error"],
            }

        # 6a. Safety guard — block DDL/DML, enforce LIMIT
        safety = safety_enforce(gen["sql"])
        if not safety["safe"]:
            validation = {"valid": False, "errors": safety["errors"], "warnings": []}
            if attempt < MAX_RETRIES:
                full_context = f"SAFETY VIOLATION: {safety['errors']}\n\n{full_context}"
            continue
        gen["sql"] = safety["sql"]  # use limit-enforced version

        # 6b. Hard rule engine (YAML-driven, no LLM)
        validation = rule_validate(gen["sql"])
        if not validation["valid"]:
            if attempt == MAX_RETRIES:
                break
            full_context = "PREVIOUS SQL FAILED RULES:\n" + "\n".join(validation["errors"]) + "\n\n" + full_context
            continue

        # 6c. SQL critic (LLM second pass)
        critic    = critique(gen["sql"])
        candidate = critic["fixed_sql"] or gen["sql"]

        # Safety-check critic output too
        safety2 = safety_enforce(candidate)
        candidate = safety2["sql"] if safety2["safe"] else gen["sql"]

        recheck = rule_validate(candidate)
        if recheck["valid"]:
            gen["sql"] = candidate
            validation = recheck
            break

        if attempt < MAX_RETRIES:
            full_context = f"CRITIC REJECTION: {critic['reason']}\n\n{full_context}"

    # ── Step 7: Execute ──────────────────────────────────────────────────────
    exec_result = {"rows": [], "count": 0, "success": False, "error": None}
    if validation["valid"] and gen["sql"]:
        exec_result = execute_sql(gen["sql"])

    # ── Step 8: Stats ────────────────────────────────────────────────────────
    stats = compute_metrics(exec_result["rows"], intent.get("metric"))

    # ── Step 8b: Data validation — recheck result against Supabase ───────────
    dv = {"verified": True, "skipped": True, "mismatch": False, "note": ""}
    if validation["valid"] and exec_result["success"] and exec_result["rows"]:
        dv = data_verify(exec_result["rows"], intent, gen["sql"])

        # Mismatch detected — retry once with explicit correction instruction
        if dv["mismatch"]:
            correction = (
                f"DATA VALIDATION FAILED: your SQL returned {dv['agent_value']} "
                f"but an independent verification query returned {dv['verified_value']}. "
                f"The aggregation or filters are wrong. Rewrite the SQL to match "
                f"the metric definition exactly."
            )
            gen_r = generate_sql(question, correction + "\n\n" + full_context,
                                 session["history"], metric_instruction="")
            s_r = safety_enforce(gen_r["sql"])
            if s_r["safe"]:
                v_r = rule_validate(s_r["sql"])
                if v_r["valid"]:
                    ex_r = execute_sql(s_r["sql"])
                    if ex_r["success"]:
                        dv_r = data_verify(ex_r["rows"], intent, s_r["sql"])
                        if not dv_r["mismatch"]:
                            gen["sql"]  = s_r["sql"]
                            exec_result = ex_r
                            validation  = v_r
                            stats       = compute_metrics(exec_result["rows"], intent.get("metric"))
                            dv          = dv_r
                            attempts   += 1

    # ── Step 9: Confidence score ─────────────────────────────────────────────
    confidence = _compute_confidence(validation, attempts, exec_result["count"], intent)
    # Downgrade confidence if data validation found a mismatch that couldn't be fixed
    if dv["mismatch"]:
        confidence = "LOW"

    # ── Step 10: User-facing note ────────────────────────────────────────────
    note = ""
    if not validation["valid"]:
        note = "Could not generate a valid query. Please rephrase or add more detail."
    elif dv["mismatch"]:
        note = dv["note"]
    elif exec_result["count"] == 0:
        note = "No data found for this query — try adjusting filters or time range."
    elif validation.get("warnings"):
        note = f"Warning: {validation['warnings'][0]}"

    # ── Step 11: Insight ─────────────────────────────────────────────────────
    insight = ""
    if not skip_insight and exec_result.get("rows") and not dv["mismatch"]:
        insight = generate_insight(question, stats)

    # ── Step 12: Session context ─────────────────────────────────────────────
    structured_context = {
        "metric":      intent["metric"]["metric"] if intent.get("metric") else None,
        "dimension":   intent.get("dimension"),
        "time_filter": intent.get("time_filter"),
        "filters":     intent.get("filters", {}),
        "operation":   intent.get("operation"),
    }
    update_session(session_id, question, gen["sql"], exec_result["rows"],
                   gen["reasoning"], rag["chunks_used"], structured_context)

    # ── Step 13: Log ─────────────────────────────────────────────────────────
    agent_log(
        session_id=session_id,
        question=question,
        intent=structured_context,
        plan={k: v for k, v in plan.items() if k != "compare_periods"},
        sql=gen["sql"],
        validation=validation,
        exec_result=exec_result,
        confidence=confidence,
        insight=insight,
        note=note,
    )

    # ── Step 14: Clean response ──────────────────────────────────────────────
    return {
        "sql":                  gen["sql"],
        "reasoning":            gen["reasoning"],
        "confidence":           confidence,
        "insight":              insight,
        "rows":                 exec_result["rows"],
        "row_count":            exec_result["count"],
        "chunks_used":          rag["chunks_used"],
        "validation":           validation,
        "metric":               intent.get("metric"),
        "stats":                stats,
        "data_validation":      dv,
        "intent":               {k: v for k, v in intent.items() if k != "metric"},
        "plan":                 {k: v for k, v in plan.items() if k != "compare_periods"},
        "session_id":           session_id,
        "clarification_needed": False,
        "note":                 note,
    }
