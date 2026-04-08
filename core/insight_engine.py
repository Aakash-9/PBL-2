# core/insight_engine.py
import os
import time
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

_client = OpenAI(
    api_key=os.environ.get("CEREBRAS_API_KEY", ""),
    base_url="https://api.groq.com/openai/v1",
)


def _call_with_retry(fn, max_retries=3, base_wait=5):
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower() or "queue" in str(e).lower():
                if attempt < max_retries - 1:
                    time.sleep(base_wait * (2 ** attempt))
                    continue
            raise
    raise RuntimeError("Max retries exceeded")

# ── Prompt for single aggregate result (e.g. "GMV last month") ───────────────
_SINGLE_PROMPT = """\
You are a sharp, friendly business analyst helping an Indian ecommerce team understand their data.
All monetary values are in Indian Rupees (INR). Never use $ or USD.
The user asked: "{question}"
The answer is: {total}

Respond conversationally — like a colleague explaining a number over Slack, not a formal report.
- If the value is 0: say the selected time period likely falls outside the available data range, and suggest trying 'last 6 months' or 'this year' instead. Do NOT speculate about data issues or system problems.
- If the value is meaningful: state it clearly using ₹ symbol, put it in context, and suggest one thing worth watching.
- 2-3 sentences max. Be direct. Use the actual number. No bullet points. No headers.
- Do NOT say "the data shows", "based on the data", or "it's worth noting".
"""

# ── Prompt for multi-row results (e.g. "Top 5 cities by revenue") ────────────
_MULTI_PROMPT = """\
You are a sharp, friendly business analyst helping an Indian ecommerce team understand their data.
All monetary values are in Indian Rupees (INR). Always use ₹ symbol, never $ or USD.
The user asked: "{question}"

Here is the COMPLETE data (use ALL columns, not just one):
{raw_data}

Key stats:
- Top performers: {top_contributors}
- Anything unusual: {anomaly}
- Number of results: {row_count}

Rules:
- Use the EXACT numbers from the raw data above. Never invent or approximate numbers.
- If there are multiple numeric columns (e.g. return_rate AND revenue_lost), mention BOTH.
- Use ₹ symbol for monetary values. Use % for rates/percentages.
- Lead with the most interesting finding.
- Mention specific city/brand/seller names with their actual numbers.
- End with one practical suggestion.
- 3-4 sentences max. No bullet points. No headers. No "the data shows".
- Write conversationally like a colleague, not a formal report.
"""

# ── Prompt for period comparison (exactly 2 rows) ────────────────────────────
_COMPARE_PROMPT = """\
You are a sharp, friendly business analyst helping an Indian ecommerce team understand their data.
All monetary values are in Indian Rupees (INR). Always use ₹ symbol, never $ or USD.
The user asked: "{question}"

Comparison result:
- This period: {curr_val} ({curr_label})
- Previous period: {prev_val} ({prev_label})
- Change: {growth_pct}

Respond conversationally — like a colleague giving a quick update.
- If both values are 0, say clearly that there were no qualifying orders in either period and suggest checking a broader time range. Do NOT invent reasons or mention products.
- If there is a real change, state clearly whether it went up or down and by how much using ₹, give one possible reason, and suggest one action.
- 2-3 sentences. No bullet points. No headers. Talk naturally. Use only the numbers given.
"""


def generate(question: str, stats: dict) -> str:
    row_count = stats.get("row_count", 0)

    if row_count == 0:
        return (
            f"Looks like there's no data matching your query for \"{question}\". "
            "This usually means the selected time period falls outside the dataset's range, "
            "or the filters are too narrow. Try a broader date range like 'this year' or 'last 6 months'."
        )

    total        = stats.get("total")
    contributors = stats.get("top_contributors") or []
    growth_pct   = stats.get("growth_pct")
    anomaly      = stats.get("anomaly")
    rows         = stats.get("_raw_rows", [])

    # ── Period comparison (exactly 2 rows with period labels) ────────────────
    is_compare = row_count == 2 and rows and stats.get("label_col")
    if is_compare:
        prev_row    = rows[0]
        curr_row    = rows[1]
        numeric_col = stats.get("numeric_col", "value")
        label_col   = stats.get("label_col", "period")
        prev_val    = prev_row.get(numeric_col, 0)
        curr_val    = curr_row.get(numeric_col, 0)
        prev_label  = prev_row.get(label_col, "previous")
        curr_label  = curr_row.get(label_col, "current")
        direction   = "up" if float(curr_val or 0) >= float(prev_val or 0) else "down"
        gp_text = f"{'+' if direction == 'up' else ''}{growth_pct}%" if growth_pct is not None else "no change (both are 0)"
        prompt = _COMPARE_PROMPT.format(
            question=question,
            curr_val=_fmt(curr_val), curr_label=curr_label,
            prev_val=_fmt(prev_val), prev_label=prev_label,
            growth_pct=gp_text,
        )

    # ── Single aggregate (no breakdown) ─────────────────────────────────────
    elif row_count == 1 and not contributors:
        prompt = _SINGLE_PROMPT.format(
            question=question,
            total=_fmt(total) if total is not None else "0",
        )

    # ── Multi-row with breakdown ─────────────────────────────────────────────
    else:
        top_text = ", ".join(
            f"{c['label']} ({_fmt(c['value'])})"
            for c in contributors[:3]
        ) if contributors else "N/A"

        # Pass ALL raw rows with correct formatting per column type
        _RATE_KEYS = {"rate", "pct", "percent"}
        def _fmt_val(k, v):
            if isinstance(v, (int, float)):
                return f"{round(v, 2)}%" if any(r in k.lower() for r in _RATE_KEYS) else _fmt(v)
            return str(v)

        raw_data_text = "\n".join(
            "  " + ", ".join(f"{k}: {_fmt_val(k, v)}" for k, v in row.items())
            for row in rows[:10]
        )

        prompt = _MULTI_PROMPT.format(
            question=question,
            raw_data=raw_data_text,
            top_contributors=top_text,
            anomaly=anomaly or "nothing unusual",
            row_count=row_count,
        )

    for model in ("llama-3.3-70b-versatile", "llama-3.1-8b-instant", "gemma2-9b-it"):
        try:
            resp = _call_with_retry(lambda m=model: _client.chat.completions.create(
                model=m,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.4,
                max_tokens=300,
            ))
            return resp.choices[0].message.content.strip()
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower():
                continue
            raise
    return "Insight unavailable — API rate limit reached. Please try again in a few minutes."


def _fmt(val) -> str:
    """Format a number as Indian Rupees — ₹1,60,9405.33 style (Indian numbering)."""
    try:
        f = float(val)
        # Indian numbering: last 3 digits, then groups of 2
        if f == int(f):
            return "\u20b9" + _indian_format(int(f))
        whole = int(f)
        dec   = round(f - whole, 2)
        dec_str = f"{dec:.2f}"[1:]  # ".33"
        return "\u20b9" + _indian_format(whole) + dec_str
    except (TypeError, ValueError):
        return str(val)


def _indian_format(n: int) -> str:
    """Format integer with Indian comma grouping: 1,60,94,05 style."""
    s = str(abs(n))
    prefix = "-" if n < 0 else ""
    if len(s) <= 3:
        return prefix + s
    # Last 3 digits, then groups of 2
    result = s[-3:]
    s = s[:-3]
    while s:
        result = s[-2:] + "," + result
        s = s[:-2]
    return prefix + result
