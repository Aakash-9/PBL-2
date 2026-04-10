# core/rag_engine.py
"""
RAG engine: indexes YAML business rules and retrieves relevant chunks.
Uses TF-IDF keyword matching — no PyTorch, no CUDA, no onnxruntime.
Works on any machine regardless of RAM/GPU.
"""
import os
import re
import yaml
import math
from collections import defaultdict

_chunks = []       # list of {id, text, source}
_tfidf  = {}       # term -> {chunk_id -> tf-idf score}
_built  = False

YAML_DIR = os.path.join(os.path.dirname(__file__), "..", "yaml")


def _tokenize(text: str) -> list:
    return re.findall(r"[a-z0-9_]+", text.lower())


def _build_index():
    global _chunks, _tfidf, _built
    _chunks = []

    # --- metric_glossary.yaml ---
    path = os.path.join(YAML_DIR, "metric_glossary.yaml")
    if os.path.exists(path):
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        for name, defn in data.items():
            fl = defn.get("formula_logic", {})
            text = (
                f"metric_name:{name} "
                f"description:{defn.get('description','')} "
                f"filter:{fl.get('filter','')} "
                f"aggregation:{fl.get('aggregation','')} "
                f"tables:{','.join(defn.get('tables',[]))}"
            )
            _chunks.append({"id": f"metric_{name}", "text": text, "source": "metric_glossary"})

    # --- join_path_specification.yaml ---
    path = os.path.join(YAML_DIR, "join_path_specification.yaml")
    if os.path.exists(path):
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        for name, p in data.get("core_paths", {}).items():
            text = (
                f"join_path:{name} "
                f"from_table:{p.get('from','')} "
                f"to_table:{p.get('to','')} "
                f"condition:{p.get('condition','')}"
            )
            _chunks.append({"id": f"join_{name}", "text": text, "source": "join_paths"})
        for rule in data.get("enforced_rules", []):
            _chunks.append({"id": f"join_rule_{abs(hash(rule))}", "text": f"join_constraint:{rule}", "source": "join_paths"})

    # --- sql_generation_standards.yaml ---
    path = os.path.join(YAML_DIR, "sql_generation_standards.yaml")
    if os.path.exists(path):
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        for i, rule in enumerate(data.get("rules", [])):
            _chunks.append({"id": f"sql_std_{i}", "text": f"sql_standard:{rule}", "source": "sql_standards"})

    # --- time_filter_governance.yaml ---
    path = os.path.join(YAML_DIR, "time_filter_governance.yaml")
    if os.path.exists(path):
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        for name, val in data.get("time_filters", {}).items():
            text = f"time_filter:{name} sql_expression:{val.get('sql','')}"
            _chunks.append({"id": f"time_{name}", "text": text, "source": "time_filters"})

    # Build TF-IDF index
    N = len(_chunks)
    df = defaultdict(int)  # document frequency per term

    # Count document frequencies
    for chunk in _chunks:
        terms = set(_tokenize(chunk["text"]))
        for term in terms:
            df[term] += 1

    # Build TF-IDF scores
    _tfidf = defaultdict(dict)
    for chunk in _chunks:
        tokens = _tokenize(chunk["text"])
        tf = defaultdict(int)
        for t in tokens:
            tf[t] += 1
        for term, count in tf.items():
            tf_score  = count / len(tokens)
            idf_score = math.log(N / (df[term] + 1)) + 1
            _tfidf[term][chunk["id"]] = tf_score * idf_score

    _built = True
    print(f"[RAG] Indexed {len(_chunks)} business rule chunks (keyword mode)")


def _ensure_built():
    if not _built:
        _build_index()


def retrieve_context(user_query: str, top_k: int = 7) -> dict:
    """Return top-k relevant chunks using TF-IDF keyword matching."""
    _ensure_built()

    query_terms = _tokenize(user_query)
    scores = defaultdict(float)

    for term in query_terms:
        if term in _tfidf:
            for chunk_id, score in _tfidf[term].items():
                scores[chunk_id] += score

    # Sort by score descending
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

    # Build chunk lookup
    chunk_map = {c["id"]: c for c in _chunks}

    # Always include at least the top metric and join chunks even if score=0
    result_ids = [cid for cid, _ in ranked]

    # Fill remaining slots with high-priority chunks if not already included
    priority_ids = [c["id"] for c in _chunks if c["source"] in ("metric_glossary", "join_paths")]
    for pid in priority_ids:
        if pid not in result_ids and len(result_ids) < top_k:
            result_ids.append(pid)

    docs  = [chunk_map[cid]["text"]   for cid in result_ids if cid in chunk_map]
    metas = [chunk_map[cid]["source"] for cid in result_ids if cid in chunk_map]
    ids   = [cid for cid in result_ids if cid in chunk_map]

    return {
        "context_text": "\n".join(docs),
        "chunks_used": [
            {"id": ids[i], "source": metas[i], "text": docs[i]}
            for i in range(len(docs))
        ],
    }


def rebuild_index():
    """Force rebuild — call after editing YAMLs."""
    global _built
    _built = False
    _build_index()
    return {"indexed": len(_chunks)}
