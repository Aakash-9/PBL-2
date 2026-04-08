# core/rag_engine.py — replaces context_selector + llm_normalizer + spell_corrector
"""
RAG engine: indexes YAML business rules into ChromaDB,
retrieves semantically relevant chunks for any user query.
Eliminates all keyword-matching fragility.
"""
import os
import yaml
import chromadb
from sentence_transformers import SentenceTransformer

_model = None
_collection = None

YAML_DIR = os.path.join(os.path.dirname(__file__), "..", "yaml")


def _get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


def _get_collection():
    global _collection
    if _collection is None:
        client = chromadb.Client()
        _collection = client.get_or_create_collection("business_rules")
        if _collection.count() == 0:
            _build_index(_collection)
    return _collection


def _build_index(collection):
    """Parse all YAML files and index each rule as a separate chunk."""
    chunks = []

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
            chunks.append({"id": f"metric_{name}", "text": text, "source": "metric_glossary"})

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
            chunks.append({"id": f"join_{name}", "text": text, "source": "join_paths"})
        for rule in data.get("enforced_rules", []):
            chunks.append({"id": f"join_rule_{hash(rule)}", "text": f"join_constraint:{rule}", "source": "join_paths"})

    # --- sql_generation_standards.yaml ---
    path = os.path.join(YAML_DIR, "sql_generation_standards.yaml")
    if os.path.exists(path):
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        for i, rule in enumerate(data.get("rules", [])):
            chunks.append({"id": f"sql_std_{i}", "text": f"sql_standard:{rule}", "source": "sql_standards"})

    # --- time_filter_governance.yaml ---
    path = os.path.join(YAML_DIR, "time_filter_governance.yaml")
    if os.path.exists(path):
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        for name, val in data.get("time_filters", {}).items():
            text = f"time_filter:{name} sql_expression:{val.get('sql','')}"
            chunks.append({"id": f"time_{name}", "text": text, "source": "time_filters"})

    # --- dataset_scope.yaml ---
    path = os.path.join(YAML_DIR, "dataset_scope.yaml")
    if os.path.exists(path):
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        for item in data.get("scope_items", []):
            chunks.append({"id": f"scope_{hash(str(item))}", "text": str(item), "source": "dataset_scope"})

    if not chunks:
        return

    model = _get_model()
    embeddings = model.encode([c["text"] for c in chunks]).tolist()
    collection.add(
        documents=[c["text"] for c in chunks],
        embeddings=embeddings,
        ids=[c["id"] for c in chunks],
        metadatas=[{"source": c["source"]} for c in chunks],
    )
    print(f"[RAG] Indexed {len(chunks)} business rule chunks")


def retrieve_context(user_query: str, top_k: int = 7) -> dict:
    """Return top-k relevant chunks + their metadata."""
    collection = _get_collection()
    model = _get_model()
    embedding = model.encode([user_query]).tolist()
    results = collection.query(query_embeddings=embedding, n_results=min(top_k, collection.count()))
    docs = results["documents"][0] if results["documents"] else []
    metas = results["metadatas"][0] if results["metadatas"] else []
    ids = results["ids"][0] if results["ids"] else []
    return {
        "context_text": "\n".join(docs),
        "chunks_used": [{"id": ids[i], "source": metas[i].get("source",""), "text": docs[i]} for i in range(len(docs))],
    }


def rebuild_index():
    """Force rebuild — call after editing YAMLs."""
    global _collection
    client = chromadb.Client()
    try:
        client.delete_collection("business_rules")
    except Exception:
        pass
    _collection = client.create_collection("business_rules")
    _build_index(_collection)
    return {"indexed": _collection.count()}
