"""The search index for published datasets, in VAST DB.

  fair.search     one row per published dataset: its text and an embedding in a vector-indexed column,
                  for searching by meaning ("satellite ocean temperature")
  fair.variables  one row per variable: name, units, CF standard name, description, for exact questions
                  ("every dataset with sea_surface_temperature")

Both are rewritten for a dataset each time it is published. Queries need the cluster's data network
(vector search runs on the VAST Query Engine, which hands back VIP addresses), so they run in the
DataEngine function and in the portal's pod, which can reach it.
"""
from __future__ import annotations

import datetime as dt
import os
import re

import pyarrow as pa

from . import llm
from .curate import accepted

DIM = 1024  # tested with Qwen3-Embedding-0.6B and BGE-M3, both 1024 dimensions
MAX_TEXT = 1200
VECTOR = pa.list_(pa.field("", pa.float32(), False), DIM)
SEARCH = pa.schema([("folder", pa.string()), ("pid", pa.string()), ("title", pa.string()), ("domain", pa.string()),
                    ("keywords", pa.string()), ("description", pa.string()), ("updated_at", pa.string()),
                    ("embedding", VECTOR)])
VARIABLES = pa.schema([("folder", pa.string()), ("pid", pa.string()), ("dataset_title", pa.string()),
                       ("name", pa.string()), ("units", pa.string()), ("standard_name", pa.string()),
                       ("description", pa.string()), ("source", pa.string())])


def embed(text: str) -> list[float]:
    """Embedding scaled to unit length, whatever the model returns, so distance maps exactly to cosine."""
    while True:
        try:
            vec = llm.embed([text])[0]
            break
        except llm.LLMError as e:  # too long for the embedding server: keep the first half and retry
            if "502" not in str(e) or len(text) < 300:
                raise
            text = text[: len(text) // 2]
    if len(vec) != DIM:
        raise ValueError("embedding has %d dims, the index expects %d (FAIR_EMBED_MODEL)" % (len(vec), DIM))
    norm = sum(x * x for x in vec) ** 0.5 or 1.0
    return [x / norm for x in vec]


# Qwen3-Embedding is trained with an instruction on the query side only (documents are embedded as they are).
# Measured on the ten demo folders: plain queries put the right dataset first for 10 of 14 test queries,
# instructed ones for 14 of 14, with 2.5x the margin over the best unrelated dataset.
QUERY_PREFIX = os.environ.get("FAIR_EMBED_QUERY_PREFIX",
                              "Instruct: Given a search query, retrieve research datasets that match it\nQuery: ")
# Short queries ("ocean", "traffic") gain from also counting their words in the title, keywords, domain and
# description: +0.10 at most, which lifted the worst-case margin from 0.02 to 0.09 in the same test.
WORD_BOOST = 0.10


def embed_query(query: str) -> list[float]:
    return embed(QUERY_PREFIX + query)


def rank(query: str, rows: list[dict]) -> list[dict]:
    """Best first. Each row's `similarity` (cosine) becomes `meaning`, plus `words` (the share of the query's
    words found in its title, keywords, domain or description); `similarity` is then meaning + WORD_BOOST x words."""
    words = [w for w in re.findall(r"[a-z0-9]+", query.lower()) if len(w) > 2]
    for r in rows:
        text = " ".join(str(r.get(k) or "") for k in ("title", "keywords", "domain"))
        text = (text + " " + str(r.get("description") or "")[:400]).lower()
        found = sum(w in text for w in words) / len(words) if words else 0.0
        r["meaning"], r["words"] = r["similarity"], round(found, 2)
        r["similarity"] = round(r["similarity"] + WORD_BOOST * found, 3)
    return sorted(rows, key=lambda r: -r["similarity"])


def ensure_tables(schema) -> None:
    from vastdb._internal import VectorIndexSpec
    if not schema.table("search", fail_if_missing=False):
        # Vectors are stored at unit length (embed), so squared L2 ranks exactly as cosine similarity.
        schema.create_table("search", SEARCH, vector_index=VectorIndexSpec("embedding", "l2sq"))
    if not schema.table("variables", fail_if_missing=False):
        schema.create_table("variables", VARIABLES)


def variable_rows(record: dict, pid: str, title: str) -> list[dict]:
    rows = []
    for v in record["variables"]:
        d = v.get("description") or {}
        inferred = d.get("value") if d.get("status") == "accepted" else None
        rows.append({"folder": record["key_prefix"], "pid": pid, "dataset_title": title, "name": v["name"],
                     "units": v.get("units") or v.get("unit_inferred"), "standard_name": v.get("standard_name"),
                     "description": v.get("long_name") or inferred,
                     "source": "header" if v.get("long_name") or v.get("standard_name") else
                               "inferred" if inferred else None})
    return rows


def document_text(record: dict) -> str:
    """What a dataset is about, as one passage: the text its embedding is made from."""
    a = accepted(record)
    parts = [a.get("title") or record["name"], a.get("description") or "",
             "Keywords: " + ", ".join(a.get("keywords") or []), "Domain: %s" % (a.get("domain") or "")]
    for key in ("measurementTechnique", "instrument", "temporalCoverage", "spatialCoverage"):
        if a.get(key):
            parts.append("%s: %s" % (key, a[key]))
    names = []
    for v in record["variables"][:150]:
        d = v.get("long_name") or ((v.get("description") or {}).get("value")
                                   if (v.get("description") or {}).get("status") == "accepted" else "")
        names.append(v["name"] + (" (%s)" % d if d else "") + (" [%s]" % v["standard_name"] if v.get("standard_name") else ""))
    if names:
        parts.append("Variables: " + "; ".join(names))
    # The lab gateway's former bge-m3 server rejected inputs much over ~1,500 characters (its batch limit), and the
    # passage leads with what matters most; variables are searched exactly in fair.variables anyway.
    return "\n".join(p for p in parts if p)[:MAX_TEXT]


def _delete_folder(table, folder: str) -> None:
    from ibis import _
    old = table.select(columns=["folder"], predicate=(_.folder == folder), internal_row_id=True).read_all()
    if old.num_rows:
        table.delete(old)


def index_record(session, bucket: str, schema_name: str, record: dict) -> dict:
    """Replace the dataset's rows in fair.search and fair.variables. Returns what was written."""
    a = accepted(record)
    title = a.get("title") or record["name"]
    vec = embed(document_text(record))
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    folder = record["key_prefix"]
    row = {"folder": folder, "pid": record.get("pid"), "title": title, "domain": a.get("domain"),
           "keywords": ", ".join(a.get("keywords") or []), "description": (a.get("description") or "")[:2000],
           "updated_at": now, "embedding": vec}
    variables = variable_rows(record, record.get("pid"), title)
    with session.transaction() as tx:
        sch = tx.bucket(bucket).schema(schema_name)
        ensure_tables(sch)
        search, var_table = sch.table("search"), sch.table("variables")
        _delete_folder(search, folder)
        _delete_folder(var_table, folder)
        search.insert(pa.RecordBatch.from_pylist([row], schema=SEARCH))
        if variables:
            var_table.insert(pa.RecordBatch.from_pylist(variables, schema=VARIABLES))
    return {"search": 1, "variables": len(variables)}


COLUMNS = ["folder", "pid", "title", "domain", "keywords", "description"]


def search(session, bucket: str, schema_name: str, query: str, limit: int = 10) -> tuple[list[dict], str]:
    """Datasets closest in meaning to the query, nearest first, with a 0-1 cosine similarity, and which
    engine answered: VAST DB's vector index, or (if the query engine refuses) the same stored vectors
    read with a plain select and ranked here."""
    vec = embed_query(query)
    with session.transaction() as tx:
        table = tx.bucket(bucket).schema(schema_name).table("search")
        try:
            # A few more than asked for, so the word boost can reorder near neighbours.
            got = table.vector_search(vec=vec, columns=COLUMNS, limit=limit * 3).read_all().to_pylist()
            engine = "VAST DB vector index"
            for r in got:
                # unit vectors: squared L2 distance d = 2 - 2·cos, so cos = 1 - d/2
                r["similarity"] = round(max(0.0, 1 - r.pop("vast_pysdk_vector_dist") / 2), 3)
            return rank(query, got)[:limit], engine
        except Exception as e:  # noqa: BLE001
            reason = str(e).split("message:")[-1].strip()[:120]
            rows = table.select(columns=COLUMNS + ["embedding"]).read_all().to_pylist()
    for r in rows:
        r["similarity"] = round(sum(a * b for a, b in zip(vec, r.pop("embedding"))), 3)
    return rank(query, rows)[:limit], "VAST DB vectors, ranked in the portal (vector query refused: %s)" % reason


def find_variables(session, bucket: str, schema_name: str, text: str, limit: int = 200) -> list[dict]:
    """Variables whose name, standard name, units or description contain the text (case-insensitive)."""
    with session.transaction() as tx:
        rows = tx.bucket(bucket).schema(schema_name).table("variables").select().read_all().to_pylist()
    return match_variables(rows, text)[:limit]


def _norm(s: str | None) -> str:
    return (s or "").lower().replace("_", " ").strip()


def match_variables(rows: list[dict], text: str) -> list[dict]:
    """Case-insensitive, with underscores and spaces treated alike, so the CF name
    sea_surface_temperature also finds a variable described as "Daily sea surface temperature"."""
    t = _norm(text)
    hits = [r for r in rows if any(t in _norm(r.get(k)) for k in ("name", "standard_name", "units", "description"))]
    # exact standard-name matches first: they are the controlled-vocabulary answer
    hits.sort(key=lambda r: (_norm(r.get("standard_name")) != t, r.get("dataset_title") or "", r["name"]))
    return hits
