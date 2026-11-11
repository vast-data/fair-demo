"""FAIR portal: the curator's inbox, the review screen, and the public landing pages.

    .venv/bin/uvicorn portal.app:app --port 8080        # FAIR_STORE=local|cluster (see store.py)

The portal never curates or publishes itself. It shows what the pipeline proposed, records a person's
decisions as _fair/review.json, and the pipeline does the rest.
"""
from __future__ import annotations

import copy
import json
import datetime as dt
import math
import os
import threading
import time
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, Response

ROOT = Path(__file__).resolve().parents[1]
# .env before the fairdemo imports: fairdemo.llm reads its model settings when it is imported.
for line in (ROOT / ".env").read_text().splitlines() if (ROOT / ".env").exists() else []:
    k, sep, v = line.partition("=")
    if sep and not k.startswith("#"):
        os.environ.setdefault(k.strip(), v.split(" #", 1)[0].strip())

from fairdemo import catalog, curate, fairscore, federation, harvest, llm, nlquery, rocrate, sweep, tables, theme  # noqa: E402
from portal import store as stores  # noqa: E402

from contextlib import asynccontextmanager  # noqa: E402


@asynccontextmanager
async def _lifespan(_app):
    _start_background()   # Site B's seed, and harvesting partners (below)
    yield


app = FastAPI(title="FAIR portal", lifespan=_lifespan)
store = stores.from_env()


def summary(r: dict) -> dict:
    a = curate.accepted(r)
    title = a.get("title") or (r["fields"].get("title") or {}).get("value")
    needs = [k for k, f in r["fields"].items() if f["status"] in ("proposed", "missing")]
    missing = [k for k in rocrate.REQUIRED_TO_PUBLISH if not a.get(k)]
    return {"folder": r.get("key_prefix") or r["name"], "name": r["name"], "status": r.get("status", "proposed"),
            "title": title, "domain": (r["fields"].get("domain") or {}).get("value"),
            "keywords": (r["fields"].get("keywords") or {}).get("value") or [],
            "description": (r["fields"].get("description") or {}).get("value"),
            "score_before": r.get("score_before"), "score_after": r.get("score_after"),
            "pid": r.get("pid"), "files": len(r["files"]), "needs": needs, "missing": missing,
            "generated": r.get("generated"), "published": r.get("published")}


def record_for(folder: str) -> dict:
    try:
        return store.get_json(f"{folder}/_fair/record.json")
    except Exception:
        raise HTTPException(404, f"no record for {folder}")


@app.get("/", response_class=HTMLResponse)
def index():
    page = (ROOT / "portal/static/index.html").read_text()
    if theme.name() != "teal" or theme.logo() != "F":
        # A partner site's own look (FAIR_SITE_THEME, FAIR_SITE_LOGO): its palette after the page's own, so it wins.
        page = page.replace("</head>", "<style>%s</style>\n</head>" % theme.css(), 1)
        page = page.replace('<span class="logo" aria-hidden="true">F</span>', '<span class="logo" aria-hidden="true">%s</span>' % theme.logo(), 1)
    return page


# This portal's name among its partners (shown in the header when set), e.g. "Site B (demo)".
SITE_NAME = os.environ.get("FAIR_SITE_NAME", "").strip()


@app.get("/api/info")
def info():
    return {"mode": store.mode, "model": llm.CHAT_MODEL or "(not set)", "site": SITE_NAME or None, "table_home": rocrate.TABLE_HOME,
            "peers": [p["name"] for p in federation.peers()],
            "search": store.db is not None or store.mode == "local", "in_cluster": store.db is not None,
            "embed_model": llm.EMBED_MODEL or "(not set)", "embed_dims": catalog.DIM,
            "embed_max_chars": catalog.MAX_TEXT}


@app.get("/api/models")
def models():
    """Which models the gateway lets this deployment's key use, and whether the configured ones are among them."""
    chat, emb = llm.CHAT_MODEL, llm.EMBED_MODEL
    try:
        names = llm.models()
    except llm.LLMError as e:
        return {"error": _model_reason(e), "chat_model": chat, "embed_model": emb}
    return {"chat_model": chat, "chat_ok": chat in names, "embed_model": emb, "embed_ok": emb in names,
            "embedding_like": [n for n in names if any(w in n.lower() for w in ("embed", "bge", "e5", "gte", "nomic"))],
            "available": names}


@app.get("/api/datasets")
def datasets():
    rows = [summary(r) for r in store.list_records()]
    return {"datasets": sorted(rows, key=lambda r: r["folder"]), "pending": store.pending()}


@app.get("/api/datasets/{folder:path}/record")
def dataset(folder: str):
    r = record_for(folder)
    stalled_tables(r)
    published = r.get("status") == "published"
    return {"record": r, "score": fairscore.score(r, published=published)}


def stalled_tables(r: dict) -> None:
    """Tables load after publication. If the function stopped part-way (its time ran out, its pod was
    replaced), the record still says "loading": past the loading budget, say what happened instead."""
    import datetime as dt
    if r.get("tables_status") != "loading" or not r.get("published"):
        return
    started = r.get("tables_started") or r["published"]
    age = (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(started)).total_seconds()
    if age > tables.LOAD_SECONDS + 300:
        r["tables_status"] = "stalled"
        r["table_notes"] = (r.get("table_notes") or []) + [
            "Loading the data as tables didn't finish (the function may have run out of time). The dataset is "
            "published as files."]


@app.post("/api/datasets/{folder:path}/tables/retry")
def retry_tables(folder: str):
    """Load a published dataset's tables again, after a load that didn't finish: the same review file is
    written again, and the function, seeing a review it has already applied, redoes only the tables."""
    r = record_for(folder)
    if r.get("status") != "published" or r.get("tables_status") != "loading":
        raise HTTPException(409, "only a published dataset whose tables didn't finish loading can be retried")
    body = store.get_bytes(f"{folder}/_fair/review.json")
    if body is None:
        raise HTTPException(404, "no review to replay")
    store.put(f"{folder}/_fair/review.json", body)
    return {"ok": True}


@app.post("/api/datasets/{folder:path}/preview")
async def preview(folder: str, request: Request):
    """The score a review would give, now and once published, without saving anything."""
    body = await request.json()
    r = curate.apply_review(copy.deepcopy(record_for(folder)), body.get("decisions", {}))
    for name, status in (body.get("variables") or {}).items():
        for v in r["variables"]:
            if v["name"] == name and v.get("description"):
                v["description"]["status"] = status
    a = curate.accepted(r)
    missing = [k for k in rocrate.REQUIRED_TO_PUBLISH if not a.get(k)]
    projected = dict(r, pid="ark:/99999/fk4…", landing_page="…", access_url="s3://…", indexed=True)
    return {"now": fairscore.score(r), "on_publish": fairscore.score(projected, published=True), "missing": missing}


@app.post("/api/datasets/{folder:path}/review")
async def review(folder: str, request: Request):
    body = await request.json()
    record_for(folder)
    # "tables": whether to also load the tabular files into VAST DataBase (the files stay where they are
    # either way). Default yes; a curator can publish as files only.
    store.put(f"{folder}/_fair/review.json", json.dumps({
        "reviewer": body.get("reviewer") or "reviewer", "decisions": body.get("decisions", {}),
        "variables": body.get("variables", {}), "tables": body.get("tables", True) is not False}, indent=2).encode())
    return {"ok": True}


def _upload_target(bucket: str, key: str) -> tuple[str, str]:
    key = key.strip("/")
    if bucket not in stores.BUCKETS or key.count("/") < 2 or ".." in key.split("/") or "/_fair/" in key:
        raise HTTPException(400, "bucket must be one of %s and key <domain>/<dataset>/<file>" % (stores.BUCKETS,))
    return bucket, key


@app.post("/api/upload/start")
async def upload_start(request: Request):
    """Begin a chunked upload of one file: returns an upload id for its parts."""
    b = await request.json()
    bucket, key = _upload_target(b.get("bucket", stores.RAW), b.get("key", ""))
    return {"upload_id": store.mp_start(bucket, key), "bucket": bucket, "key": key, "part_size": 5 * 1024 * 1024}


@app.put("/api/upload/part")
async def upload_part(request: Request, bucket: str, key: str, upload_id: str, n: int):
    bucket, key = _upload_target(bucket, key)
    body = await request.body()
    if not 1 <= n <= 10000 or len(body) > 64 * 1024 * 1024:
        raise HTTPException(400, "part number 1-10000, at most 64 MB each")
    return {"n": n, "etag": store.mp_part(bucket, key, upload_id, n, body)}


@app.post("/api/upload/complete")
async def upload_complete(request: Request):
    b = await request.json()
    bucket, key = _upload_target(b.get("bucket", stores.RAW), b.get("key", ""))
    store.mp_complete(bucket, key, b["upload_id"], b["parts"])
    return {"ok": True, "bucket": bucket, "key": key}


@app.post("/api/upload/ready")
async def upload_ready(request: Request):
    """Mark a folder complete (_READY), which starts curation."""
    b = await request.json()
    bucket, folder = b.get("bucket", stores.RAW), (b.get("folder") or "").strip("/")
    if bucket not in stores.BUCKETS or folder.count("/") < 1:
        raise HTTPException(400, "folder must be <domain>/<dataset>")
    store.put_marker(bucket, f"{folder}/_READY")
    return {"ok": True, "dataset": folder if bucket == stores.RAW else f"{bucket}/{folder}"}


@app.post("/api/upload")
async def upload(folder: str = Form(...), files: list[UploadFile] = File(...)):
    folder = folder.strip("/")
    if folder.count("/") != 1:
        raise HTTPException(400, "folder must be <domain>/<dataset>")
    for f in files:
        name = Path(f.filename).name
        store.put(f"{folder}/{name}", await f.read())
    store.put(f"{folder}/_READY", b"")
    return {"ok": True, "folder": folder, "files": len(files)}


DB_BUCKET, DB_SCHEMA = os.environ.get("DB_BUCKET", "fair-db"), os.environ.get("DB_SCHEMA", "fair")
DATA_SCHEMA = os.environ.get("DATA_SCHEMA", "data")
_local_vectors: dict[str, tuple[str, list[float]]] = {}  # local mode: folder -> (record version, unit vector)


def _published() -> list[dict]:
    return [r for r in store.list_records() if r.get("status") == "published"]


@app.get("/api/search")
def search(q: str, limit: int = 10):
    """Published datasets ranked by meaning. Cluster: VAST Vector Search in VAST DataBase. Local: the same embeddings,
    ranked in memory. If the embedding model can't be reached, falls back to matching the words."""
    q = q.strip()
    if not q:
        return {"results": [], "engine": None}
    try:
        return _search_by_meaning(q, limit)
    except llm.LLMError as e:
        return {"results": _keyword_search(q, limit), "degraded": True,
                "engine": "word match: the embedding model is unavailable (%s)" % _model_reason(e)}


def _model_reason(e: Exception) -> str:
    """The gateway's own reason, short: e.g. that the key has no access to the model."""
    text = str(e)
    try:
        body = json.loads(text[text.index("{"):])
        return (body.get("error") or {}).get("type") or text[:80]
    except ValueError:
        return text[:80]


def _keyword_search(q: str, limit: int) -> list[dict]:
    """Published datasets containing the query's words, the most words matched first."""
    words = [w for w in q.lower().split() if len(w) > 1]
    out = []
    for r in _remote_records():
        hit = sum(w in federation.document_text(r).lower() for w in words) / max(len(words), 1)
        if hit:
            out.append({k: r[k] for k in ("folder", "pid", "title", "description")} |
                       {"domain": None, "keywords": ", ".join(r["keywords"]), "similarity": round(hit, 3)})
    for r in _published():
        text = catalog.document_text(r).lower()
        hit = sum(w in text for w in words) / max(len(words), 1)
        if hit:
            s = summary(r)
            out.append({k: s[k] for k in ("folder", "pid", "title", "domain", "description")} |
                       {"keywords": ", ".join(s["keywords"]), "similarity": round(hit, 3)})
    return sorted(out, key=lambda x: -x["similarity"])[:limit]


def _search_by_meaning(q: str, limit: int) -> dict:
    if store.db is not None:
        results, engine = catalog.search(store.db, DB_BUCKET, DB_SCHEMA, q, limit)
        return {"results": results, "engine": engine}
    if store.mode == "cluster":
        raise HTTPException(503, "search needs VAST DB's data network: run the portal in its pod (deploy/portal.yaml), or in local mode")
    qv = catalog.embed_query(q)
    out = []
    for r in _published():
        folder = r["key_prefix"]
        cached = _local_vectors.get(folder)
        if not cached or cached[0] != r.get("published"):
            _local_vectors[folder] = cached = (r.get("published"), catalog.embed(catalog.document_text(r)))
        s = summary(r)
        sim = sum(a * b for a, b in zip(qv, cached[1]))
        out.append({k: s[k] for k in ("folder", "pid", "title", "domain", "description")} |
                   {"keywords": ", ".join(s["keywords"]), "similarity": round(sim, 3)})
    for r in _remote_records():   # partners' records, embedded with the same model, so they rank together
        cached = _local_vectors.get(r["folder"])
        if not cached or cached[0] != r["datestamp"]:
            _local_vectors[r["folder"]] = cached = (r["datestamp"], catalog.embed(federation.document_text(r)))
        out.append({k: r[k] for k in ("folder", "pid", "title", "description")} |
                   {"domain": None, "keywords": ", ".join(r["keywords"]),
                    "similarity": round(sum(a * b for a, b in zip(qv, cached[1])), 3)})
    return {"results": catalog.rank(q, out)[:limit], "engine": "in-memory (local mode)"}


# ---------------------------------------------------------------- Find data: published datasets, for the people who use them

def facts(r: dict) -> dict:
    """What someone choosing a dataset needs to see, from its published record."""
    a = curate.accepted(r)
    tabs = r.get("tables") or []
    return {"folder": r.get("key_prefix") or r["name"], "pid": r.get("pid"), "title": a.get("title") or r["name"],
            "description": (a.get("description") or "")[:600], "domain": a.get("domain"),
            "keywords": a.get("keywords") or [], "licence": a.get("license"),
            "creators": [c.get("name", "") if isinstance(c, dict) else str(c) for c in a.get("creators") or []],
            "formats": sorted({f["format"] for f in r["files"]}), "files": len(r["files"]),
            "bytes": sum(f["size"] for f in r["files"]), "tables": len(tabs), "rows": sum(t["rows"] for t in tabs),
            "score": r.get("score_after"), "published": r.get("published"), "landing_page": r.get("landing_page"),
            "temporal": a.get("temporalCoverage"), "spatial": a.get("spatialCoverage"), "site": None}


def remote_facts(r: dict) -> dict:
    """A partner's dataset, as harvested: the same shape as facts(), with the site that holds it."""
    return {"folder": r["folder"], "pid": r["pid"], "title": r["title"], "description": (r["description"] or "")[:600],
            "domain": None, "keywords": r["keywords"], "licence": r["licence"], "creators": r["creators"],
            "formats": r["formats"], "files": None, "bytes": 0, "tables": 0, "rows": 0, "score": None,
            "published": r.get("date"), "landing_page": r["landing_page"], "temporal": None, "spatial": None,
            "site": r["site"]}


def _facets(rows: list[dict]) -> dict:
    from collections import Counter
    return {"domain": Counter(x["domain"] for x in rows if x["domain"]).most_common(),
            "format": Counter(f for x in rows for f in x["formats"]).most_common(),
            "licence": Counter(x["licence"] for x in rows if x["licence"]).most_common(),
            "queryable": sum(1 for x in rows if x["tables"]),
            "site": ([("here", sum(1 for x in rows if not x["site"]))] + Counter(x["site"] for x in rows if x["site"]).most_common())
                    if any(x["site"] for x in rows) else []}


@app.get("/api/find")
def find(q: str = "", domain: str = "", format: str = "", licence: str = "", queryable: bool = False, site: str = ""):
    """Published datasets for people looking for data: by meaning (the stored embeddings) and by variable
    (names, CF standard names, units, descriptions), narrowed by domain, format, licence and whether the
    data can be queried in place. Without a query, all of them, by title."""
    rows = {x["folder"]: x for x in (facts(r) for r in _published())}
    rows |= {r["folder"]: remote_facts(r) for r in _remote_records()}   # partners' datasets, harvested
    facets = _facets(list(rows.values()))
    engine, q = None, q.strip()
    if q:
        try:
            found = _search_by_meaning(q, 50)
        except (llm.LLMError, HTTPException) as e:
            found = {"results": _keyword_search(q, 50),
                     "engine": "word match: the embedding model is unavailable (%s)" % (_model_reason(e) if isinstance(e, llm.LLMError) else e.detail)}
        engine = found["engine"]
        for hit in found["results"]:
            if hit["folder"] in rows:
                rows[hit["folder"]]["similarity"] = hit["similarity"]
        for v in variables(q)["variables"]:
            if v["folder"] in rows:
                rows[v["folder"]].setdefault("matched", []).append(
                    {k: v.get(k) for k in ("name", "units", "standard_name", "description")})
        for x in rows.values():
            if x.get("matched"):   # a variable that matches is strong evidence: rank it up
                x["similarity"] = round(max(x.get("similarity") or 0, 0.5) + 0.1, 3)
                x["matched"] = x["matched"][:5]
        hits = [x for x in rows.values() if x.get("similarity") is not None]
        hits.sort(key=lambda x: -x["similarity"])
    else:
        hits = sorted(rows.values(), key=lambda x: x["title"].lower())
    hits = [x for x in hits if (not domain or x["domain"] == domain) and (not format or format in x["formats"])
            and (not licence or x["licence"] == licence) and (not queryable or x["tables"])
            and (not site or (x["site"] or "here") == site)]
    return {"results": hits, "facets": facets, "engine": engine, "total": len(rows)}


SIMILAR_MIN = float(os.environ.get("FAIR_SIMILAR_MIN", "0.45"))


@app.get("/api/similar/{folder:path}")
def similar(folder: str, limit: int = 5):
    """Published datasets close in meaning to this one, and any that hold the very same files (by checksum)."""
    pubs = {r["key_prefix"]: r for r in _published()}
    me = pubs.get(folder)
    if not me:
        raise HTTPException(404, f"{folder} is not published")
    mine = {f["sha256"] for f in me["files"]}
    same = [facts(r) for k, r in pubs.items() if k != folder and mine & {f["sha256"] for f in r["files"]}]
    try:
        if store.db is not None:
            near, engine = catalog.similar(store.db, DB_BUCKET, DB_SCHEMA, folder, limit)
        elif store.mode == "local":
            def vec(r):
                cached = _local_vectors.get(r["key_prefix"])
                if not cached or cached[0] != r.get("published"):
                    _local_vectors[r["key_prefix"]] = cached = (r.get("published"), catalog.embed(catalog.document_text(r)))
                return cached[1]
            v = vec(me)
            near = sorted(({"folder": k, "similarity": round(sum(a * b for a, b in zip(v, vec(r))), 3)}
                           for k, r in pubs.items() if k != folder), key=lambda x: -x["similarity"])[:limit]
            engine = "in-memory (local mode)"
        else:
            near, engine = [], "needs VAST DB's data network"
    except llm.LLMError as e:
        near, engine = [], "the embedding model is unavailable (%s)" % _model_reason(e)
    # The nearest neighbours are always "nearest", even among unrelated datasets: only list ones close in
    # meaning (cosine >= SIMILAR_MIN), so a handful of published datasets doesn't make a satellite SST
    # product look similar to microscopy images.
    out = []
    for n in near:
        if n["folder"] in pubs and n["similarity"] >= SIMILAR_MIN:
            out.append(facts(pubs[n["folder"]]) | {"similarity": n["similarity"]})
    return {"similar": out, "same_files": same, "engine": engine, "threshold": SIMILAR_MIN}


@app.get("/api/variables")
def variables(q: str):
    """Variables of published datasets matching a name, CF standard name, unit or description."""
    if store.db is not None:
        return {"variables": catalog.find_variables(store.db, DB_BUCKET, DB_SCHEMA, q), "engine": "VAST DB fair.variables"}
    rows = [v for r in _published() for v in catalog.variable_rows(r, r.get("pid"), summary(r)["title"])]
    return {"variables": catalog.match_variables(rows, q)[:200], "engine": "records (local mode)"}


@app.post("/api/reindex")
def reindex():
    """Rebuild fair.search and fair.variables from every published record (cluster only)."""
    if store.db is None:
        raise HTTPException(503, "reindex runs on the cluster")
    done = {r["key_prefix"]: catalog.index_record(store.db, DB_BUCKET, DB_SCHEMA, r) for r in _published()}
    return {"indexed": done}


@app.get("/api/backlog")
def backlog():
    """Folders of data on the system with no FAIR record: the dark-data work queue."""
    elements, source = store.catalog_elements()
    rows = [r for r in sweep.backlog(elements) if r["bucket"] in stores.BUCKETS]
    for r in rows:   # the id the Datasets list and review screen use
        prefix = r["prefix"].strip("/")
        r["dataset"] = prefix if r["bucket"] == stores.RAW else f"{r['bucket']}/{prefix}"
    return {"source": source, "summary": sweep.summary(rows), "folders": rows}


@app.post("/api/backlog/curate")
async def backlog_curate(request: Request):
    """Curate a backlog folder in place: a _READY marker sets the pipeline on it, as an upload would."""
    body = await request.json()
    bucket, prefix = body.get("bucket"), (body.get("prefix") or "").strip("/")
    if bucket not in stores.BUCKETS or not prefix:
        raise HTTPException(400, "bucket must be one of %s, with a folder prefix" % (stores.BUCKETS,))
    store.put_marker(bucket, f"{prefix}/_READY")
    return {"ok": True, "dataset": prefix if bucket == stores.RAW else f"{bucket}/{prefix}"}


@app.get("/api/drafts")
def drafts():
    """Each dataset's draft as the pipeline wrote it, before any person's review: the latest fair.records
    version by fair-curate. For the accuracy scorecard (scripts/scorecard.py)."""
    if store.db is None:
        return {"drafts": {r.get("key_prefix") or r["name"]: r for r in store.list_records()},
                "source": "current records (local mode)"}
    with store.db.transaction() as tx:
        rows = tx.bucket(DB_BUCKET).schema(DB_SCHEMA).table("records").select(
            columns=["folder", "version", "actor", "record"]).read_all().to_pylist()
    latest = {}
    for r in rows:
        if r["actor"] == "fair-curate" and (r["folder"] not in latest or r["version"] > latest[r["folder"]]["version"]):
            latest[r["folder"]] = r
    return {"drafts": {k: json.loads(v["record"]) for k, v in latest.items()}, "source": "VAST DB fair.records"}


# ---------------------------------------------------------------- the data, queried in place

QUERY_ROWS, EXPORT_ROWS = 10_000, tables.EXPORT_ROWS   # JSON answers; CSV and Parquet downloads


def _table_sink():
    if store.db is not None:
        return tables.VastTables(store.db, DB_BUCKET, DATA_SCHEMA)
    if store.mode == "local":
        return tables.LocalTables(store.root / "fair-db" / "data")
    raise HTTPException(503, "tables are read over VAST DB's data network: run the portal in its pod, or in local mode")


def _table_entry(r: dict, name: str) -> dict:
    t = next((t for t in r.get("tables") or [] if t["table"] == name), None)
    if not t:
        raise HTTPException(404, f"{name} is not a table of {r.get('pid') or r.get('key_prefix')}")
    return t


def _run_query(entry, columns, where, limit, with_summary=False, aggregate=None, order=None):
    sink = _table_sink()
    try:
        return tables.query(sink, entry, columns, where, limit, with_summary=with_summary, aggregate=aggregate,
                            order=order)
    except ValueError as e:   # the question: an unknown column, a value that doesn't fit
        raise HTTPException(400, str(e))
    except Exception as e:  # noqa: BLE001 - the table store: say so, rather than fail the page
        raise HTTPException(503, f"{entry['location']} couldn't be read just now ({str(e)[:160]})")


def _json_rows(t) -> list[dict]:
    """Rows as JSON values: times as ISO 8601, float32 at its own precision (15.74, not 15.7399997), and NaN or
    infinity as null."""
    import pyarrow as pa
    for i, f in enumerate(t.schema):
        if pa.types.is_float32(f.type):
            t = t.set_column(i, f.name, pa.array([None if v is None else float(v) for v in t.column(i).cast(pa.string()).to_pylist()]))
    def val(v):
        if isinstance(v, float) and not math.isfinite(v):
            return None   # NaN or infinity, from a table loaded before NaN became null: JSON has neither
        return v.isoformat() if hasattr(v, "isoformat") else v
    return [{k: val(v) for k, v in row.items()} for row in t.to_pylist()]


@app.post("/api/query")
async def query(request: Request):
    """A published dataset's table, read in place: {folder, table, columns, where: ["lat>=40", ...], limit,
    summary}. Returns the rows, the equivalent SQL, and (with summary) statistics over every matching row."""
    b = await request.json()
    r = record_for(b.get("folder", ""))
    if r.get("status") != "published":
        raise HTTPException(409, "only published datasets are loaded as tables")
    entry = _table_entry(r, b.get("table", ""))
    limit = max(1, min(int(b.get("limit") or 100), QUERY_ROWS))
    import time
    t0 = time.time()
    out = _run_query(entry, b.get("columns"), [w for w in b.get("where") or [] if w.strip()], limit,
                     with_summary=bool(b.get("summary")), aggregate=b.get("aggregate"), order=b.get("order"))
    out["rows"], out["ms"] = _json_rows(out["rows"]), round((time.time() - t0) * 1000)
    out["api"] = "%s/tables/%s" % (r.get("landing_page", ""), entry["table"])
    return out


_suggestions: dict[tuple, list] = {}   # (dataset, publication time) -> checked example questions


@app.get("/api/suggest/{folder:path}")
def suggest(folder: str):
    """Example questions for a published dataset's tables, from the model, each with a query already checked
    against the tables (so running one needs no second model call). Cached per publication."""
    r = record_for(folder)
    if r.get("status") != "published" or not r.get("tables"):
        return {"suggestions": []}
    if r.get("questions"):   # made by the function when the tables were loaded
        return {"suggestions": r["questions"], "model": "record"}
    key = (folder, r.get("published"), tuple(t["table"] for t in r["tables"]))
    if key not in _suggestions:
        try:
            _suggestions[key] = nlquery.suggest(r, summary(r)["title"] or r["name"])
        except llm.LLMError as e:
            return {"suggestions": [], "error": "the model is unavailable (%s)" % _model_reason(e)}
    return {"suggestions": _suggestions[key], "model": llm.CHAT_MODEL}


@app.post("/api/ask")
async def ask(request: Request):
    """A question in plain language: the model proposes a query from the tables' curated metadata, it is
    checked like any other, and run. Returns the query as read (to show and edit) and its answer."""
    b = await request.json()
    r = record_for(b.get("folder", ""))
    if r.get("status") != "published" or not r.get("tables"):
        raise HTTPException(409, "only a published dataset with tables can be asked")
    question = (b.get("question") or "").strip()
    if not question:
        raise HTTPException(400, "ask a question")
    import time
    t0 = time.time()
    try:
        q = nlquery.interpret(question, r, summary(r)["title"] or r["name"])
    except llm.LLMError as e:
        raise HTTPException(503, "the model is unavailable (%s); use the controls below instead" % _model_reason(e))
    except ValueError as e:
        raise HTTPException(422, "the question couldn't be turned into a valid query (%s); try rephrasing it" % e)
    if q.get("unanswerable"):
        return {"unanswerable": q["unanswerable"], "model": llm.CHAT_MODEL}
    entry = _table_entry(r, q["table"])
    t1 = time.time()
    out = _run_query(entry, q["columns"], q["where"], q["limit"], with_summary=not q["aggregate"], aggregate=q["aggregate"],
                     order=q["order"])
    out["rows"], out["ms"] = _json_rows(out["rows"]), round((time.time() - t1) * 1000)
    out.update(interpreted=q, model=llm.CHAT_MODEL, model_ms=round((t1 - t0) * 1000))
    return out


def table_api(pid: str, name: str, request: Request):
    """/id/<pid>/tables/<name>?columns=a,b&where=a>=1&where=b=x&limit=100&format=json|csv|parquet"""
    import io
    r = next((r for r in store.list_records() if r.get("pid") == pid and r.get("status") == "published"), None)
    if not r:
        raise HTTPException(404, f"unknown identifier {pid}")
    entry = _table_entry(r, name)
    p = request.query_params
    fmt = p.get("format", "json")
    if fmt not in ("json", "csv", "parquet"):
        raise HTTPException(400, "format is json, csv or parquet")
    cap = QUERY_ROWS if fmt == "json" else EXPORT_ROWS
    limit = min(int(p.get("limit") or (100 if fmt == "json" else cap)), cap)
    columns = [c for c in (p.get("columns") or "").split(",") if c] or None
    agg = None
    if p.get("agg"):   # agg=avg:sst (or agg=count) and optionally by=year
        fn, _, col = p["agg"].partition(":")
        agg = {"fn": fn, "column": col or None, "by": p.get("by") or None}
    order = None
    if p.get("order"):   # order=sst or order=-sst (descending)
        order = {"column": p["order"].lstrip("-"), "desc": p["order"].startswith("-")}
    out = _run_query(entry, columns, p.getlist("where"), limit, aggregate=agg, order=order)
    headers = {"Link": f'<{r.get("landing_page", "")}>; rel="cite-as"', "X-Source-SHA256": entry["source_sha256"],
               "X-Query-SQL": " ".join(out["sql"].split())}
    if fmt == "json":
        return Response(json.dumps({"identifier": pid, "table": entry["location"], "source": entry["source"],
                                    "source_sha256": entry["source_sha256"], "sql": out["sql"],
                                    "columns": [c for c in entry["columns"] if c["name"] in out["columns"]],
                                    "rows": _json_rows(out["rows"])}, default=str),
                        media_type="application/json", headers=headers)
    buf = io.BytesIO()
    if fmt == "csv":
        import pyarrow as pa
        import pyarrow.csv as pcsv
        rows = out["rows"]
        for i, f in enumerate(rows.schema):   # float32 at its own precision (12.39, not 12.389999)
            if pa.types.is_float32(f.type):
                rows = rows.set_column(i, f.name, rows.column(i).cast(pa.string()))
        pcsv.write_csv(rows, buf)
        media = "text/csv"
    else:
        import pyarrow.parquet as pq
        pq.write_table(out["rows"], buf)
        media = "application/vnd.apache.parquet"
    headers["Content-Disposition"] = f'attachment; filename="{name}.{fmt}"'
    return Response(buf.getvalue(), media_type=media, headers=headers)


@app.get("/api/log")
def log():
    return {"log": list(store.log)}


def download(pid: str, fname: str):
    """A published file over HTTP, streamed from S3, with the SHA-256 it was published with. If the live
    object has changed since publication, refuse (409) and point at the snapshot that holds the original."""
    import base64
    import datetime as dt
    from fastapi.responses import JSONResponse, StreamingResponse
    for r in store.list_records():
        if r.get("pid") != pid:
            continue
        f = next((x for x in r["files"] if x["name"] == fname), None)
        if not f:
            raise HTTPException(404, f"{fname} is not part of {pid}")
        bucket = r.get("bucket") or stores.RAW
        prefix = r.get("prefix") or r["key_prefix"]
        obj = store.s3.get_object(Bucket=bucket, Key=f"{prefix}/{fname}")
        changed = obj.get("LastModified") and r.get("published") and \
            obj["LastModified"] > dt.datetime.fromisoformat(r["published"])
        if changed or (obj.get("ContentLength") not in (None, f["size"])):
            return JSONResponse(status_code=409, content={
                "detail": f"{fname} has changed since {pid} was published", "published_sha256": f["sha256"],
                "snapshot": r.get("snapshot")})
        digest = base64.b64encode(bytes.fromhex(f["sha256"])).decode()
        body = obj["Body"]
        chunks = body.iter_chunks(1 << 20) if hasattr(body, "iter_chunks") else iter(lambda: body.read(1 << 20), b"")
        return StreamingResponse(chunks, media_type=f.get("media_type") or "application/octet-stream", headers={
            "Content-Length": str(f["size"]), "Content-Disposition": f'attachment; filename="{fname}"',
            "Digest": f"sha-256={digest}", "Link": f'<{r.get("landing_page", "")}>; rel="cite-as"'})
    raise HTTPException(404, f"unknown identifier {pid}")


# ---------------------------------------------------------------- catalog of catalogs: partners' datasets, harvested

HARVEST_MINUTES = float(os.environ.get("FAIR_HARVEST_MINUTES", "10"))
_peer_status: dict[str, dict] = {}
_local_remote: dict[str, dict] = {}            # local mode: folder -> harvested record
_remote_cache = {"t": 0.0, "rows": []}         # cluster: fair.remote, re-read at most every 30 s


def _remote_records() -> list[dict]:
    """Every partner dataset this portal has harvested."""
    if store.db is not None:
        if time.time() - _remote_cache["t"] > 30:
            try:
                _remote_cache["rows"] = federation.load(store.db, DB_BUCKET, DB_SCHEMA)
            except Exception:  # noqa: BLE001 - keep what was read last
                pass
            _remote_cache["t"] = time.time()
        return _remote_cache["rows"]
    return list(_local_remote.values())


def harvest_peers() -> list[dict]:
    """Harvest every partner in FAIR_PEERS now: their records into VAST DB, embedded into fair.search."""
    out = []
    for p in federation.peers():
        st = {"name": p["name"], "oai": p["oai"], "harvested_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
        try:
            recs = federation.harvest(p)
            if store.db is not None:
                st |= federation.sync(store.db, DB_BUCKET, DB_SCHEMA, p["name"], recs, catalog.embed)
                _remote_cache["t"] = 0.0
            else:
                for f in [f for f, r in _local_remote.items() if r["site"] == p["name"]]:
                    del _local_remote[f]
                _local_remote.update({r["folder"]: r for r in recs})
                st["records"] = len(recs)
            st["error"] = None
        except Exception as e:  # noqa: BLE001 - a partner that is down keeps its last harvest
            st["error"] = str(e)[:300]
        _peer_status[p["name"]] = {**_peer_status.get(p["name"], {}), **st}
        out.append(_peer_status[p["name"]])
    return out


@app.get("/api/peers")
def peers_status():
    return {"site": SITE_NAME or None, "every_minutes": HARVEST_MINUTES,
            "peers": [_peer_status.get(p["name"], {"name": p["name"], "oai": p["oai"]}) for p in federation.peers()],
            "harvested": len(_remote_records())}


@app.post("/api/peers/harvest")
def peers_harvest():
    return {"peers": harvest_peers()}


@app.get("/api/remote/{folder:path}")
def remote(folder: str):
    r = next((x for x in _remote_records() if x["folder"] == folder), None)
    if not r:
        raise HTTPException(404, f"no harvested record {folder}")
    return {"record": r, "facts": remote_facts(r), "peer": _peer_status.get(r["site"])}


def _every(minutes: float, fn):
    def loop():
        time.sleep(5)
        while True:
            try:
                fn()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(max(minutes, 0.5) * 60)
    threading.Thread(target=loop, daemon=True).start()


def _start_background():
    # Site B (demo) and other local-mode sites publish their seed datasets through their own pipeline.
    if os.environ.get("FAIR_SITE_SEED") and store.mode == "local":
        from portal import seed
        threading.Thread(target=seed.seed, args=(store, Path(os.environ["FAIR_SITE_SEED"])), daemon=True).start()
    if federation.peers():
        _every(HARVEST_MINUTES, harvest_peers)


def _harvest_entries() -> list[dict]:
    """Every published dataset as its published RO-Crate says it, for the harvest endpoints."""
    out = []
    for r in _published():
        body = store.get_bytes(f"{r.get('key_prefix') or r['name']}/ro-crate-metadata.json")
        if body:
            e = harvest.entry(r, json.loads(body))
            if e["pid"] and e["datestamp"]:
                out.append(e)
    return out


def _base(request: Request) -> str:
    return str(request.base_url).rstrip("/")


@app.get("/harvest/dcat")
def harvest_dcat(request: Request):
    """The catalogue as DCAT-AP 3 JSON-LD, for data portals that harvest DCAT."""
    doc = harvest.dcat_catalog(_harvest_entries(), f"{_base(request)}/harvest/dcat", f"{_base(request)}/",
                               rocrate.PUBLISHER)
    return Response(json.dumps(doc, indent=1), media_type="application/ld+json")


@app.api_route("/oai", methods=["GET", "POST"])
async def harvest_oai(request: Request):
    """OAI-PMH 2.0 with Dublin Core (oai_dc), for repositories and aggregators that harvest OAI-PMH."""
    params: dict[str, list[str]] = {}
    pairs = list(request.query_params.multi_items())
    if request.method == "POST":
        pairs += list((await request.form()).multi_items())
    for k, v in pairs:
        params.setdefault(k, []).append(str(v))
    xml = harvest.oai_response(params, _harvest_entries(), f"{_base(request)}/oai",
                               f"{rocrate.PUBLISHER}: published datasets",
                               os.environ.get("FAIR_ADMIN_EMAIL", "fair-admin@example.org"))
    return Response(xml, media_type="text/xml; charset=utf-8")


@app.get("/id/{pid:path}")
def landing(pid: str, request: Request):
    """Resolve an identifier to its landing page (or the RO-Crate, for machine clients), and
    /id/<pid>/ro-crate-metadata.json | croissant.json to those documents."""
    name = None
    if "/files/" in pid:
        pid, _, fname = pid.partition("/files/")
        return download(pid, fname)
    if "/tables/" in pid:
        pid, _, tname = pid.partition("/tables/")
        return table_api(pid, tname, request)
    head, _, tail = pid.rpartition("/")
    if tail in curate.OUTPUTS:
        pid, name = head, tail
    for r in store.list_records():
        if r.get("pid") != pid:
            continue
        folder = r.get("key_prefix") or r["name"]
        if name is None:
            wants_ld = "ld+json" in request.headers.get("accept", "")
            name = "ro-crate-metadata.json" if wants_ld else "ro-crate-preview.html"
        body = None
        if name == "ro-crate-preview.html":
            # Drawn from the published RO-Crate as it is served, so pages published earlier get the current design.
            crate = store.get_bytes(f"{folder}/ro-crate-metadata.json")
            if crate:
                try:
                    body = rocrate.render_preview(r, json.loads(crate)).encode()
                except Exception:  # noqa: BLE001 - the page as published, then
                    body = None
        if body is None:
            body = store.get_bytes(f"{folder}/{name}")
        if body is None:
            break
        media = "text/html; charset=utf-8" if name.endswith(".html") else "application/ld+json"
        headers = {"Link": f'</id/{pid}/ro-crate-metadata.json>; rel="describedby"; type="application/ld+json", '
                           f'<{r.get("landing_page", "")}>; rel="cite-as"'}
        return Response(body, media_type=media, headers=headers)
    raise HTTPException(404, f"unknown identifier {pid}")
