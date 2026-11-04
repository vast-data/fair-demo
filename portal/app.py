"""FAIR portal: the curator's inbox, the review screen, and the public landing pages.

    .venv/bin/uvicorn portal.app:app --port 8080        # FAIR_STORE=local|cluster (see store.py)

The portal never curates or publishes itself. It shows what the pipeline proposed, records a person's
decisions as _fair/review.json, and the pipeline does the rest.
"""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, Response

from fairdemo import catalog, curate, fairscore, llm, rocrate, sweep
from portal import store as stores

ROOT = Path(__file__).resolve().parents[1]
for line in (ROOT / ".env").read_text().splitlines() if (ROOT / ".env").exists() else []:
    k, sep, v = line.partition("=")
    if sep and not k.startswith("#"):
        os.environ.setdefault(k.strip(), v.strip())

app = FastAPI(title="FAIR portal")
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
    return (ROOT / "portal/static/index.html").read_text()


@app.get("/api/info")
def info():
    return {"mode": store.mode, "model": llm.CHAT_MODEL or "(not set)",
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
    published = r.get("status") == "published"
    return {"record": r, "score": fairscore.score(r, published=published)}


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
    store.put(f"{folder}/_fair/review.json", json.dumps({
        "reviewer": body.get("reviewer") or "reviewer", "decisions": body.get("decisions", {}),
        "variables": body.get("variables", {})}, indent=2).encode())
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
_local_vectors: dict[str, tuple[str, list[float]]] = {}  # local mode: folder -> (record version, unit vector)


def _published() -> list[dict]:
    return [r for r in store.list_records() if r.get("status") == "published"]


@app.get("/api/search")
def search(q: str, limit: int = 10):
    """Published datasets ranked by meaning. Cluster: VAST DB vector index. Local: the same embeddings,
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
    return {"results": catalog.rank(q, out)[:limit], "engine": "in-memory (local mode)"}


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


@app.get("/id/{pid:path}")
def landing(pid: str, request: Request):
    """Resolve an identifier to its landing page (or the RO-Crate, for machine clients), and
    /id/<pid>/ro-crate-metadata.json | croissant.json to those documents."""
    name = None
    if "/files/" in pid:
        pid, _, fname = pid.partition("/files/")
        return download(pid, fname)
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
        body = store.get_bytes(f"{folder}/{name}")
        if body is None:
            break
        media = "text/html; charset=utf-8" if name.endswith(".html") else "application/ld+json"
        headers = {"Link": f'</id/{pid}/ro-crate-metadata.json>; rel="describedby"; type="application/ld+json", '
                           f'<{r.get("landing_page", "")}>; rel="cite-as"'}
        return Response(body, media_type=media, headers=headers)
    raise HTTPException(404, f"unknown identifier {pid}")
