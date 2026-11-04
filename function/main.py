"""fair-curate: the FAIR curation pipeline as one DataEngine function behind an S3 Element trigger.

A dataset is a folder (prefix) in the landing bucket, e.g. s3://fair-raw/climate/oisst-2024-09/. What
the function does depends on the object that arrived:

  any data file           profile it (format, header, checksum) -> row in fair.files
  <folder>/_READY         the folder is complete: curate it from the stored profiles and the folder's
                          companion documents -> record version (status "proposed") in fair.records
  <folder>/_fair/review.json   a person's review decisions: apply them, publish ro-crate-metadata.json,
                          croissant.json and ro-crate-preview.html into the folder, a catalog row
                          (status "published"), and the landing text into the InsightEngine docs bucket

Its own outputs land in the same bucket and come back as events; they are skipped. Every table is
append-only, so fair.records is also the audit trail of who changed what.

Configuration:
- Secret vast-s3-creds (vast-s3-access-key, vast-s3-secret-key): S3 and VAST DB
- Secret fair-llm (api-key): the LiteLLM gateway key
- Env vast-s3-endpoint-url, LITELLM_BASE_URL, FAIR_CHAT_MODEL, HTTPS_PROXY (optional),
  DB_BUCKET, DB_SCHEMA, DOCS_BUCKET, LANDING_BASE_URL
"""

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import vastdb
from ibis import _

from fairdemo import catalog, companions, curate, extract, fairscore, rocrate
from vast.dataengine.sdk import Context, VastEvent

DB_BUCKET = os.getenv("DB_BUCKET", "fair-db")
DB_SCHEMA = os.getenv("DB_SCHEMA", "fair")
DOCS_BUCKET = os.getenv("DOCS_BUCKET", "fair-docs")
LANDING_BASE_URL = os.getenv("LANDING_BASE_URL", "http://fair.localhost/id")
VMS_URL, VMS_TENANT = os.getenv("VMS_URL", ""), os.getenv("VMS_TENANT", "fair")
RAW_BUCKET = os.getenv("RAW_BUCKET", "fair-raw")
READY, REVIEW = "_READY", "_fair/review.json"


def dataset_id(bucket, prefix):
    """The dataset's key in the tables and the portal: its prefix in the landing bucket (as it always was),
    or <bucket>/<prefix> in any other bucket, such as the estate the backlog sweep curates in place."""
    return prefix if bucket == RAW_BUCKET else f"{bucket}/{prefix}"
COMPANION_MAX = 5 * 1024 * 1024

TABLES = {
    "files": pa.schema([("folder", pa.string()), ("key", pa.string()), ("name", pa.string()),
                        ("size", pa.int64()), ("sha256", pa.string()), ("format", pa.string()),
                        ("media_type", pa.string()), ("profile", pa.string()), ("profiled_at", pa.string())]),
    "records": pa.schema([("folder", pa.string()), ("version", pa.int64()), ("status", pa.string()),
                          ("title", pa.string()), ("domain", pa.string()), ("pid", pa.string()),
                          ("score_before", pa.int64()), ("score_after", pa.int64()),
                          ("record", pa.string()), ("actor", pa.string()), ("updated_at", pa.string())]),
}

_clients = {}


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def init(ctx: Context) -> None:
    creds = ctx.secrets.get("vast-s3-creds") or {}
    access, secret = creds.get("vast-s3-access-key", "").strip(), creds.get("vast-s3-secret-key", "").strip()
    endpoint = os.getenv("vast-s3-endpoint-url")
    if not (access and secret and endpoint):
        raise RuntimeError("needs secret vast-s3-creds and env vast-s3-endpoint-url")
    llm = ctx.secrets.get("fair-llm") or {}
    os.environ["LITELLM_API_KEY"] = llm.get("api-key", "").strip()
    # S3 and VAST DB stay direct even when HTTPS_PROXY routes the model calls through a proxy.
    os.environ["NO_PROXY"] = ",".join(filter(None, [os.getenv("NO_PROXY"), endpoint.split("//")[-1].split(":")[0]]))
    _clients["s3"] = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=access,
                                  aws_secret_access_key=secret, region_name="us-east-1")
    _clients["db"] = vastdb.connect(endpoint=endpoint, access=access, secret=secret)
    with _clients["db"].transaction() as tx:
        bucket = tx.bucket(DB_BUCKET)
        schema = bucket.schema(DB_SCHEMA, fail_if_missing=False) or bucket.create_schema(DB_SCHEMA)
        for name, cols in TABLES.items():
            if not schema.table(name, fail_if_missing=False):
                schema.create_table(name, cols)
    ctx.logger.info(f"fair-curate ready: S3 and VAST DB at {endpoint}, tables {DB_BUCKET}/{DB_SCHEMA}")


# ---------------------------------------------------------------- tables

def insert(table, row: dict):
    cols = TABLES[table]
    batch = pa.RecordBatch.from_pylist([row], schema=cols)
    with _clients["db"].transaction() as tx:
        tx.bucket(DB_BUCKET).schema(DB_SCHEMA).table(table).insert(batch)


def select(table, folder=None, sha256=None) -> list[dict]:
    with _clients["db"].transaction() as tx:
        t = tx.bucket(DB_BUCKET).schema(DB_SCHEMA).table(table)
        pred = (_.folder == folder) if folder is not None else (_.sha256 == sha256)
        return t.select(predicate=pred).read_all().to_pylist()


def latest_record(folder) -> dict | None:
    rows = select("records", folder)
    return max(rows, key=lambda r: r["version"]) if rows else None


def put_record(folder, record, status, actor, score_after=None):
    prev = latest_record(folder)
    a = curate.accepted(record)
    insert("records", {
        "folder": folder, "version": (prev["version"] + 1) if prev else 1, "status": status,
        "title": a.get("title"), "domain": a.get("domain") or (record["fields"].get("domain") or {}).get("value"),
        "pid": record.get("pid"), "score_before": record.get("score_before"), "score_after": score_after,
        "record": json.dumps(record, default=str), "actor": actor, "updated_at": now()})


# ---------------------------------------------------------------- steps

def download(bucket, key, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    _clients["s3"].download_file(bucket, key, str(dest))
    return dest


def on_data_file(ctx, bucket, key, folder):
    with tempfile.TemporaryDirectory() as tmp:
        path = download(bucket, key, Path(tmp) / Path(key).name)
        if not extract.is_data_file(path):
            ctx.logger.info(f"{key}: not a data file (a companion document); profiled at curation")
            return {"skipped": key}
        p = extract.profile(path)
    p["path"] = f"s3://{bucket}/{key}"
    insert("files", {"folder": dataset_id(bucket, folder), "key": key, "name": p["name"], "size": p["size"], "sha256": p["sha256"],
                     "format": p["format"], "media_type": p["media_type"], "profile": json.dumps(p, default=str),
                     "profiled_at": now()})
    ctx.logger.info(f"profiled {key}: {p['format']}, {len(p['variables'])} variables, notes {p['notes']}")
    return {"profiled": key, "format": p["format"], "profile": p}


def _exists(key, dataset) -> bool:
    """Does the object a fair.files row points at still exist? (dataset ids outside the landing bucket
    start with their bucket's name.)"""
    bucket = RAW_BUCKET if dataset == key.rsplit("/", 1)[0] else dataset.split("/", 1)[0]
    try:
        _clients["s3"].head_object(Bucket=bucket, Key=key)
        return True
    except Exception:  # noqa: BLE001 - missing, or not ours to see
        return False


def on_ready(ctx, bucket, folder):
    # Events arrive on several partitions at once, so _READY can overtake a big file's own event:
    # profile here whatever has landed but not been profiled yet (its own event then just repeats it).
    # A row written a moment ago by another event may not be visible to this read yet, so the profiles
    # used are the ones read once here plus the ones taken here, never a second read.
    did = dataset_id(bucket, folder)
    latest = {}
    for r in sorted(select("files", did), key=lambda r: r["profiled_at"]):
        latest[r["key"]] = json.loads(r["profile"])
    for page in _clients["s3"].get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=folder + "/"):
        for o in page.get("Contents", []):
            rel = o["Key"][len(folder) + 1:]
            name = rel.rsplit("/", 1)[-1]
            if o["Key"] in latest or rel == READY or rel.startswith("_fair/") or name in curate.OUTPUTS \
                    or any(rx.match(name) for rx in (companions.README, companions.CITATION, companions.LICENSE,
                                                     companions.SCRIPT)):
                continue
            got = on_data_file(ctx, bucket, o["Key"], folder)
            if "profile" in got:
                latest[o["Key"]] = got["profile"]
    profiles = list(latest.values())
    data_keys = set(latest)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / folder.rsplit("/", 1)[-1]
        root.mkdir(parents=True)
        pages = _clients["s3"].get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=folder + "/")
        for page in pages:
            for o in page.get("Contents", []):
                rel = o["Key"][len(folder) + 1:]
                if o["Key"] in data_keys or rel == READY or rel.startswith("_fair/") or rel in curate.OUTPUTS \
                        or o["Size"] > COMPANION_MAX or rel.count("/") > 1:
                    continue
                download(bucket, o["Key"], root / rel)
        comp = companions.find(root)
        record = curate.curate_profiles(folder.rsplit("/", 1)[-1], f"s3://{bucket}/{folder}", profiles, comp)
        record["score_before"] = fairscore.score(curate.curate_profiles(record["name"], record["folder"], profiles, comp,
                                                                        use_llm=False))["total"]
    record["key_prefix"], record["bucket"], record["prefix"] = did, bucket, folder
    record["status"] = "proposed"
    # The same bytes elsewhere in the estate: worth a curator's look before a second copy is published.
    # fair.files keeps rows for data since deleted, so only count copies whose file is still there.
    dupes = sorted({r["folder"] for p in profiles for r in select("files", sha256=p["sha256"])
                    if r["folder"] != did and _exists(r["key"], r["folder"])})
    if dupes:
        record["duplicates"] = dupes
    _clients["s3"].put_object(Bucket=bucket, Key=f"{folder}/_fair/record.json",
                              Body=json.dumps(record, indent=2, default=str).encode())
    put_record(did, record, "proposed", "fair-curate")
    todo = [k for k, f in record["fields"].items() if f["status"] != "accepted"]
    ctx.logger.info(f"curated {folder}: {len(profiles)} files, FAIR before {record['score_before']}, "
                    f"for review: {todo}, duplicates: {dupes or 'none'}, model error: {record['llm_error']}")
    return {"curated": folder, "review": todo}


def on_review(ctx, bucket, folder, key):
    raw = _clients["s3"].get_object(Bucket=bucket, Key=key)["Body"].read()
    review = json.loads(raw)
    did = dataset_id(bucket, folder)
    prev = latest_record(did)
    if not prev:
        raise RuntimeError(f"review for {folder} but no record yet")
    # Events are delivered at least once: a review already applied is not applied (or published) again.
    review_id = hashlib.sha256(raw).hexdigest()[:16]
    record = json.loads(prev["record"])
    if record.get("review_id") == review_id:
        ctx.logger.info(f"{folder}: review {review_id} already applied (redelivered event)")
        return {"skipped": f"{key}: already applied"}
    record = curate.apply_review(record, review.get("decisions", {}))
    record["review_id"], record["reviewer"] = review_id, review.get("reviewer")
    for name, status in (review.get("variables") or {}).items():
        for v in record["variables"]:
            if v["name"] == name and v.get("description"):
                v["description"]["status"] = status
    actor = review.get("reviewer", "reviewer")
    try:
        rocrate.check_publishable(record)
    except rocrate.NotReady:
        pass  # handled just below, by build
    else:
        # Preserve the bytes first, so the crate can name the snapshot that holds them.
        pid = record.get("pid") or rocrate.mint_pid(record)
        try:
            # Named for the identifier and the moment: a republish keeps every earlier snapshot.
            record["snapshot"] = snapshot_folder(ctx, bucket, folder, "fair-%s-%s" % (
                pid.rsplit("/", 1)[-1], datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        except Exception as e:  # noqa: BLE001 - publish without it, and say so
            record["snapshot"] = None
            ctx.logger.info(f"{did}: no snapshot taken: {e}")
    try:
        docs = rocrate.build(record, LANDING_BASE_URL, f"s3://{bucket}/{folder}")
    except rocrate.NotReady as e:
        put_record(did, record, "needs-review", actor)
        record["status"] = "needs-review"
        record["missing"] = e.missing
        _clients["s3"].put_object(Bucket=bucket, Key=f"{folder}/_fair/record.json",
                                  Body=json.dumps(record, indent=2, default=str).encode())
        ctx.logger.info(f"{folder}: reviewed by {actor} but not publishable yet: {e}")
        return {"needs_review": folder, "missing": e.missing}
    for name in curate.OUTPUTS:
        body = docs[name] if name.endswith(".html") else json.dumps(docs[name], indent=2, default=str)
        _clients["s3"].put_object(Bucket=bucket, Key=f"{folder}/{name}", Body=body.encode(),
                                  ContentType="text/html" if name.endswith(".html") else "application/ld+json")
    record.update(pid=docs["pid"], landing_page=docs["landing_page"], access_url=docs["landing_page"] + "/files/",
                  s3_url=f"s3://{bucket}/{folder}/", published=now())
    # The landing text goes to the InsightEngine bucket, whose own trigger chunks and embeds it for RAG.
    _clients["s3"].put_object(Bucket=DOCS_BUCKET, Key=re.sub(r"[^A-Za-z0-9._-]+", "-", docs["pid"]) + ".md",
                              Body=landing_markdown(record, docs).encode(), ContentType="text/markdown")
    record["indexed"] = True
    score = fairscore.score(record, published=True)
    record["score_after"] = score["total"]
    put_record(did, record, "published", actor, score_after=score["total"])
    record["status"] = "published"
    _clients["s3"].put_object(Bucket=bucket, Key=f"{folder}/_fair/record.json",
                              Body=json.dumps(record, indent=2, default=str).encode())
    # Searchable by meaning and by variable. A failure here leaves the dataset published but unsearched;
    # the portal's reindex puts it right.
    try:
        got = catalog.index_record(_clients["db"], DB_BUCKET, DB_SCHEMA, record)
        ctx.logger.info(f"indexed {folder}: {got}")
    except Exception as e:  # noqa: BLE001
        ctx.logger.info(f"{folder}: published, not indexed for search: {e}")
    ctx.logger.info(f"published {folder} as {docs['pid']}: FAIR {record.get('score_before')} -> {score['total']}")
    return {"published": folder, "pid": docs["pid"], "score": score["total"]}


def snapshot_folder(ctx, bucket, folder, name) -> dict | None:
    """A VAST snapshot of the dataset folder at publication: the bytes the identifier refers to, kept
    however the live folder changes. Through VMS as the tenant admin; skipped (None) when not configured."""
    creds = ctx.secrets.get("fair-vms") or {}
    if not (VMS_URL and creds.get("vms-user")):
        return None
    import ssl
    import urllib.parse
    import urllib.request
    tls = ssl._create_unverified_context()  # the cluster's VMS certificate is self-signed
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=tls))
    form = urllib.parse.urlencode({"username": creds["vms-user"], "password": creds["vms-password"]}).encode()
    tok = json.load(opener.open(f"{VMS_URL}/api/token/{VMS_TENANT}", form, timeout=30))["access"]
    req = urllib.request.Request(f"{VMS_URL}/api/snapshots/", json.dumps({"name": name, "path": f"/{bucket}/{folder}/"}).encode(),
                                 {"Authorization": "Bearer " + tok, "Content-Type": "application/json"})
    snap = json.load(opener.open(req, timeout=60))
    return {k: snap.get(k) for k in ("id", "guid", "name", "path", "created", "access_path", "locked", "state")}


def landing_markdown(record, docs) -> str:
    a = curate.accepted(record)
    lines = [f"# {a.get('title') or record['name']}", "", f"Identifier: {docs['pid']}",
             f"Location: {record['access_url']}", f"Licence: {a.get('license', 'not stated')}",
             "Creators: " + "; ".join(c.get("name", str(c)) if isinstance(c, dict) else str(c)
                                       for c in a.get("creators") or []),
             f"Domain: {a.get('domain', '')}", f"Keywords: {', '.join(a.get('keywords') or [])}", "",
             a.get("description", ""), "", "## Variables"]
    for v in record["variables"][:200]:
        desc = v.get("long_name") or (v.get("description") or {}).get("value") or ""
        lines.append(f"- {v['name']}" + (f" ({v['units']})" if v.get("units") else "") + (f": {desc}" if desc else ""))
    lines += ["", "## Files"] + [f"- {f['name']} ({f['format']}, {f['size']} bytes)" for f in record["files"]]
    return "\n".join(lines) + "\n"


def handler(ctx: Context, event: VastEvent) -> dict:
    if not _clients:
        init(ctx)
    element = event.as_element_event()
    bucket, key = element.bucket, element.object_key
    name = key.rsplit("/", 1)[-1]
    if "/" not in key:
        return {"skipped": f"{key}: datasets are folders"}
    if key.endswith("/" + REVIEW):
        return on_review(ctx, bucket, key[: -len(REVIEW) - 1], key)
    if "/_fair/" in key or name in curate.OUTPUTS or name.startswith("."):
        return {"skipped": f"{key}: pipeline output"}
    folder = key.rsplit("/", 1)[0]
    if name == READY:
        return on_ready(ctx, bucket, folder)
    out = on_data_file(ctx, bucket, key, folder)
    out.pop("profile", None)  # kept for on_ready; too big for the function's result log
    return out
