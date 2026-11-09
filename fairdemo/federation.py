"""A catalog of catalogs: harvest partner repositories over OAI-PMH, so one search ranks datasets across sites.

Partners are listed in FAIR_PEERS, as JSON: [{"name": "Site B (demo)", "oai": "http://.../oai"}]. Any repository
that speaks OAI-PMH with Dublin Core (oai_dc) can be one, which is what university repositories run.

The portal harvests each partner's records and keeps them in VAST DB (fair.remote). It embeds them, with the same
model as its own datasets, into the same fair.search table under the folder "@<site>/<identifier>", so search by
meaning ranks datasets from every site together. Access stays with the owning site: a harvested record links to
its landing page there, where its data lives. A full harvest replaces the site's records, so a dataset the
partner no longer lists drops out here too.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

import pyarrow as pa

OAI = "{http://www.openarchives.org/OAI/2.0/}"
DC = "{http://purl.org/dc/elements/1.1/}"
REMOTE = pa.schema([(k, pa.string()) for k in (
    "folder", "site", "pid", "title", "description", "creators", "keywords", "licence", "formats", "publisher",
    "date", "coverage", "landing_page", "datestamp", "harvested_at")])
LISTS = ("creators", "keywords", "formats", "coverage")   # stored as JSON text
# Media types as this portal names formats, so a harvested CSV and a local one share the Format filter.
FORMATS = {"text/csv": "CSV", "application/x-netcdf": "NetCDF", "application/netcdf": "NetCDF", "application/fits": "FITS",
           "image/fits": "FITS", "application/vnd.apache.parquet": "Parquet", "image/tiff": "TIFF",
           "application/x-hdf5": "HDF5", "text/x-vcf": "VCF", "application/x-root": "ROOT"}
LICENCES = {"https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/": "OGL-UK-3.0"}


def peers() -> list[dict]:
    """The partner repositories to harvest, from FAIR_PEERS."""
    raw = os.environ.get("FAIR_PEERS", "").strip()
    if not raw:
        return []
    out = []
    for p in json.loads(raw):
        if p.get("name") and p.get("oai"):
            out.append({"name": p["name"], "oai": p["oai"].rstrip("/"), "slug": slug(p["name"])})
    return out


def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "site"


def folder_for(site: str, pid: str) -> str:
    return "@%s/%s" % (slug(site), pid)


def is_remote(folder: str) -> bool:
    return folder.startswith("@")


def fetch(base: str, params: dict, timeout: float = 30) -> str:
    url = base + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "fair-portal-harvester"}), timeout=timeout) as r:
        return r.read().decode("utf-8")


def _licence(rights: str | None) -> str | None:
    if not rights:
        return None
    m = re.match(r"https?://spdx\.org/licenses/([^/?#]+)", rights)
    return m.group(1) if m else LICENCES.get(rights, rights)


def parse(xml: str, site: str, harvested_at: str) -> tuple[list[dict], str | None]:
    """One ListRecords page: its records, and the resumption token for the next page (None when done)."""
    root = ET.fromstring(xml)
    err = root.find(OAI + "error")
    if err is not None:
        if err.get("code") == "noRecordsMatch":
            return [], None
        raise ValueError("OAI-PMH error %s: %s" % (err.get("code"), (err.text or "").strip()))
    out = []
    for rec in root.iter(OAI + "record"):
        head = rec.find(OAI + "header")
        if head is None or head.get("status") == "deleted":
            continue
        pid = head.findtext(OAI + "identifier")
        dc = rec.find(".//{http://www.openarchives.org/OAI/2.0/oai_dc/}dc")
        if not pid or dc is None:
            continue
        vals = lambda tag: [(e.text or "").strip() for e in dc.findall(DC + tag) if (e.text or "").strip()]
        ids = vals("identifier")
        landing = next((i for i in ids if i.startswith(("http://", "https://"))), None)
        out.append({
            "folder": folder_for(site, pid), "site": site, "pid": pid,
            "title": (vals("title") or [pid])[0], "description": " ".join(vals("description")),
            "creators": vals("creator"), "keywords": vals("subject"),
            "licence": _licence((vals("rights") or [None])[0]),
            "formats": sorted({FORMATS.get(f, f) for f in vals("format")}),
            "publisher": (vals("publisher") or [None])[0], "date": (vals("date") or [None])[0],
            "coverage": vals("coverage"), "landing_page": landing,
            "datestamp": head.findtext(OAI + "datestamp"), "harvested_at": harvested_at})
    token = root.find(f".//{OAI}resumptionToken")
    return out, (token.text.strip() if token is not None and (token.text or "").strip() else None)


def harvest(peer: dict, get=fetch) -> list[dict]:
    """Every record a partner lists, following resumption tokens."""
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    records, params = [], {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
    for _ in range(1000):   # a guard against a token that never ends
        page, token = parse(get(peer["oai"], params), peer["name"], now)
        records += page
        if not token:
            return records
        params = {"verb": "ListRecords", "resumptionToken": token}
    raise ValueError("%s: more than 1000 pages" % peer["name"])


def document_text(r: dict) -> str:
    """The passage a harvested record is embedded from: the same shape as this site's own (catalog.document_text)."""
    parts = [r["title"], r.get("description") or "", "Keywords: " + ", ".join(r.get("keywords") or [])]
    if r.get("coverage"):
        parts.append("Coverage: " + "; ".join(r["coverage"]))
    from .catalog import MAX_TEXT
    return "\n".join(p for p in parts if p)[:MAX_TEXT]


def to_row(r: dict) -> dict:
    return {k: json.dumps(r.get(k) or []) if k in LISTS else r.get(k) for k in REMOTE.names}


def from_row(row: dict) -> dict:
    return {k: json.loads(row[k] or "[]") if k in LISTS else row[k] for k in REMOTE.names}


# --- VAST DB ------------------------------------------------------------------------------------------

def ensure_table(schema) -> None:
    if not schema.table("remote", fail_if_missing=False):
        schema.create_table("remote", REMOTE)


def load(session, bucket: str, schema_name: str) -> list[dict]:
    with session.transaction() as tx:
        sch = tx.bucket(bucket).schema(schema_name)
        t = sch.table("remote", fail_if_missing=False)
        return [from_row(x) for x in t.select().read_all().to_pylist()] if t else []


def sync(session, bucket: str, schema_name: str, site: str, records: list[dict], embed) -> dict:
    """Make VAST DB hold exactly these records for the site: fair.remote rows, and their embeddings in
    fair.search. A record whose datestamp hasn't changed keeps its embedding; only new or changed ones
    are embedded."""
    from ibis import _
    from . import catalog
    with session.transaction() as tx:
        sch = tx.bucket(bucket).schema(schema_name)
        catalog.ensure_tables(sch)
        ensure_table(sch)
        remote, search = sch.table("remote"), sch.table("search")
        old = {x["folder"]: x for x in remote.select(predicate=(_.site == site)).read_all().to_pylist()}
    new = {r["folder"]: r for r in records}
    changed = [r for f, r in new.items() if f not in old or old[f]["datestamp"] != r["datestamp"]]
    gone = [f for f in old if f not in new]
    vectors = {r["folder"]: embed(document_text(r)) for r in changed}   # outside the transaction: model calls are slow
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    with session.transaction() as tx:
        sch = tx.bucket(bucket).schema(schema_name)
        remote, search = sch.table("remote"), sch.table("search")
        for f in gone + [r["folder"] for r in changed]:
            catalog._delete_folder(search, f)
        stale = remote.select(columns=["folder"], predicate=(_.site == site), internal_row_id=True).read_all()
        if stale.num_rows:
            remote.delete(stale)
        if new:
            remote.insert(pa.RecordBatch.from_pylist([to_row(r) for r in new.values()], schema=REMOTE))
        if changed:
            rows = [{"folder": r["folder"], "pid": r["pid"], "title": r["title"], "domain": None,
                     "keywords": ", ".join(r["keywords"]), "description": (r["description"] or "")[:2000],
                     "updated_at": now, "embedding": vectors[r["folder"]]} for r in changed]
            search.insert(pa.RecordBatch.from_pylist(rows, schema=catalog.SEARCH))
    return {"records": len(new), "embedded": len(changed), "removed": len(gone)}
