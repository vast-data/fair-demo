"""Let other catalogs harvest the published records, in the two open standards research catalogs collect from.

  DCAT-AP 3     one JSON-LD catalogue of every published dataset (what national and EU data portals harvest)
  OAI-PMH 2.0   the harvesting protocol of research repositories and aggregators, with Dublin Core records

Both are built from each dataset's published ro-crate-metadata.json, so a harvester sees exactly what was
published, and nothing still under review. Only reads: nothing here changes a record.
"""
from __future__ import annotations

import datetime as dt
import re
import xml.etree.ElementTree as ET

DCAT_CONTEXT = {
    "dcat": "http://www.w3.org/ns/dcat#", "dct": "http://purl.org/dc/terms/", "foaf": "http://xmlns.com/foaf/0.1/",
    "xsd": "http://www.w3.org/2001/XMLSchema#", "spdx": "http://spdx.org/rdf/terms#",
    "skos": "http://www.w3.org/2004/02/skos/core#", "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
}
PUBLIC = "http://publications.europa.eu/resource/authority/access-right/PUBLIC"
IANA = "https://www.iana.org/assignments/media-types/"


def _as_list(v):
    return [] if v is None else v if isinstance(v, list) else [v]


def _text(v) -> str | None:
    if isinstance(v, dict):
        v = v.get("name") or v.get("@id")
    return str(v) if v not in (None, "") else None


def _date(v) -> str | None:
    """An ISO date or date-time from a schema.org value, or None."""
    v = _text(v)
    return v if v and re.match(r"^\d{4}(-\d{2}(-\d{2}([T ][\d:.]+(Z|[+-]\d{2}:?\d{2})?)?)?)?$", v) else None


def entry(record: dict, crate: dict) -> dict:
    """One published dataset, read from its RO-Crate: the fields both harvest formats need."""
    g = {e["@id"]: e for e in crate["@graph"]}
    root = g["./"]
    ref = lambda r: g.get(r["@id"], r) if isinstance(r, dict) and "@id" in r else r
    files = [g[p["@id"]] for p in root.get("hasPart", []) if p["@id"] in g]
    tables = [g[d["@id"]] for d in root.get("distribution", []) if d["@id"].startswith("#table-") and d["@id"] in g]
    landing = root.get("url") or record.get("landing_page")
    return {
        "pid": root.get("identifier") or record.get("pid"), "landing": landing,
        "title": root.get("name"), "description": root.get("description"),
        "keywords": [k.strip() for k in (root["keywords"].split(",") if isinstance(root.get("keywords"), str)
                                         else _as_list(root.get("keywords"))) if str(k).strip()],
        "creators": [_text(ref(c)) for c in _as_list(root.get("creator")) if _text(ref(c))],
        "publisher": _text(ref(root.get("publisher"))),
        "issued": _date(root.get("datePublished")),
        "license": (root.get("license") or {}).get("@id") if isinstance(root.get("license"), dict) else root.get("license"),
        "license_name": _text(g.get((root.get("license") or {}).get("@id"))) if isinstance(root.get("license"), dict) else None,
        "open": bool(root.get("isAccessibleForFree")),
        "temporal": _text(root.get("temporalCoverage")), "spatial": _text(root.get("spatialCoverage")),
        "domain": _text(root.get("about")), "version": _text(root.get("version")),
        "files": [{"name": f["name"], "url": f.get("contentUrl"), "media": f.get("encodingFormat"),
                   "size": f.get("contentSize"), "sha256": f.get("sha256")} for f in files],
        "tables": [{"name": t.get("name"), "url": t.get("contentUrl"), "rows": t.get("contentSize"),
                    "description": t.get("description")} for t in tables],
        # When it was published: the datestamp a harvester uses to ask only for what changed.
        "datestamp": record.get("published") or record.get("reviewed") or record.get("generated"),
    }


# --- DCAT-AP 3 ------------------------------------------------------------------------------------------

def _ref(iri: str, cls: str) -> dict:
    """A link, typed as DCAT-AP expects for that property (a licence is a dct:LicenseDocument, and so on)."""
    return {"@id": iri, "@type": cls}


def _typed(value: str) -> dict:
    return {"@value": value, "@type": "xsd:dateTime" if "T" in value else "xsd:date" if len(value) == 10 else
            "xsd:gYearMonth" if len(value) == 7 else "xsd:gYear"}


def _period(cover: str | None) -> dict | None:
    """An ISO 8601 interval such as 2018-07-25T19:03/2018-08-22 as a dct:PeriodOfTime."""
    if not cover or "/" not in cover:
        return None
    start, end = (_date(x.strip()) for x in cover.split("/", 1))
    if not (start or end):
        return None
    p = {"@type": "dct:PeriodOfTime"}
    if start:
        p["dcat:startDate"] = _typed(start)
    if end:
        p["dcat:endDate"] = _typed(end)
    return p


def _distribution(access, download=None, media=None, size=None, sha256=None, title=None, description=None,
                  license=None, conforms=None):
    d = {"@type": "dcat:Distribution", "dcat:accessURL": _ref(access, "rdfs:Resource")}
    if download:
        d["dcat:downloadURL"] = _ref(download, "rdfs:Resource")
    if title:
        d["dct:title"] = title
    if description:
        d["dct:description"] = description
    if media and "/" in media:
        d["dcat:mediaType"] = _ref(IANA + media, "dct:MediaType")
    if isinstance(size, int):
        d["dcat:byteSize"] = {"@value": str(size), "@type": "xsd:nonNegativeInteger"}
    if sha256:
        d["spdx:checksum"] = {"@type": "spdx:Checksum", "spdx:algorithm": _ref("spdx:checksumAlgorithm_sha256", "spdx:ChecksumAlgorithm"),
                              "spdx:checksumValue": {"@value": sha256, "@type": "xsd:hexBinary"}}
    if license:
        d["dct:license"] = _ref(license, "dct:LicenseDocument")
    if conforms:
        d["dct:conformsTo"] = _ref(conforms, "dct:Standard")
    return d


def dcat_dataset(e: dict) -> dict:
    d = {"@id": e["landing"], "@type": "dcat:Dataset", "dct:identifier": e["pid"], "dct:title": e["title"],
         "dct:description": e["description"] or e["title"], "dcat:landingPage": _ref(e["landing"], "foaf:Document")}
    if e["keywords"]:
        d["dcat:keyword"] = e["keywords"]
    if e["creators"]:
        d["dct:creator"] = [{"@type": "foaf:Agent", "foaf:name": c} for c in e["creators"]]
    if e["publisher"]:
        d["dct:publisher"] = {"@type": "foaf:Agent", "foaf:name": e["publisher"]}
    if e["issued"]:
        d["dct:issued"] = _typed(e["issued"])
    if e["datestamp"]:
        d["dct:modified"] = _typed(_oai_time(e["datestamp"]))
    if e["version"]:
        d["dcat:version"] = e["version"]
    if e["open"]:
        d["dct:accessRights"] = _ref(PUBLIC, "dct:RightsStatement")
    period = _period(e["temporal"])
    if period:
        d["dct:temporal"] = period
    if e["spatial"]:
        d["dct:spatial"] = {"@type": "dct:Location", "skos:prefLabel": e["spatial"]}
    # Each file, downloadable; each data table, queryable over HTTP; and the full record in both standards.
    dist = [_distribution(e["landing"], f["url"], f["media"], f["size"], f["sha256"], title=f["name"],
                          license=e["license"]) for f in e["files"]]
    dist += [_distribution(t["url"], t["url"], "text/csv", title=t["name"], description=t["description"],
                           license=e["license"]) for t in e["tables"] if t["url"]]
    for doc, title, std in (("ro-crate-metadata.json", "RO-Crate metadata", "https://w3id.org/ro/crate/1.1"),
                            ("croissant.json", "Croissant metadata", "http://mlcommons.org/croissant/1.0")):
        url = f"{e['landing']}/{doc}"
        dist.append(_distribution(url, url, "application/ld+json", title=title, license=e["license"], conforms=std))
    d["dcat:distribution"] = dist
    return d


def dcat_catalog(entries: list[dict], url: str, homepage: str, publisher: str) -> dict:
    """The whole catalogue as DCAT-AP 3 JSON-LD."""
    stamps = [_oai_time(e["datestamp"]) for e in entries if e["datestamp"]]
    cat = {"@context": DCAT_CONTEXT, "@id": url, "@type": "dcat:Catalog",
           "dct:title": f"{publisher}: published datasets",
           "dct:description": "Every dataset published by this FAIR portal, harvested from its published RO-Crate metadata.",
           "dct:publisher": {"@type": "foaf:Agent", "foaf:name": publisher},
           "foaf:homepage": _ref(homepage, "foaf:Document"),
           "dcat:dataset": [dcat_dataset(e) for e in entries]}
    if stamps:
        cat["dct:modified"] = _typed(max(stamps))
    return cat


# --- OAI-PMH 2.0 ----------------------------------------------------------------------------------------

OAI = "http://www.openarchives.org/OAI/2.0/"
OAI_DC = "http://www.openarchives.org/OAI/2.0/oai_dc/"
DC = "http://purl.org/dc/elements/1.1/"
XSI = "http://www.w3.org/2001/XMLSchema-instance"
for _p, _u in (("", OAI), ("oai_dc", OAI_DC), ("dc", DC), ("xsi", XSI)):
    ET.register_namespace(_p, _u)
FORMATS = {"oai_dc": ("http://www.openarchives.org/OAI/2.0/oai_dc.xsd", OAI_DC)}
VERBS = {  # verb: (required, optional, exclusive) arguments, as the protocol defines them
    "Identify": (set(), set(), None),
    "ListMetadataFormats": (set(), {"identifier"}, None),
    "ListSets": (set(), set(), "resumptionToken"),
    "ListIdentifiers": ({"metadataPrefix"}, {"from", "until", "set"}, "resumptionToken"),
    "ListRecords": ({"metadataPrefix"}, {"from", "until", "set"}, "resumptionToken"),
    "GetRecord": ({"identifier", "metadataPrefix"}, set(), None),
}


def _oai_time(v: str) -> str:
    """Any ISO date or date-time as the protocol's UTC form, YYYY-MM-DDThh:mm:ssZ."""
    t = dt.datetime.fromisoformat(v.replace("Z", "+00:00")) if "T" in v or " " in v else \
        dt.datetime.fromisoformat(v + "T00:00:00+00:00")
    t = t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _bound(v: str, end: bool) -> str | None:
    """A from/until argument, in either granularity the protocol allows; None if it is malformed."""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return v + ("T23:59:59Z" if end else "T00:00:00Z")
    return v if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", v) else None


def _el(parent, tag, text=None, **attrs):
    e = ET.SubElement(parent, tag, {k: v for k, v in attrs.items() if v is not None})
    if text is not None:
        e.text = str(text)
    return e


def _header(parent, e):
    h = _el(parent, f"{{{OAI}}}header")
    _el(h, f"{{{OAI}}}identifier", e["pid"])
    _el(h, f"{{{OAI}}}datestamp", _oai_time(e["datestamp"]))
    return h


def _oai_dc(parent, e):
    m = _el(parent, f"{{{OAI}}}metadata")
    d = _el(m, f"{{{OAI_DC}}}dc", **{f"{{{XSI}}}schemaLocation": f"{OAI_DC} {FORMATS['oai_dc'][0]}"})
    add = lambda tag, v: v and _el(d, f"{{{DC}}}{tag}", v)
    add("title", e["title"])
    for c in e["creators"]:
        add("creator", c)
    for k in e["keywords"]:
        add("subject", k)
    add("subject", e["domain"])
    add("description", e["description"])
    add("publisher", e["publisher"])
    add("date", e["issued"])
    add("type", "Dataset")
    for media in sorted({f["media"] for f in e["files"] if f["media"]}):
        add("format", media)
    add("identifier", e["landing"])
    add("identifier", e["pid"])
    add("coverage", e["temporal"])
    add("coverage", e["spatial"])
    add("rights", e["license"])
    return m


def oai_response(params: dict[str, list[str]], entries: list[dict], base_url: str, name: str, admin_email: str,
                 now: dt.datetime | None = None) -> str:
    """One OAI-PMH request (its query arguments, each a list of values) answered as the protocol's XML.
    The catalogue is small, so lists come whole: no resumption tokens are ever issued."""
    now = now or dt.datetime.now(dt.timezone.utc)
    root = ET.Element(f"{{{OAI}}}OAI-PMH", {f"{{{XSI}}}schemaLocation": f"{OAI} {OAI}OAI-PMH.xsd"})
    _el(root, f"{{{OAI}}}responseDate", now.strftime("%Y-%m-%dT%H:%M:%SZ"))
    req = _el(root, f"{{{OAI}}}request", base_url)

    def error(code, msg, echo=True):
        if echo:  # badVerb and badArgument answers must not echo the arguments
            for k, v in args.items():
                req.set(k, v)
        _el(root, f"{{{OAI}}}error", msg, code=code)
        return _xml(root)

    verbs = params.get("verb", [])
    if len(verbs) != 1 or verbs[0] not in VERBS:
        args = {}
        return error("badVerb", "missing, repeated or unknown verb", echo=False)
    verb = verbs[0]
    if any(len(v) != 1 for v in params.values()):
        args = {}
        return error("badArgument", "an argument is repeated", echo=False)
    args = {k: v[0] for k, v in params.items()}
    required, optional, exclusive = VERBS[verb]
    given = set(args) - {"verb"}
    if exclusive and exclusive in given:
        if given != {exclusive}:
            args = {}
            return error("badArgument", f"{exclusive} must be the only argument", echo=False)
        return error("badResumptionToken", "this repository never issues resumption tokens")
    if given - required - optional or required - given:
        args = {}
        return error("badArgument", f"{verb} takes {sorted(required) or 'no'} required and {sorted(optional) or 'no'} optional arguments", echo=False)
    for k, v in args.items():
        req.set(k, v)
    by_pid = {e["pid"]: e for e in entries}

    if verb == "Identify":
        i = _el(root, f"{{{OAI}}}Identify")
        _el(i, f"{{{OAI}}}repositoryName", name)
        _el(i, f"{{{OAI}}}baseURL", base_url)
        _el(i, f"{{{OAI}}}protocolVersion", "2.0")
        _el(i, f"{{{OAI}}}adminEmail", admin_email)
        stamps = [_oai_time(e["datestamp"]) for e in entries if e["datestamp"]]
        _el(i, f"{{{OAI}}}earliestDatestamp", min(stamps) if stamps else now.strftime("%Y-%m-%dT%H:%M:%SZ"))
        _el(i, f"{{{OAI}}}deletedRecord", "no")
        _el(i, f"{{{OAI}}}granularity", "YYYY-MM-DDThh:mm:ssZ")
        return _xml(root)
    if verb == "ListSets":
        return error("noSetHierarchy", "this repository has no sets")
    if verb == "ListMetadataFormats":
        if "identifier" in args and args["identifier"] not in by_pid:
            return error("idDoesNotExist", f"no record {args['identifier']}")
        lm = _el(root, f"{{{OAI}}}ListMetadataFormats")
        for prefix, (schema, ns) in FORMATS.items():
            f = _el(lm, f"{{{OAI}}}metadataFormat")
            _el(f, f"{{{OAI}}}metadataPrefix", prefix)
            _el(f, f"{{{OAI}}}schema", schema)
            _el(f, f"{{{OAI}}}metadataNamespace", ns)
        return _xml(root)
    if args["metadataPrefix"] not in FORMATS:
        return error("cannotDisseminateFormat", f"{args['metadataPrefix']} is not offered: use oai_dc")
    if verb == "GetRecord":
        e = by_pid.get(args["identifier"])
        if not e:
            return error("idDoesNotExist", f"no record {args['identifier']}")
        r = _el(_el(root, f"{{{OAI}}}GetRecord"), f"{{{OAI}}}record")
        _header(r, e); _oai_dc(r, e)
        return _xml(root)

    # ListIdentifiers, ListRecords
    if "set" in args:
        return error("noSetHierarchy", "this repository has no sets")
    lo = _bound(args["from"], False) if "from" in args else None
    hi = _bound(args["until"], True) if "until" in args else None
    if ("from" in args and not lo) or ("until" in args and not hi) or \
            ("from" in args and "until" in args and len(args["from"]) != len(args["until"])) or (lo and hi and lo > hi):
        return error("badArgument", "from and until must be dates or UTC date-times of the same granularity, from before until")
    picked = [e for e in sorted(entries, key=lambda e: _oai_time(e["datestamp"]))
              if (not lo or _oai_time(e["datestamp"]) >= lo) and (not hi or _oai_time(e["datestamp"]) <= hi)]
    if not picked:
        return error("noRecordsMatch", "no records in that range")
    out = _el(root, f"{{{OAI}}}{verb}")
    for e in picked:
        if verb == "ListIdentifiers":
            _header(out, e)
        else:
            r = _el(out, f"{{{OAI}}}record")
            _header(r, e); _oai_dc(r, e)
    return _xml(root)


def _xml(root) -> str:
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="unicode")
