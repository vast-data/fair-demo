"""DCAT and OAI-PMH, built from a published RO-Crate."""
import datetime as dt
import json
import xml.etree.ElementTree as ET

from fairdemo import curate, harvest, rocrate
from tests.test_pipeline import _folder

OAI = "{http://www.openarchives.org/OAI/2.0/}"
DC = "{http://purl.org/dc/elements/1.1/}"
NOW = dt.datetime(2026, 11, 9, 12, 0, tzinfo=dt.timezone.utc)


def _entry(tmp_path, published="2026-11-07T12:33:34+00:00"):
    rec = curate.curate(_folder(tmp_path), use_llm=False)
    docs = rocrate.build(rec, "http://example.test/id", "s3://bucket/x")
    rec.update(pid=docs["pid"], landing_page=docs["landing_page"], status="published", published=published)
    return harvest.entry(rec, docs["ro-crate-metadata.json"])


def _oai(entries, **params):
    xml = harvest.oai_response({k: [v] for k, v in params.items()}, entries, "http://example.test/oai",
                               "Test repository", "admin@example.test", now=NOW)
    return ET.fromstring(xml)


def test_entry_reads_the_published_crate(tmp_path):
    e = _entry(tmp_path)
    assert e["pid"].startswith("ark:/99999/fk4") and e["landing"] == "http://example.test/id/" + e["pid"]
    assert e["title"] == "Test SST" and e["creators"] == ["Ada Lovelace"]
    assert e["license"] == "https://spdx.org/licenses/CC-BY-4.0"
    assert [f["name"] for f in e["files"]] == ["a.nc"] and len(e["files"][0]["sha256"]) == 64


def test_dcat_catalogue(tmp_path):
    e = _entry(tmp_path)
    cat = harvest.dcat_catalog([e], "http://example.test/harvest/dcat", "http://example.test/", "Test repository")
    json.dumps(cat)
    assert cat["@type"] == "dcat:Catalog" and cat["dct:modified"]["@value"] == "2026-11-07T12:33:34Z"
    d = cat["dcat:dataset"][0]
    assert d["@id"] == e["landing"] and d["dct:identifier"] == e["pid"]
    assert d["dct:title"] == "Test SST" and d["dct:description"]
    dist = d["dcat:distribution"]
    nc = next(x for x in dist if x["dct:title"] == "a.nc")
    assert nc["dcat:downloadURL"]["@id"].endswith("/files/a.nc")
    assert nc["spdx:checksum"]["spdx:checksumValue"]["@value"] == e["files"][0]["sha256"]
    assert nc["dct:license"] == {"@id": "https://spdx.org/licenses/CC-BY-4.0", "@type": "dct:LicenseDocument"}
    crate = next(x for x in dist if x["dct:title"] == "RO-Crate metadata")
    assert crate["dct:conformsTo"]["@id"] == "https://w3id.org/ro/crate/1.1"


def test_oai_identify_and_formats(tmp_path):
    e = _entry(tmp_path)
    r = _oai([e], verb="Identify")
    i = r.find(OAI + "Identify")
    assert i.findtext(OAI + "baseURL") == "http://example.test/oai"
    assert i.findtext(OAI + "protocolVersion") == "2.0"
    assert i.findtext(OAI + "earliestDatestamp") == "2026-11-07T12:33:34Z"
    assert r.find(OAI + "request").get("verb") == "Identify"
    f = _oai([e], verb="ListMetadataFormats").find(f"{OAI}ListMetadataFormats/{OAI}metadataFormat")
    assert f.findtext(OAI + "metadataPrefix") == "oai_dc"


def test_oai_records_are_dublin_core(tmp_path):
    e = _entry(tmp_path)
    rec = _oai([e], verb="GetRecord", identifier=e["pid"], metadataPrefix="oai_dc").find(f"{OAI}GetRecord/{OAI}record")
    assert rec.findtext(f"{OAI}header/{OAI}identifier") == e["pid"]
    assert rec.findtext(f"{OAI}header/{OAI}datestamp") == "2026-11-07T12:33:34Z"
    dc = rec.find(f"{OAI}metadata/{{http://www.openarchives.org/OAI/2.0/oai_dc/}}dc")
    assert dc.findtext(DC + "title") == "Test SST" and dc.findtext(DC + "creator") == "Ada Lovelace"
    assert dc.findtext(DC + "type") == "Dataset"
    assert e["landing"] in [x.text for x in dc.findall(DC + "identifier")]
    assert len(_oai([e], verb="ListRecords", metadataPrefix="oai_dc").findall(f"{OAI}ListRecords/{OAI}record")) == 1


def test_oai_date_ranges(tmp_path):
    e = _entry(tmp_path)
    ids = lambda **p: _oai([e], verb="ListIdentifiers", metadataPrefix="oai_dc", **p).findall(f"{OAI}ListIdentifiers/{OAI}header")
    assert len(ids(**{"from": "2026-11-07", "until": "2026-11-07"})) == 1
    assert len(ids(**{"from": "2026-11-07T12:33:34Z"})) == 1
    code = lambda r: r.find(OAI + "error").get("code")
    assert code(_oai([e], verb="ListRecords", metadataPrefix="oai_dc", **{"from": "2026-11-08"})) == "noRecordsMatch"
    # from and until must share a granularity, and from must not come after until
    assert code(_oai([e], verb="ListRecords", metadataPrefix="oai_dc", **{"from": "2026-11-07", "until": "2026-11-07T00:00:00Z"})) == "badArgument"
    assert code(_oai([e], verb="ListRecords", metadataPrefix="oai_dc", **{"from": "2026-11-08", "until": "2026-11-07"})) == "badArgument"


def test_oai_errors(tmp_path):
    e = _entry(tmp_path)
    code = lambda **p: _oai([e], **p).find(OAI + "error").get("code")
    assert code(verb="Nope") == "badVerb"
    assert code(verb="ListRecords") == "badArgument"
    assert code(verb="ListRecords", metadataPrefix="marc21") == "cannotDisseminateFormat"
    assert code(verb="GetRecord", identifier="ark:/99999/none", metadataPrefix="oai_dc") == "idDoesNotExist"
    assert code(verb="ListRecords", resumptionToken="x") == "badResumptionToken"
    assert code(verb="ListSets") == "noSetHierarchy"
    # badVerb and badArgument answers carry no request attributes
    assert not _oai([e], verb="Nope").find(OAI + "request").attrib
    repeated = harvest.oai_response({"verb": ["Identify", "Identify"]}, [e], "http://example.test/oai", "T", "a@example.test")
    assert 'code="badVerb"' in repeated


def test_endpoints_serve_only_published_records(tmp_path, monkeypatch):
    import pytest
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient
    from portal import app as portal

    rec = curate.curate(_folder(tmp_path), use_llm=False)
    docs = rocrate.build(rec, "http://example.test/id", "s3://bucket/x")
    published = dict(rec, pid=docs["pid"], landing_page=docs["landing_page"], status="published",
                     published="2026-11-07T12:33:34+00:00", key_prefix="ocean/sst")
    draft = dict(rec, status="proposed", key_prefix="ocean/draft")

    class Store:  # just what the harvest endpoints read
        def list_records(self):
            return [published, draft]

        def get_bytes(self, key):
            return json.dumps(docs["ro-crate-metadata.json"]).encode() if key == "ocean/sst/ro-crate-metadata.json" else None

    monkeypatch.setattr(portal, "store", Store())
    c = TestClient(portal.app)
    cat = c.get("/harvest/dcat")
    assert cat.status_code == 200 and cat.headers["content-type"].startswith("application/ld+json")
    assert [d["dct:identifier"] for d in cat.json()["dcat:dataset"]] == [docs["pid"]]
    oai = c.get("/oai", params={"verb": "ListIdentifiers", "metadataPrefix": "oai_dc"})
    assert oai.headers["content-type"].startswith("text/xml")
    assert ET.fromstring(oai.text).findtext(f"{OAI}ListIdentifiers/{OAI}header/{OAI}identifier") == docs["pid"]
    assert ET.fromstring(oai.text).findtext(f"{OAI}request") == "http://testserver/oai"
    # OAI-PMH allows POST with form-encoded arguments too
    post = c.post("/oai", data={"verb": "Identify"})
    assert ET.fromstring(post.text).find(f"{OAI}Identify/{OAI}baseURL").text == "http://testserver/oai"
