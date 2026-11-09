"""The catalog of catalogs: harvesting a partner over OAI-PMH, and searching its datasets with this site's."""
import json
import urllib.parse

import pytest

from fairdemo import curate, federation, harvest, rocrate
from tests.test_pipeline import _folder

PEER = {"name": "Site B (demo)", "oai": "http://site-b.test/oai", "slug": "site-b-demo"}


def _partner(tmp_path):
    """A partner site's published dataset, served by this project's own OAI-PMH endpoint code."""
    rec = curate.curate(_folder(tmp_path), use_llm=False)
    docs = rocrate.build(rec, "http://site-b.test/id", "s3://bucket/x")
    rec.update(pid=docs["pid"], landing_page=docs["landing_page"], status="published", published="2026-11-08T09:00:00+00:00")
    entry = harvest.entry(rec, docs["ro-crate-metadata.json"])

    def get(base, params):
        return harvest.oai_response({k: [v] for k, v in params.items()}, [entry], base, "Site B", "a@site-b.test")
    return entry, get


def test_harvest_reads_dublin_core(tmp_path):
    entry, get = _partner(tmp_path)
    [r] = federation.harvest(PEER, get=get)
    assert r["folder"] == "@site-b-demo/" + entry["pid"] and federation.is_remote(r["folder"])
    assert r["site"] == "Site B (demo)" and r["pid"] == entry["pid"]
    assert r["title"] == "Test SST" and r["creators"] == ["Ada Lovelace"]
    assert r["licence"] == "CC-BY-4.0"                      # the SPDX URL, back to its id
    assert r["landing_page"] == entry["landing"]            # where the data lives: the partner's own page
    assert r["datestamp"] == "2026-11-08T09:00:00Z"
    assert federation.from_row(federation.to_row(r)) == r   # survives a round trip through VAST DB's row shape


def test_harvest_follows_resumption_tokens():
    page = lambda n, token: f"""<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><ListRecords>
      <record><header><identifier>ark:/99999/{n}</identifier><datestamp>2026-11-08T00:00:00Z</datestamp></header>
      <metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/">
      <dc:title>Dataset {n}</dc:title><dc:format>text/csv</dc:format><dc:identifier>https://b.test/id/{n}</dc:identifier></oai_dc:dc></metadata></record>
      <record><header status="deleted"><identifier>ark:/99999/gone</identifier><datestamp>2026-11-08T00:00:00Z</datestamp></header></record>
      {f'<resumptionToken>{token}</resumptionToken>' if token else '<resumptionToken/>'}</ListRecords></OAI-PMH>"""
    seen = []

    def get(base, params):
        seen.append(params)
        return page("one", "t1") if "resumptionToken" not in params else page("two", None)
    got = federation.harvest(PEER, get=get)
    assert [r["title"] for r in got] == ["Dataset one", "Dataset two"]   # a deleted record is skipped
    assert got[0]["formats"] == ["CSV"] and seen[1] == {"verb": "ListRecords", "resumptionToken": "t1"}


def test_an_empty_partner_and_a_failing_one():
    empty = '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><error code="noRecordsMatch">none</error></OAI-PMH>'
    assert federation.harvest(PEER, get=lambda b, p: empty) == []
    bad = '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><error code="badArgument">no</error></OAI-PMH>'
    with pytest.raises(ValueError):
        federation.harvest(PEER, get=lambda b, p: bad)


def test_peers_from_the_environment(monkeypatch):
    monkeypatch.setenv("FAIR_PEERS", json.dumps([{"name": "Site B (demo)", "oai": "http://b.test/oai/"}, {"name": "no url"}]))
    assert federation.peers() == [{"name": "Site B (demo)", "oai": "http://b.test/oai", "slug": "site-b-demo"}]


def test_find_searches_both_sites_and_opens_the_partners_record(tmp_path, monkeypatch):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient
    from fairdemo import catalog
    from portal import app as portal

    here = curate.curate(_folder(tmp_path), use_llm=False)
    docs = rocrate.build(here, "http://here.test/id", "s3://bucket/x")
    here.update(pid=docs["pid"], landing_page=docs["landing_page"], status="published",
                published="2026-11-07T12:00:00+00:00", key_prefix="ocean/sst")
    (tmp_path / "b").mkdir()
    _, get = _partner(tmp_path / "b")
    theirs = federation.harvest(PEER, get=get)

    class Store:  # local mode, with one published dataset of its own
        mode, db = "local", None

        def list_records(self):
            return [here]

    # One deterministic "model": both datasets are about sea surface temperature.
    monkeypatch.setattr(catalog, "embed", lambda text: [1.0, 0.0] if "sst" in text.lower() or "sea" in text.lower() else [0.0, 1.0])
    monkeypatch.setattr(portal, "store", Store())
    monkeypatch.setattr(portal, "_local_remote", {r["folder"]: r for r in theirs})
    monkeypatch.setattr(portal, "_local_vectors", {})
    c = TestClient(portal.app)

    got = c.get("/api/find", params={"q": "sea surface temperature"}).json()
    sites = {x["folder"]: x["site"] for x in got["results"]}
    assert sites == {"ocean/sst": None, theirs[0]["folder"]: "Site B (demo)"}
    assert dict(got["facets"]["site"]) == {"here": 1, "Site B (demo)": 1}
    only_here = c.get("/api/find", params={"site": "here"}).json()["results"]
    assert [x["folder"] for x in only_here] == ["ocean/sst"]
    only_b = c.get("/api/find", params={"site": "Site B (demo)"}).json()["results"]
    assert [x["landing_page"] for x in only_b] == [theirs[0]["landing_page"]]
    remote = c.get("/api/remote/" + urllib.parse.quote(theirs[0]["folder"])).json()
    assert remote["record"]["pid"] == theirs[0]["pid"] and remote["facts"]["site"] == "Site B (demo)"
    assert c.get("/api/remote/@site-b-demo/ark:/99999/none").status_code == 404


def test_seed_publishes_through_the_pipeline_once(tmp_path, monkeypatch):
    import sys
    from portal import seed, store as stores
    # LocalStore loads function/main.py as "main" and stubs the DataEngine SDK; put both back afterwards,
    # so the function's own tests get a fresh module.
    names = ("main", "vast", "vast.dataengine", "vast.dataengine.sdk")
    saved = {n: sys.modules.pop(n) for n in names if n in sys.modules}
    try:
        _seed_once(tmp_path, monkeypatch, seed, stores)
    finally:
        for n in names:
            sys.modules.pop(n, None)
        sys.modules.update(saved)


def _seed_once(tmp_path, monkeypatch, seed, stores):
    monkeypatch.setenv("FAIR_CHAT_MODEL", "")
    monkeypatch.setenv("FAIR_EMBED_MODEL", "")
    src = tmp_path / "seed/transport/counts"
    src.mkdir(parents=True)
    (src / "counts.csv").write_text("year,count\n2023,10\n2024,12\n")
    (src / "seed-review.json").write_text(json.dumps({"reviewer": "Test curator", "edits": {
        "title": "Test counts", "description": "Two years of counts.", "creators": [{"name": "Test Lab"}],
        "license": "CC0-1.0", "keywords": ["counts"]}}))
    s = stores.LocalStore(tmp_path / "buckets")
    assert seed.seed(s, tmp_path / "seed", log=lambda m: None) == ["transport/counts"]
    rec = s.get_json("transport/counts/_fair/record.json")
    assert rec["status"] == "published" and rec["pid"].startswith("ark:/99999/fk4")
    assert rec["fields"]["title"]["source"] == "human" and rec["reviewer"] == "Test curator"
    assert seed.seed(s, tmp_path / "seed", log=lambda m: None) == []   # a restart doesn't publish it again
