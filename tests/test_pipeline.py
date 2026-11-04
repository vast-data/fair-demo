import json

import pytest

from fairdemo import curate, extract, fairscore, rocrate
from tests import fixtures

EXPECT = {"t.nc": "NetCDF", "t.h5": "HDF5", "t.fits": "FITS", "t.root": "ROOT", "t.vcf.gz": "VCF",
          "t.ome.tif": "OME-TIFF", "t.csv": "CSV", "t.parquet": "Parquet"}


@pytest.mark.parametrize("name", list(fixtures.ALL))
def test_profile_every_format(tmp_path, name):
    path = tmp_path / name
    fixtures.ALL[name](path)
    p = extract.profile(path)
    assert p["format"] == EXPECT[name], p["notes"]
    assert not p["notes"], p["notes"]
    assert len(p["sha256"]) == 64
    assert p["variables"], "no variables found in %s" % name


def test_netcdf_facts(tmp_path):
    fixtures.netcdf(tmp_path / "t.nc")
    p = extract.profile(tmp_path / "t.nc")
    sst = next(v for v in p["variables"] if v["name"] == "sst")
    assert sst["standard_name"] == "sea_surface_temperature" and sst["units"] == "degC"
    assert p["coverage"]["time"][0].startswith("2024-09-01")
    assert p["coverage"]["lat"] == [-60.0, 60.0]


def _folder(tmp_path, documented=True):
    d = tmp_path / ("documented" if documented else "dark")
    d.mkdir()
    fixtures.netcdf(d / "a.nc", documented)
    if documented:
        (d / "CITATION.cff").write_text(
            "cff-version: 1.2.0\ntitle: Test SST\nauthors:\n  - given-names: Ada\n    family-names: Lovelace\n"
            "    orcid: https://orcid.org/0000-0002-1825-0097\nlicense: CC-BY-4.0\ndate-released: 2024-10-01\n")
        (d / "regrid.py").write_text("# regrid SST to 0.25 degrees\nimport numpy\n")
    return d


def test_harvest_takes_facts_not_guesses(tmp_path):
    rec = curate.curate(_folder(tmp_path), use_llm=False)
    f = rec["fields"]
    assert f["creators"]["value"][0]["name"] == "Ada Lovelace" and f["creators"]["source"] == "companion"
    assert f["license"]["value"] == "CC-BY-4.0"
    assert f["isBasedOn"]["value"] == ["regrid.py"]
    assert all(x["status"] == "accepted" for x in f.values() if x["source"] in ("header", "companion"))


def test_dark_folder_reports_what_is_missing(tmp_path):
    rec = curate.curate(_folder(tmp_path, documented=False), use_llm=False)
    missing = {k for k, f in rec["fields"].items() if f["status"] == "missing"}
    assert {"title", "creators", "license"} <= missing


def test_score_is_deterministic_and_publish_raises_it(tmp_path):
    rec = curate.curate(_folder(tmp_path), use_llm=False)
    before = fairscore.score(rec)
    assert before == fairscore.score(rec)
    docs = rocrate.build(rec, "http://example.test/id", "s3://bucket/x")
    rec.update(pid=docs["pid"], landing_page=docs["landing_page"], access_url="s3://bucket/x", indexed=True)
    after = fairscore.score(rec, published=True)
    assert after["total"] > before["total"]
    assert docs["pid"].startswith("ark:/99999/fk4")


def test_rocrate_shape(tmp_path):
    rec = curate.curate(_folder(tmp_path), use_llm=False)
    crate = rocrate.build(rec, "http://example.test/id", "s3://bucket/x")["ro-crate-metadata.json"]
    ids = {g["@id"]: g for g in crate["@graph"]}
    assert ids["ro-crate-metadata.json"]["about"] == {"@id": "./"}
    root = ids["./"]
    assert root["license"] == {"@id": "https://spdx.org/licenses/CC-BY-4.0"}
    assert root["creator"] == [{"@id": "https://orcid.org/0000-0002-1825-0097"}]
    assert ids["a.nc"]["sha256"] == rec["files"][0]["sha256"]
    json.dumps(crate)


def test_review_edit_wins(tmp_path):
    rec = curate.curate(_folder(tmp_path, documented=False), use_llm=False)
    rec = curate.apply_review(rec, {"license": {"action": "edit", "value": "CC0-1.0", "by": "orcid:0000"}})
    assert curate.accepted(rec)["license"] == "CC0-1.0"
    assert rec["fields"]["license"]["source"] == "human"


def test_publish_refuses_without_licence(tmp_path):
    rec = curate.curate(_folder(tmp_path, documented=False), use_llm=False)
    with pytest.raises(rocrate.NotReady) as e:
        rocrate.build(rec, "http://example.test/id", "s3://bucket/x")
    assert "license" in e.value.missing and "creators" in e.value.missing


def test_search_text_and_variable_rows(tmp_path):
    from fairdemo import catalog
    rec = curate.curate(_folder(tmp_path), use_llm=False)
    rec["key_prefix"] = "climate/documented"
    text = catalog.document_text(rec)
    assert "Test SST" in text and "sea_surface_temperature" in text
    rows = catalog.variable_rows(rec, "ark:/99999/fk4x", "Test SST")
    sst = next(r for r in rows if r["name"] == "sst")
    assert sst["standard_name"] == "sea_surface_temperature" and sst["units"] == "degC" and sst["source"] == "header"


def test_llm_retry_builds_a_fresh_request(monkeypatch):
    import urllib.error
    from fairdemo import llm
    monkeypatch.setenv("LITELLM_BASE_URL", "https://gateway.test")
    monkeypatch.setenv("LITELLM_API_KEY", "k")
    seen = []

    def fake_urlopen(req, timeout):
        seen.append(req)
        if len(seen) == 1:
            raise urllib.error.URLError("transient")
        import io
        return io.BytesIO(b'{"data": [{"index": 0, "embedding": [1.0]}]}')
    monkeypatch.setattr(llm.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(llm.time, "sleep", lambda s: None)
    assert llm.embed(["x"], model="test-embed") == [[1.0]]
    assert len(seen) == 2 and seen[0] is not seen[1]


def test_variable_match_treats_underscores_as_spaces():
    from fairdemo import catalog
    rows = [{"name": "sst", "standard_name": None, "units": "Celsius", "description": "Daily sea surface temperature",
             "dataset_title": "OISST"},
            {"name": "t", "standard_name": "sea_surface_temperature", "units": "K", "description": None, "dataset_title": "B"}]
    hits = catalog.match_variables(rows, "sea_surface_temperature")
    assert [h["name"] for h in hits] == ["t", "sst"]  # the exact CF name first


def test_backlog_groups_folders_and_marks_status():
    from fairdemo import sweep
    F = lambda path, name, size=10, ext=None: {"parent_path": path, "name": name, "size": size, "extension": ext,
                                               "mtime": 1_700_000_000_000_000_000, "element_type": "FILE"}
    rows = sweep.backlog([
        F("/fair-estate/scratch/u1/", "out_0901.nc", 500, "nc"),
        F("/fair-estate/lab/run2/", "a.csv", 50, "csv"), F("/fair-estate/lab/run2/", "README.md"),
        F("/fair-estate/lab/run2/", "_READY"),
        F("/fair-raw/climate/x/", "b.nc", 70, "nc"), F("/fair-raw/climate/x/", "ro-crate-metadata.json"),
        F("/fair-estate/docs-only/", "notes.txt"),
        F("/fair-raw/climate/y/", "c.nc", 9, "nc"), F("/fair-raw/climate/y/_fair/", "record.json"),
        F("/fair-raw/.vast/extended_triggers/", "abc"),
    ])
    by = {r["path"]: r for r in rows}
    assert set(by) == {"/fair-estate/scratch/u1/", "/fair-estate/lab/run2/", "/fair-raw/climate/x/", "/fair-raw/climate/y/"}
    assert by["/fair-raw/climate/y/"]["status"] == "review"
    assert by["/fair-estate/scratch/u1/"]["status"] == "dark" and by["/fair-estate/scratch/u1/"]["docs"] == []
    assert by["/fair-estate/lab/run2/"]["status"] == "queued" and by["/fair-estate/lab/run2/"]["docs"] == ["readme"]
    assert by["/fair-raw/climate/x/"]["status"] == "curated"
    assert rows[0]["path"] == "/fair-estate/scratch/u1/"  # dark first
    assert by["/fair-estate/scratch/u1/"]["bucket"] == "fair-estate" and by["/fair-estate/scratch/u1/"]["prefix"] == "scratch/u1"


def test_local_multipart_upload_round_trip(tmp_path):
    from portal.store import LocalS3
    events = []
    s3 = LocalS3(tmp_path, on_put=lambda b, k: events.append((b, k)))
    uid = s3.create_multipart_upload(Bucket="fair-raw", Key="d/x/big.bin")["UploadId"]
    parts = [{"PartNumber": n, "ETag": s3.upload_part(Bucket="fair-raw", Key="d/x/big.bin", UploadId=uid,
                                                       PartNumber=n, Body=bytes([n]) * 10)["ETag"]} for n in (2, 1)]
    s3.complete_multipart_upload(Bucket="fair-raw", Key="d/x/big.bin", UploadId=uid,
                                 MultipartUpload={"Parts": sorted(parts, key=lambda p: p["PartNumber"])})
    assert (tmp_path / "fair-raw/d/x/big.bin").read_bytes() == bytes([1]) * 10 + bytes([2]) * 10
    assert events == [("fair-raw", "d/x/big.bin")]  # one event, as S3 raises on completion


def test_rank_adds_a_small_boost_for_query_words():
    from fairdemo import catalog
    rows = [{"title": "Road traffic counts", "keywords": "traffic, roads", "domain": "transport", "similarity": 0.40},
            {"title": "Daily sea surface temperature", "keywords": "ocean, sst", "domain": "climate", "similarity": 0.37}]
    got = catalog.rank("ocean", rows)
    assert [r["title"] for r in got] == ["Daily sea surface temperature", "Road traffic counts"]
    assert got[0]["meaning"] == 0.37 and got[0]["words"] == 1.0 and got[0]["similarity"] == round(0.37 + catalog.WORD_BOOST, 3)
    assert got[1]["similarity"] == 0.40   # no query word in it: meaning alone
