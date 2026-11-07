import json

import awkward as ak
import numpy as np
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


@pytest.mark.parametrize("name,rows,first", [("t.csv", 2, "date"), ("t.parquet", 2, "id"), ("t.fits", 3, "time"),
                                             ("t.nc", 60, "time"), ("t.vcf.gz", 3, "chrom"), ("t.root", 3, "entry")])
def test_every_tabular_format_becomes_a_table(tmp_path, name, rows, first):
    from fairdemo import tables
    p = tmp_path / name
    fixtures.ALL[name](p)
    parts = tables.convert(p, extract.profile(p))
    part, t, sources = parts[0]
    assert t.num_rows == rows and t.column_names[0] == first
    assert all(s is None or s in {v["name"] for v in extract.profile(p)["variables"]} or s in ("CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER")
               for s in sources.values())


def test_root_jagged_branches_become_a_child_table(tmp_path):
    import uproot
    from fairdemo import tables
    p = tmp_path / "j.root"
    with uproot.recreate(p) as f:
        f["Events"] = {"nMuon": np.array([2, 1], dtype=np.int32),
                       "Muon": ak.zip({"pt": ak.Array([[20.0, 30.0], [40.0]]), "eta": ak.Array([[0.1, 0.2], [0.3]])})}
    parts = {part: t for part, t, _ in tables.convert(p, extract.profile(p))}
    muon = next(t for k, t in parts.items() if k.lower().endswith("muon"))
    assert muon.to_pydict()["entry"] == [0, 0, 1] and muon.to_pydict()["idx"] == [0, 1, 0]
    assert sorted(c for c in muon.column_names if c not in ("entry", "idx")) == ["eta", "pt"]


def test_filters_are_parsed_never_executed():
    from fairdemo import tables
    cols = [{"name": "lat", "type": "float"}, {"name": "name", "type": "string"}, {"name": "n", "type": "int64"}]
    assert tables.parse_where(["lat>=40", "name=O'Brien", "n!=3"], cols) == [
        ("lat", ">=", 40.0), ("name", "=", "O'Brien"), ("n", "!=", 3)]
    for bad in ("lat>=forty", "nope=1", "lat; drop table x", "lat ~ 3"):
        with pytest.raises(ValueError):
            tables.parse_where([bad], cols)
    entry = {"location": "fair-db/data/t"}
    assert "'O''Brien'" in tables.sql(entry, ["name"], [("name", "=", "O'Brien")], 5)


def test_messy_csv_still_becomes_a_table(tmp_path):
    """A code column with letters far down, a ragged row, and Latin-1 text."""
    from fairdemo import tables
    p = tmp_path / "messy.csv"
    rows = ["id,code,city,value"] + ["%d,%d,Zürich,%d.5" % (i, i, i) for i in range(5000)]
    rows += ["5000,A12,Zürich,1.0", "5001,7", "5002,8,Köln,2.5"]
    p.write_bytes("\n".join(rows).encode("latin-1"))
    notes = []
    (part, t, _), = tables.convert(p, extract.profile(p), notes)
    assert t.num_rows == 5002 and str(t.schema.field("code").type) == "string"
    assert str(t.schema.field("value").type) == "double" and t.column("city")[0].as_py() == "Zürich"
    assert any("malformed" in n for n in notes)


def test_awkward_types_become_portable_ones(tmp_path):
    import datetime as dt
    import decimal
    import pyarrow as pa, pyarrow.parquet as pq
    from fairdemo import tables
    p = tmp_path / "types.parquet"
    pq.write_table(pa.table({
        "u32": pa.array([1, 2], pa.uint32()), "u64big": pa.array([2 ** 64 - 1, 1], pa.uint64()),
        "dec": pa.array([decimal.Decimal("1.25"), None], pa.decimal128(5, 2)),
        "ts": pa.array([dt.datetime(2024, 1, 1, 0, 0, 0, 1)] * 2, pa.timestamp("ns", tz="UTC")),
        "tod": pa.array([dt.time(1, 2), dt.time(3, 4)], pa.time64("us")),
        "nested": pa.array([{"a": 1}, {"a": 2}]), "blob": pa.array([b"x", b"y"], pa.binary())}), p)
    notes = []
    (_, t, _), = tables.convert(p, extract.profile(p), notes)
    types = {f.name: str(f.type) for f in t.schema}
    assert types == {"u32": "int64", "u64big": "string", "dec": "double", "ts": "timestamp[us]", "tod": "string"}
    assert sum("left out" in n for n in notes) == 2


def test_netcdf_text_variables_are_left_in_the_file(tmp_path):
    import netCDF4
    from fairdemo import tables
    p = tmp_path / "s.nc"
    with netCDF4.Dataset(p, "w") as ds:
        ds.createDimension("station", 3); ds.createDimension("nchar", 4)
        ds.createVariable("name", "S1", ("station", "nchar"))[:] = netCDF4.stringtochar(np.array(["abcd", "efgh", "ijkl"], "S4"))
        ds.createVariable("temp", "f4", ("station",))[:] = [1.5, 2.5, 3.5]
    (_, t, _), = tables.convert(p, extract.profile(p))
    assert t.column_names == ["station", "temp"] and t.num_rows == 3


def test_a_file_over_the_byte_limit_is_not_read(tmp_path, monkeypatch):
    from fairdemo import tables
    p = tmp_path / "t.csv"
    fixtures.csv(p)
    monkeypatch.setattr(tables, "MAX_FILE_BYTES", 10)
    with pytest.raises(tables.TooBig):
        tables.convert(p, extract.profile(p))


def test_junk_and_empty_files_make_no_table(tmp_path):
    import os
    from fairdemo import tables
    junk, empty = tmp_path / "r.csv", tmp_path / "h.csv"
    junk.write_bytes(b"a,b\n" + os.urandom(4096))
    empty.write_text("a,b,c\n")
    with pytest.raises(ValueError):
        tables.convert(junk, extract.profile(junk))
    notes = []
    assert tables.convert(empty, extract.profile(empty), notes) == [] and "no rows" in notes[0]


def _toy_table(tmp_path):
    import pyarrow as pa
    from fairdemo import tables
    t = pa.table({"lat": pa.array([10.0, 55.0, 58.0, 70.0], pa.float32()), "lon": pa.array([10.0, 335.0, 345.0, 355.0], pa.float32()),
                  "sst": pa.array([25.0, 12.0, 14.0, None], pa.float32()), "region": ["S", "N", "N", "Arctic"]})
    sink = tables.LocalTables(tmp_path / "db"); sink.replace("toy", t)
    cols = tables.column_stats(t, {"lat": "lat", "lon": "lon", "sst": "sst", "region": "region"})
    for c in cols:
        c["units"] = {"lat": "degrees_north", "lon": "degrees_east", "sst": "Celsius"}.get(c["name"])
    entry = {"table": "toy", "location": "fair-db/data/toy", "rows": 4, "source": "toy.nc", "columns": cols}
    return sink, entry


def test_aggregates_and_ordering(tmp_path):
    from fairdemo import tables
    sink, entry = _toy_table(tmp_path)
    out = tables.query(sink, entry, None, ["lat>=50"], 10, aggregate={"fn": "avg", "column": "sst", "by": "region"},
                       order={"column": "avg_sst", "desc": True})
    assert out["rows"].to_pylist() == [{"region": "N", "avg_sst": 13.0}, {"region": "Arctic", "avg_sst": None}] or \
        out["rows"].to_pylist()[0] == {"region": "N", "avg_sst": 13.0}
    assert "GROUP BY \"region\"" in out["sql"] and "ORDER BY avg_sst DESC" in out["sql"]
    top = tables.query(sink, entry, ["lat", "sst"], [], 2, order={"column": "sst", "desc": True})
    assert top["rows"].column("sst").to_pylist() == [25.0, 14.0]
    assert tables.query(sink, entry, None, [], 5, aggregate={"fn": "count"})["rows"].to_pylist() == [{"count": 4}]
    for bad in ({"fn": "avg", "column": "region"}, {"fn": "median", "column": "sst"}, {"fn": "max", "column": "nope"}):
        with pytest.raises(ValueError):
            tables.query(sink, entry, None, [], 5, aggregate=bad)
    with pytest.raises(ValueError):
        tables.query(sink, entry, None, [], 5, order={"column": "nope"})


def test_questions_become_checked_queries(tmp_path, monkeypatch):
    """The model proposes; the proposal is checked like any query. One wrong answer is sent back with the
    reason; a second is refused. Western longitudes are converted here, not by the model."""
    from fairdemo import llm, nlquery
    sink, entry = _toy_table(tmp_path)
    record = {"tables": [entry]}
    replies, prompts = [], []

    def fake(system, user, **kw):
        prompts.append(user)
        return replies.pop(0)
    monkeypatch.setattr(llm, "chat_json", fake)
    good = {"table": "toy", "columns": ["sst"], "where": ["lat >= 50", "lon >= 330", "lon <= 350"],
            "aggregate": {"fn": "avg", "column": "sst", "by": None}, "limit": 20, "explanation": "average sst in the box"}
    replies[:] = [good]
    q = nlquery.interpret("average temperature north of 50N between 30 and 10 degrees west", record, "Toy")
    assert q["where"] == good["where"] and q["aggregate"]["fn"] == "avg"
    assert "30 degrees west = 330, 10 degrees west = 350" in prompts[-1] and "values such as 'N'" in prompts[-1]
    # a made-up column, then corrected
    replies[:] = [dict(good, columns=["temperature"]), good]
    assert nlquery.interpret("q", record, "Toy")["columns"] == ["sst"] and "refused: no column temperature" in prompts[-1]
    # an empty range is a slip, sent back
    replies[:] = [dict(good, where=["lon >= 330", "lon <= 330"]), good]
    nlquery.interpret("q", record, "Toy")
    assert "select nothing" in prompts[-1]
    # wrong twice: refused
    replies[:] = [dict(good, where=["lat >= north"]), dict(good, where=["sst; drop table toy"])]
    with pytest.raises(ValueError):
        nlquery.interpret("q", record, "Toy")
    replies[:] = [{"unanswerable": "no funding information"}]
    assert nlquery.interpret("who funded it?", record, "Toy") == {"unanswerable": "no funding information"}


def test_a_converter_that_dies_only_loses_its_own_table(tmp_path, monkeypatch):
    """On the cluster each file converts in a child process: if the kernel kills it for memory (SIGKILL)
    or a library crashes, the load notes it and the function carries on."""
    import os, signal, sys as _sys
    from fairdemo import tables
    if not hasattr(os, "fork"):
        pytest.skip("needs fork")
    monkeypatch.setattr(tables, "ISOLATE", True)
    sink = tables.LocalTables(tmp_path / "db")
    p = tmp_path / "t.csv"
    fixtures.csv(p)
    prof = extract.profile(p)
    record = {"key_prefix": "test/ds", "files": [dict(prof, name="t.csv")], "variables": prof["variables"]}
    got, notes = tables.load(sink, record, lambda name: p)
    assert [t["table"] for t in got] == ["test_ds"] and got[0]["rows"] == 2 and not notes
    monkeypatch.setattr(tables, "convert", lambda *a, **k: os.kill(os.getpid(), signal.SIGKILL))
    got, notes = tables.load(sink, record, lambda name: p)
    assert got == [] and "ran out of memory or crashed" in notes[0]


def test_vast_predicates_name_columns_not_methods():
    """VCF tables have a column called filter (and CSVs can have count): the predicate must mean the
    column, not ibis's Table.filter method, or VAST DB is handed True instead of a comparison."""
    import ibis
    from fairdemo import tables
    t = ibis.table({"filter": "string", "count": "int64", "af": "float64"}, name="t")
    pred = tables.vast_predicate([("filter", "!=", "PASS"), ("count", ">", 3), ("af", ">=", 0.05)]).resolve(t)
    assert type(pred.op()).__name__ == "And"
    assert "NotEquals" in repr(pred) and "Greater" in repr(pred)


def test_suggested_questions_are_checked_like_any_query(tmp_path, monkeypatch):
    from fairdemo import llm, nlquery
    sink, entry = _toy_table(tmp_path)
    record = {"tables": [entry]}
    monkeypatch.setattr(llm, "chat_json", lambda *a, **k: {"questions": [
        {"question": "Average sea temperature by region", "table": "toy", "columns": [], "where": [],
         "aggregate": {"op": "average", "column": "sst", "group_by": "region"}, "order": None, "limit": 10},
        {"question": "Warmest 2 places", "table": "toy", "columns": ["lat", "sst"], "where": [], "aggregate": None,
         "order": {"column": "sst", "direction": "desc"}, "limit": 2},
        {"question": "Average sst per region", "table": "toy", "columns": [], "where": [],      # says per, no grouping
         "aggregate": {"fn": "avg", "column": "sst"}, "order": None, "limit": 10},
        {"question": "Made-up column", "table": "toy", "columns": ["depth"], "where": [], "limit": 5},
        {"question": "warmest 2 places", "table": "toy", "columns": ["sst"], "where": [], "limit": 2}]})   # duplicate
    got = nlquery.suggest(record, "Toy", n=6)
    assert [g["question"] for g in got] == ["Average sea temperature by region", "Warmest 2 places"]
    assert got[0]["query"]["aggregate"] == {"fn": "avg", "column": "sst", "by": "region"}
    assert got[1]["query"]["order"] == {"column": "sst", "desc": True}


def test_a_ranked_total_needs_a_grouping(tmp_path, monkeypatch):
    """'Top 10 busiest roads' read as one grand total, sorted, is a slip: refused, so the model corrects it."""
    from fairdemo import llm, nlquery
    sink, entry = _toy_table(tmp_path)
    bad = {"table": "toy", "columns": [], "where": [], "aggregate": {"fn": "sum", "column": "sst"},
           "order": {"column": "sum_sst", "desc": True}, "limit": 10}
    good = dict(bad, aggregate={"fn": "sum", "column": "sst", "by": "region"})
    replies, prompts = [bad, good], []
    monkeypatch.setattr(llm, "chat_json", lambda s, u, **k: prompts.append(u) or replies.pop(0))
    q = nlquery.interpret("top regions by total sst", {"tables": [entry]}, "Toy")
    assert q["aggregate"]["by"] == "region" and "group it with" in prompts[-1]


def test_nan_is_stored_as_missing_and_answers_stay_valid_json(tmp_path):
    """A TESS light curve marks gaps in the flux with NaN: averages skip them and the portal can send every row."""
    import math
    import pyarrow as pa
    from astropy.io import fits as F
    from fairdemo import tables
    from portal.app import _json_rows
    p = tmp_path / "lc.fits"
    flux = np.array([1.0, np.nan, 3.0], dtype=np.float32)
    cols = F.ColDefs([F.Column(name="TIME", format="D", array=np.arange(3.0)), F.Column(name="PDCSAP_FLUX", format="E", array=flux)])
    F.HDUList([F.PrimaryHDU(), F.BinTableHDU.from_columns(cols, name="LIGHTCURVE")]).writeto(p)
    _, t, sources = tables.convert(p, extract.profile(p))[0]
    assert t.column("pdcsap_flux").null_count == 1   # names are lower-cased on load
    sink = tables.LocalTables(tmp_path / "db"); sink.replace("lc", t)
    entry = {"table": "lc", "location": "fair-db/data/lc", "rows": 3, "source": "lc.fits", "columns": tables.column_stats(t, sources)}
    avg = tables.query(sink, entry, None, [], 5, aggregate={"fn": "avg", "column": "pdcsap_flux"})
    assert avg["rows"].to_pylist() == [{"avg_pdcsap_flux": 2.0}]
    rows = tables.query(sink, entry, None, [], 5, with_summary=True)
    json.dumps(_json_rows(rows["rows"]), allow_nan=False)
    json.dumps(rows["summary"], allow_nan=False)
    # A table loaded before this fix still holds NaN and infinity: the rows go out as null.
    old = pa.table({"x": pa.array([1.5, math.nan, math.inf, None], pa.float64()), "y": pa.array([math.nan, 2.0, 3.0, 4.0], pa.float32())})
    assert _json_rows(old) == [{"x": 1.5, "y": None}, {"x": None, "y": 2.0}, {"x": None, "y": 3.0}, {"x": None, "y": 4.0}]
