"""The DataEngine function's event routing, against an in-memory S3 and table store (no cluster)."""
import importlib
import io
import json
import sys
import types
from pathlib import Path

import pytest

from tests import fixtures

ROOT = Path(__file__).resolve().parents[1]


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, **_):
        self.objects[(Bucket, Key)] = Body if isinstance(Body, bytes) else Body.encode()

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise KeyError(Key)
        return {}

    def download_file(self, bucket, key, dest):
        Path(dest).write_bytes(self.objects[(bucket, key)])

    def get_paginator(self, _):
        s3 = self

        class P:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k, "Size": len(v)} for (b, k), v in s3.objects.items()
                                    if b == Bucket and k.startswith(Prefix)]}
        return P()


class Logger:
    def info(self, msg):
        print(msg)


@pytest.fixture
def fn(monkeypatch):
    sdk = types.ModuleType("vast.dataengine.sdk")
    sdk.Context = sdk.VastEvent = object
    for name in ("vast", "vast.dataengine"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, "vast.dataengine.sdk", sdk)
    monkeypatch.syspath_prepend(str(ROOT / "function"))
    main = importlib.import_module("main")
    tables = {"files": [], "records": [], "tables": []}
    monkeypatch.setattr(main, "insert", lambda t, row: tables[t].append(row))
    monkeypatch.setattr(main, "select", lambda t, folder=None, sha256=None: [
        r for r in tables[t] if (r["folder"] == folder if folder is not None else r.get("sha256") == sha256)])
    main._clients.clear()
    main._clients.update(s3=FakeS3(), db=None)
    main.indexed = []
    monkeypatch.setattr(main.catalog, "index_record", lambda db, b, s, rec: main.indexed.append(rec["key_prefix"]) or {})
    main.tables = tables
    # No model in unit tests: curation runs on facts only.
    real = main.curate.curate_profiles
    monkeypatch.setattr(main.curate, "curate_profiles",
                        lambda *a, **k: real(*a[:4], use_llm=False))
    yield main
    sys.modules.pop("main", None)


def event(bucket, key):
    e = types.SimpleNamespace(bucket=bucket, object_key=key)
    return types.SimpleNamespace(as_element_event=lambda: e)


def put_file(main, key, maker, tmp_path):
    p = tmp_path / Path(key).name
    maker(p)
    main._clients["s3"].put_object(Bucket="fair-raw", Key=key, Body=p.read_bytes())


def test_profile_curate_review_publish(fn, tmp_path):
    ctx = types.SimpleNamespace(logger=Logger())
    put_file(fn, "climate/sst/a.nc", fixtures.netcdf, tmp_path)
    assert fn.handler(ctx, event("fair-raw", "climate/sst/a.nc"))["format"] == "NetCDF"

    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/CITATION.cff", Body=(
        "cff-version: 1.2.0\ntitle: SST\nauthors:\n  - name: Test Ocean Lab\nlicense: CC-BY-4.0\n"))
    assert "skipped" in fn.handler(ctx, event("fair-raw", "climate/sst/CITATION.cff"))

    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_READY", Body=b"")
    out = fn.handler(ctx, event("fair-raw", "climate/sst/_READY"))
    rec = json.loads(fn.tables["records"][-1]["record"])
    assert rec["fields"]["license"]["value"] == "CC-BY-4.0"
    assert ("fair-raw", "climate/sst/_fair/record.json") in fn._clients["s3"].objects

    review = {"reviewer": "ada", "decisions": {"keywords": {"action": "edit", "value": ["sst", "ocean", "satellite",
                                                                                       "temperature", "daily"]}}}
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_fair/review.json", Body=json.dumps(review))
    pub = fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))
    assert pub["pid"].startswith("ark:/99999/fk4")
    last = fn.tables["records"][-1]
    assert last["status"] == "published" and last["version"] == 2 and last["actor"] == "ada"
    assert last["score_after"] > last["score_before"]
    crate = json.loads(fn._clients["s3"].objects[("fair-raw", "climate/sst/ro-crate-metadata.json")])
    assert any(g.get("@type") == "Dataset" for g in crate["@graph"])
    f = next(g for g in crate["@graph"] if g.get("@type") == "File")
    assert f["contentUrl"].startswith("http") and "/files/a.nc" in f["contentUrl"]  # downloadable over HTTP
    assert f["sameAs"] == "s3://fair-raw/climate/sst/a.nc"
    assert any(b == "fair-docs" for b, _ in fn._clients["s3"].objects)
    assert fn.indexed == ["climate/sst"]

    # The function's own outputs come back as events and are ignored.
    for k in ("climate/sst/ro-crate-metadata.json", "climate/sst/_fair/record.json"):
        assert "skipped" in fn.handler(ctx, event("fair-raw", k))


def test_ready_before_file_event(fn, tmp_path):
    """_READY handled before the data file's own event: curation profiles the file itself."""
    ctx = types.SimpleNamespace(logger=Logger())
    put_file(fn, "climate/fast/a.nc", fixtures.netcdf, tmp_path)
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/fast/_READY", Body=b"")
    fn.handler(ctx, event("fair-raw", "climate/fast/_READY"))
    rec = json.loads(fn.tables["records"][-1]["record"])
    assert [f["name"] for f in rec["files"]] == ["a.nc"]


def test_redelivered_review_is_applied_once(fn, tmp_path):
    ctx = types.SimpleNamespace(logger=Logger())
    put_file(fn, "climate/sst/a.nc", fixtures.netcdf, tmp_path)
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/CITATION.cff", Body=(
        "cff-version: 1.2.0\ntitle: SST\nauthors:\n  - name: Lab\nlicense: CC-BY-4.0\n"))
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_READY", Body=b"")
    fn.handler(ctx, event("fair-raw", "climate/sst/_READY"))
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_fair/review.json", Body=b'{"decisions": {}}')
    fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))
    n = len(fn.tables["records"])
    assert "skipped" in fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))
    assert len(fn.tables["records"]) == n


def test_estate_folder_is_curated_in_place(fn, tmp_path):
    """A backlog folder outside the landing bucket: no per-file events, just _READY; its id carries the bucket."""
    ctx = types.SimpleNamespace(logger=Logger())
    p = tmp_path / "out.nc"
    fixtures.netcdf(p)
    fn._clients["s3"].put_object(Bucket="fair-estate", Key="scratch/u1/out.nc", Body=p.read_bytes())
    fn._clients["s3"].put_object(Bucket="fair-estate", Key="scratch/u1/LICENSE", Body=b"Creative Commons Zero CC0")
    fn._clients["s3"].put_object(Bucket="fair-estate", Key="scratch/u1/_READY", Body=b"")
    fn.handler(ctx, event("fair-estate", "scratch/u1/_READY"))
    rec = json.loads(fn.tables["records"][-1]["record"])
    assert rec["key_prefix"] == "fair-estate/scratch/u1" and rec["bucket"] == "fair-estate"
    assert fn.tables["files"][-1]["folder"] == "fair-estate/scratch/u1"
    assert ("fair-estate", "scratch/u1/_fair/record.json") in fn._clients["s3"].objects


def test_duplicates_ignore_deleted_copies(fn, tmp_path):
    ctx = types.SimpleNamespace(logger=Logger())
    for folder in ("climate/a", "climate/b"):
        put_file(fn, f"{folder}/x.nc", fixtures.netcdf, tmp_path)
        fn.handler(ctx, event("fair-raw", f"{folder}/x.nc"))
    del fn._clients["s3"].objects[("fair-raw", "climate/b/x.nc")]   # the copy in b is deleted later
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/a/_READY", Body=b"")
    fn.handler(ctx, event("fair-raw", "climate/a/_READY"))
    rec = json.loads(fn.tables["records"][-1]["record"])
    assert "duplicates" not in rec


def test_publish_loads_the_data_as_a_queryable_table(fn, tmp_path):
    """The published data itself becomes a table: columns carry the file's units and CF names, the crate
    lists it as a distribution, and a filtered read is pushed down to the table."""
    from fairdemo import tables
    fn.table_sink = tables.LocalTables(tmp_path / "db")
    ctx = types.SimpleNamespace(logger=Logger())
    put_file(fn, "climate/sst/a.nc", fixtures.netcdf, tmp_path)
    fn.handler(ctx, event("fair-raw", "climate/sst/a.nc"))
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_READY", Body=b"")
    fn.handler(ctx, event("fair-raw", "climate/sst/_READY"))
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_fair/review.json",
                                 Body=json.dumps({"reviewer": "ada", "decisions": {}}))
    assert "pid" in fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))
    rec = json.loads(fn._clients["s3"].objects[("fair-raw", "climate/sst/_fair/record.json")])
    (t,) = rec["tables"]
    assert t["table"] == "climate_sst" and t["location"] == "fair-db/data/climate_sst" and t["rows"] == 60
    sst = next(c for c in t["columns"] if c["name"] == "sst")
    assert sst["units"] == "degC" and sst["standard_name"] == "sea_surface_temperature"
    assert [c["name"] for c in t["columns"]][:3] == ["time", "lat", "lon"]
    assert fn.tables["tables"][-1]["source_sha256"] == rec["files"][0]["sha256"]
    crate = json.loads(fn._clients["s3"].objects[("fair-raw", "climate/sst/ro-crate-metadata.json")])
    dl = next(g for g in crate["@graph"] if g["@id"] == "#table-climate_sst")
    assert dl["@type"] == "DataDownload" and "/tables/climate_sst" in dl["contentUrl"]
    assert "Query it in place" in fn._clients["s3"].objects[("fair-raw", "climate/sst/ro-crate-preview.html")].decode()

    out = tables.query(fn.table_sink, t, ["lat", "sst"], ["lat>=20"], 100, with_summary=True)
    assert out["rows"].num_rows == 30 and min(out["rows"]["lat"].to_pylist()) == 20
    assert out["matched"] == 30 and out["summary"][1]["column"] == "sst"

    assert out["sql"] == 'SELECT "lat", "sst"\nFROM vast."fair-db/data".climate_sst\nWHERE "lat" >= 20.0\nLIMIT 100'

    # Republished unchanged: the table is kept, not loaded again.
    before = fn.table_sink._path("climate_sst").stat().st_mtime_ns
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_fair/review.json",
                                 Body=json.dumps({"reviewer": "bob", "decisions": {}}))
    fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))
    assert fn.table_sink._path("climate_sst").stat().st_mtime_ns == before

    # And again: finished loads don't count towards the guard against loads that stop the function.
    for who in ("cy", "di"):
        fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_fair/review.json",
                                     Body=json.dumps({"reviewer": who, "decisions": {}}))
        fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))
        rec = json.loads(fn._clients["s3"].objects[("fair-raw", "climate/sst/_fair/record.json")])
        assert rec["tables_status"] == "loaded" and len(rec["tables"]) == 1 and "tables_attempts" not in rec


def test_a_curator_can_publish_as_files_only(fn, tmp_path):
    """The review's "tables": false publishes the dataset without loading its data into VAST DataBase; the
    files, record, crate and identifier are unchanged by the choice."""
    from fairdemo import tables
    fn.table_sink = tables.LocalTables(tmp_path / "db")
    ctx = types.SimpleNamespace(logger=Logger())
    put_file(fn, "climate/sst/a.nc", fixtures.netcdf, tmp_path)
    fn.handler(ctx, event("fair-raw", "climate/sst/a.nc"))
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_READY", Body=b"")
    fn.handler(ctx, event("fair-raw", "climate/sst/_READY"))
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_fair/review.json",
                                 Body=json.dumps({"reviewer": "ada", "decisions": {}, "tables": False}))
    out = fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))
    assert "pid" in out and out["tables"] == 0
    rec = json.loads(fn._clients["s3"].objects[("fair-raw", "climate/sst/_fair/record.json")])
    assert rec["status"] == "published" and rec["tables_status"] == "none" and rec["tables"] == [] \
        and rec["tables_wanted"] is False
    assert not fn.tables["tables"]
    crate = json.loads(fn._clients["s3"].objects[("fair-raw", "climate/sst/ro-crate-metadata.json")])
    assert not any(str(g["@id"]).startswith("#table-") for g in crate["@graph"])


def _publish_sst(fn, tmp_path):
    ctx = types.SimpleNamespace(logger=Logger())
    put_file(fn, "climate/sst/a.nc", fixtures.netcdf, tmp_path)
    fn.handler(ctx, event("fair-raw", "climate/sst/a.nc"))
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_READY", Body=b"")
    fn.handler(ctx, event("fair-raw", "climate/sst/_READY"))
    fn._clients["s3"].put_object(Bucket="fair-raw", Key="climate/sst/_fair/review.json",
                                 Body=json.dumps({"reviewer": "ada", "decisions": {}}))
    out = fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))
    return out, json.loads(fn._clients["s3"].objects[("fair-raw", "climate/sst/_fair/record.json")])


@pytest.mark.parametrize("failure", ["write", "read", "crash"])
def test_tables_never_stop_a_dataset_publishing(fn, tmp_path, monkeypatch, failure):
    """Whatever goes wrong loading tables (the store refuses a write, a file can't be read, the converter
    blows up), the dataset is published as files, and the record says why there is no table."""
    from fairdemo import tables
    sink = tables.LocalTables(tmp_path / "db")
    if failure == "write":
        monkeypatch.setattr(sink, "replace", lambda name, t: (_ for _ in ()).throw(RuntimeError("type refused")))
    elif failure == "read":
        monkeypatch.setattr(tables, "MAX_CELLS", 10)
    else:
        monkeypatch.setattr(tables, "convert", lambda *a, **k: (_ for _ in ()).throw(MemoryError()))
    fn.table_sink = sink
    out, rec = _publish_sst(fn, tmp_path)
    assert out["pid"].startswith("ark:/99999/fk4") and rec["status"] == "published"
    assert rec["tables"] == [] and rec["tables_status"] == "none" and rec["table_notes"]
    assert {"write": "couldn't be written", "read": "cell budget", "crash": "not loaded as a table"}[failure] in rec["table_notes"][0]
    crate = json.loads(fn._clients["s3"].objects[("fair-raw", "climate/sst/ro-crate-metadata.json")])
    assert not any(str(g["@id"]).startswith("#table-") for g in crate["@graph"])
    assert [r["status"] for r in fn.tables["records"]][-2:] == ["published", "published"]


def test_tables_are_listed_after_publication(fn, tmp_path):
    from fairdemo import tables
    fn.table_sink = tables.LocalTables(tmp_path / "db")
    out, rec = _publish_sst(fn, tmp_path)
    assert out["tables"] == 1 and rec["tables_status"] == "loaded"
    assert fn.tables["records"][-1]["actor"] == "fair-curate/tables"   # not mistaken for the model's draft
    assert fn.tables["records"][-2]["actor"] == "ada"


def test_an_unfinished_table_load_is_retried_by_replaying_the_review(fn, tmp_path, monkeypatch):
    """The function stops part-way through loading tables: the dataset is published, the record says
    loading. The same review, delivered again, loads only the tables."""
    from fairdemo import tables
    fn.table_sink = tables.LocalTables(tmp_path / "db")
    real = fn.publish_tables
    monkeypatch.setattr(fn, "publish_tables", lambda *a: 0)   # stopped before loading
    out, rec = _publish_sst(fn, tmp_path)
    assert rec["status"] == "published" and rec["tables_status"] == "loading" and not rec["tables"]
    monkeypatch.setattr(fn, "publish_tables", real)
    versions = len(fn.tables["records"])
    ctx = types.SimpleNamespace(logger=Logger())
    again = fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))
    rec = json.loads(fn._clients["s3"].objects[("fair-raw", "climate/sst/_fair/record.json")])
    assert again == {"tables_retried": "climate/sst", "tables": 1} and rec["tables_status"] == "loaded"
    assert len(fn.tables["records"]) == versions + 1 and fn.tables["records"][-1]["actor"] == "fair-curate/tables"
    # Once loaded, a further redelivery does nothing.
    assert "skipped" in fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))


def test_a_load_that_keeps_stopping_the_function_is_given_up(fn, tmp_path, monkeypatch):
    """Each redelivery retries the tables, but after two loads that never finished, it stops trying."""
    from fairdemo import tables
    fn.table_sink = tables.LocalTables(tmp_path / "db")

    class Killed(BaseException):   # what the pod's death looks like from inside: nothing after it runs
        pass
    monkeypatch.setattr(fn, "load_tables", lambda *a, **k: (_ for _ in ()).throw(Killed()))
    with pytest.raises(Killed):
        _publish_sst(fn, tmp_path)
    ctx = types.SimpleNamespace(logger=Logger())
    with pytest.raises(Killed):   # first redelivery: second attempt
        fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))
    out = fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))   # third: given up
    rec = json.loads(fn._clients["s3"].objects[("fair-raw", "climate/sst/_fair/record.json")])
    assert out == {"tables_retried": "climate/sst", "tables": 0}
    assert rec["status"] == "published" and rec["tables_status"] == "none" and "stopped the function 2 times" in rec["table_notes"][0]
    assert "skipped" in fn.handler(ctx, event("fair-raw", "climate/sst/_fair/review.json"))
