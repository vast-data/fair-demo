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
    tables = {"files": [], "records": []}
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
