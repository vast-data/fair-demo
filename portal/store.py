"""Where the portal reads datasets and writes reviews.

S3Store talks to the cluster: the DataEngine function does the work, the portal only reads each dataset's
_fair/record.json and writes _fair/review.json. LocalStore runs the very same function code in-process,
against a folder on disk that stands in for the buckets, so the demo also works with no cluster at all.
"""
from __future__ import annotations

import importlib
import io
import json
import os
import sys
import threading
import time
import types
from collections import deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = "fair-raw"
ESTATE = os.environ.get("ESTATE_BUCKET", "fair-estate")
# Datasets in the landing bucket are keyed by their prefix; anywhere else by <bucket>/<prefix>
# (function/main.py dataset_id). A key starting with another bucket's name resolves to that bucket.
BUCKETS = (RAW, ESTATE)


class S3Store:
    mode = "cluster"
    db = None  # a vastdb session when the portal runs on the cluster

    def __init__(self, endpoint: str, access: str, secret: str, bucket: str = RAW):
        import boto3
        self.s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=access,
                               aws_secret_access_key=secret, region_name="us-east-1")
        self.bucket = bucket
        self.log = deque(maxlen=200)

    def _split(self, key: str) -> tuple[str, str]:
        for b in BUCKETS[1:]:
            if key.startswith(b + "/"):
                return b, key[len(b) + 1:]
        return self.bucket, key

    def list_records(self) -> list[dict]:
        out = []
        for bucket in BUCKETS:
            try:
                for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                    for o in page.get("Contents", []):
                        if o["Key"].endswith("/_fair/record.json"):
                            prefix = "" if bucket == self.bucket else bucket + "/"
                            out.append(self.get_json(prefix + o["Key"]))
            except Exception:  # an estate bucket that doesn't exist (yet) is simply empty
                if bucket == self.bucket:
                    raise
        return out

    def put_marker(self, bucket: str, key: str):
        self.s3.put_object(Bucket=bucket, Key=key, Body=b"")

    # Chunked uploads (S3 multipart): each part is its own short request, so a large file survives a flaky
    # network one part at a time. Parts are 5 MB or more, except the last.
    def mp_start(self, bucket, key) -> str:
        return self.s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]

    def mp_part(self, bucket, key, upload_id, n: int, body: bytes) -> str:
        return self.s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=n, Body=body)["ETag"]

    def mp_complete(self, bucket, key, upload_id, parts: list[dict]):
        self.s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={
            "Parts": [{"PartNumber": p["n"], "ETag": p["etag"]} for p in sorted(parts, key=lambda p: p["n"])]})

    def catalog_elements(self) -> tuple[list[dict], str]:
        """Every file the tenant can see, from VAST Catalog (a periodic snapshot of the namespace), queried
        through VMS as the tenant's admin, so the answer is scoped to this tenant."""
        import requests
        import urllib3
        urllib3.disable_warnings()
        vms, tenant = os.environ["VMS_URL"].rstrip("/"), os.environ.get("VMS_TENANT", "fair")
        tok = requests.post(f"{vms}/api/token/{tenant}", data={"username": os.environ["VMS_USER"],
                                                              "password": os.environ["VMS_PASSWORD"]},
                            verify=False, timeout=30).json()["access"]
        fields = ["parent_path", "name", "extension", "size", "mtime", "element_type"]
        url, params, out = f"{vms}/api/bigcatalogconfig/query_data/", {
            "path": "/", "fields": ",".join(fields), "filters": "{}", "limit": 5000}, []
        while url:
            r = requests.get(url, headers={"Authorization": "Bearer " + tok}, params=params, verify=False, timeout=120)
            r.raise_for_status()
            d = r.json()
            out += [dict(zip(d["prop_list"], row)) for row in d["results"]]
            url, params = d.get("next"), None
        return out, "VAST Catalog (snapshot of the namespace, refreshed every 15 min)"

    def pending(self) -> list[str]:
        """Folders with data but no record yet (still arriving, or being curated)."""
        folders, done = set(), set()
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket):
            for o in page.get("Contents", []):
                k = o["Key"]
                if k.count("/") >= 2:
                    f = k.split("/_fair/")[0] if "/_fair/" in k else k.rsplit("/", 1)[0]
                    (done if k.endswith("/_fair/record.json") else folders).add(f)
        return sorted(folders - done)

    def get_json(self, key):
        bucket, key = self._split(key)
        return json.loads(self.s3.get_object(Bucket=bucket, Key=key)["Body"].read())

    def get_bytes(self, key) -> bytes | None:
        bucket, key = self._split(key)
        try:
            return self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except Exception:
            return None

    def put(self, key, body: bytes):
        bucket, key = self._split(key)
        self.s3.put_object(Bucket=bucket, Key=key, Body=body)


class LocalS3:
    """Just enough of the boto3 S3 client for function/main.py, over a directory of buckets."""

    def __init__(self, root: Path, on_put=None):
        self.root, self.on_put = root, on_put

    def _p(self, bucket, key):
        return self.root / bucket / key

    exceptions = type("exceptions", (), {"NoSuchKey": FileNotFoundError})

    def put_object(self, Bucket, Key, Body, **_):
        p = self._p(Bucket, Key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(Body if isinstance(Body, bytes) else Body.encode())
        if self.on_put:
            self.on_put(Bucket, Key)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self._p(Bucket, Key).read_bytes())}

    def create_multipart_upload(self, Bucket, Key):
        uid = "local-%d" % time.time_ns()
        (self.root / ".uploads" / uid).mkdir(parents=True)
        return {"UploadId": uid}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        (self.root / ".uploads" / UploadId / ("%05d" % PartNumber)).write_bytes(Body)
        return {"ETag": str(PartNumber)}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        d = self.root / ".uploads" / UploadId
        body = b"".join((d / ("%05d" % p["PartNumber"])).read_bytes() for p in MultipartUpload["Parts"])
        for f in d.iterdir():
            f.unlink()
        d.rmdir()
        self.put_object(Bucket=Bucket, Key=Key, Body=body)

    def download_file(self, bucket, key, dest):
        Path(dest).write_bytes(self._p(bucket, key).read_bytes())

    def get_paginator(self, _):
        s3 = self

        class Pages:
            def paginate(self, Bucket, Prefix=""):
                base = s3.root / Bucket
                yield {"Contents": [{"Key": str(p.relative_to(base)), "Size": p.stat().st_size}
                                    for p in sorted(base.rglob("*")) if p.is_file()
                                    and str(p.relative_to(base)).startswith(Prefix)]}
        return Pages()


class LocalStore(S3Store):
    """The cluster pipeline, in-process: every put fires function/main.py's handler, as the trigger would."""
    mode = "local"

    def __init__(self, root: Path):
        self.root = root
        self.bucket = RAW
        self.log = deque(maxlen=200)
        self.queue: deque = deque()
        self.s3 = LocalS3(root, on_put=self._event)
        self.main = self._load_function()
        threading.Thread(target=self._worker, daemon=True).start()

    def _load_function(self):
        sdk = types.ModuleType("vast.dataengine.sdk")
        sdk.Context = sdk.VastEvent = object
        sys.modules.setdefault("vast", types.ModuleType("vast"))
        sys.modules.setdefault("vast.dataengine", types.ModuleType("vast.dataengine"))
        sys.modules["vast.dataengine.sdk"] = sdk
        sys.path.insert(0, str(ROOT / "function"))
        main = importlib.import_module("main")
        tables_file = self.root / "_tables.jsonl"
        lock = threading.Lock()

        def insert(table, row):
            with lock, open(tables_file, "a") as f:
                f.write(json.dumps({"table": table, **row}) + "\n")

        def select(table, folder=None, sha256=None):
            if not tables_file.exists():
                return []
            rows = (json.loads(line) for line in tables_file.read_text().splitlines())
            key, val = ("folder", folder) if folder is not None else ("sha256", sha256)
            return [{k: v for k, v in r.items() if k != "table"} for r in rows
                    if r["table"] == table and r.get(key) == val]

        main.insert, main.select = insert, select
        main._clients.update(s3=self.s3, db=None)
        from fairdemo import tables
        main.table_sink = tables.LocalTables(self.root / "fair-db" / "data")   # stands in for VAST DataBase
        return main

    def _event(self, bucket, key):
        # As the triggers: everything in the landing bucket; only _READY and reviews in the estate.
        if bucket == RAW or (bucket == ESTATE and (key.endswith("/_READY") or key.endswith("/_fair/review.json"))):
            self.queue.append((bucket, key))

    def _worker(self):
        ctx = types.SimpleNamespace(logger=types.SimpleNamespace(info=self._logline))
        while True:
            if not self.queue:
                time.sleep(0.2)
                continue
            bucket, key = self.queue.popleft()
            ev = types.SimpleNamespace(as_element_event=lambda b=bucket, k=key: types.SimpleNamespace(bucket=b, object_key=k))
            try:
                self.main.handler(ctx, ev)
            except Exception as e:  # a failed event lands in the log, as a dead letter would
                self._logline(f"ERROR {key}: {e}")

    def _logline(self, msg):
        self.log.append({"t": time.strftime("%H:%M:%S"), "msg": msg})

    def catalog_elements(self) -> tuple[list[dict], str]:
        """Local mode has no VAST Catalog: walk the bucket folders instead, live."""
        out = []
        for bucket in BUCKETS:
            base = self.root / bucket
            for f in base.rglob("*") if base.exists() else []:
                if f.is_file():
                    rel = f.relative_to(self.root)
                    out.append({"parent_path": "/%s/" % rel.parent, "name": f.name, "extension": f.suffix.lstrip(".") or None,
                                "size": f.stat().st_size, "mtime": int(f.stat().st_mtime * 1e9), "element_type": "FILE"})
        return out, "folder walk (local mode)"


def from_env() -> S3Store:
    """FAIR_STORE=local (default when no cluster state) or cluster.

    In the portal's pod, S3 and VAST DB are reached directly, with keys from the environment, and search
    uses VAST Vector Search in VAST DataBase. Elsewhere (FAIR_IN_CLUSTER unset) S3 works through FAIR_S3_ENDPOINT, but
    search needs the cluster's data network."""
    state = ROOT / "deploy/.state.json"
    mode = os.environ.get("FAIR_STORE") or ("cluster" if state.exists() else "local")
    if mode == "cluster":
        access, secret = os.environ.get("FAIR_S3_ACCESS_KEY"), os.environ.get("FAIR_S3_SECRET_KEY")
        if not access:
            st = json.loads(state.read_text())
            access, secret = st["access_key"], st["secret_key"]
        endpoint = os.environ.get("FAIR_S3_ENDPOINT")
        if not endpoint:
            raise SystemExit("FAIR_STORE=cluster needs FAIR_S3_ENDPOINT (the cluster's S3 VIP, e.g. http://<vip>)")
        store = S3Store(endpoint, access, secret)
        if os.environ.get("FAIR_IN_CLUSTER"):
            import vastdb
            store.db = vastdb.connect(endpoint=endpoint, access=access, secret=secret)
        return store
    root = Path(os.environ.get("FAIR_LOCAL_ROOT", ROOT / "data/buckets"))
    (root / RAW).mkdir(parents=True, exist_ok=True)
    (root / "fair-docs").mkdir(parents=True, exist_ok=True)
    (root / ESTATE).mkdir(parents=True, exist_ok=True)
    return LocalStore(root)
