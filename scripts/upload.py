#!/usr/bin/env python3
"""Upload dataset folders through the portal, as a researcher would. Works for every target (local or
cluster) from anywhere that can reach the portal: files go up in 5 MB parts (S3 multipart through
the portal), each part retried on its own, and a _READY marker then starts curation.

    python3 scripts/upload.py --portal http://<portal>                  # every folder in data/staged
    python3 scripts/upload.py --portal http://<portal> climate/scratch-run-0901
    python3 scripts/upload.py --portal http://<portal> --bucket fair-estate --to scratch/u4471/out_0901 \\
        --no-ready climate/scratch-run-0901                              # existing data, for the backlog sweep
"""
import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

STAGED = Path(__file__).resolve().parents[1] / "data/staged"


def call(url, body=None, method=None, raw=None, tries=5):
    for attempt in range(tries):
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        req = urllib.request.Request(url, data, {"Content-Type": "application/octet-stream" if raw is not None
                                                 else "application/json", "Accept": "*/*"}, method=method)
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code < 500 or attempt == tries - 1:
                raise SystemExit(f"{method or 'POST'} {url}: {e.code} {e.read()[:300]!r}")
        except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
            if attempt == tries - 1:
                raise SystemExit(f"{url}: {e}")
        time.sleep(2 * (attempt + 1))


def upload_file(portal, bucket, key, path: Path):
    start = call(f"{portal}/api/upload/start", {"bucket": bucket, "key": key})
    size, parts = start["part_size"], []
    with open(path, "rb") as f:
        n = 1
        while chunk := f.read(size):
            q = urllib.parse.urlencode({"bucket": bucket, "key": key, "upload_id": start["upload_id"], "n": n})
            parts.append(call(f"{portal}/api/upload/part?{q}", raw=chunk, method="PUT"))
            n += 1
    if not parts:  # an empty file is one empty part
        q = urllib.parse.urlencode({"bucket": bucket, "key": key, "upload_id": start["upload_id"], "n": 1})
        parts.append(call(f"{portal}/api/upload/part?{q}", raw=b"", method="PUT"))
    call(f"{portal}/api/upload/complete", {"bucket": bucket, "key": key, "upload_id": start["upload_id"], "parts": parts})


def bucket_of(portal, which="raw"):
    """The portal's landing (raw) or estate bucket, as it reports them: a deployment can prefix bucket names."""
    return call(f"{portal}/api/info", method="GET").get(f"{which}_bucket") or f"fair-{which}"


def upload(portal, folder, bucket=None, to=None, ready=True):
    bucket = bucket or bucket_of(portal)
    dest = (to or folder).strip("/")
    files = sorted(p for p in (STAGED / folder).iterdir() if p.is_file())
    for p in files:
        upload_file(portal, bucket, f"{dest}/{p.name}", p)
    if ready:
        call(f"{portal}/api/upload/ready", {"bucket": bucket, "folder": dest})
    print(f"s3://{bucket}/{dest}/  {len(files)} files{'' if ready else '  (no _READY)'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("folders", nargs="*", help="folders under data/staged (default: all)")
    ap.add_argument("--portal", default="http://localhost:8080")
    ap.add_argument("--bucket", help="default: the portal's landing bucket (fair-raw, or with its prefix)")
    ap.add_argument("--to", help="destination prefix in the bucket (one folder only)")
    ap.add_argument("--no-ready", action="store_true", help="leave it for the backlog sweep")
    args = ap.parse_args()
    folders = args.folders or sorted(str(p.relative_to(STAGED)) for p in STAGED.glob("*/*") if p.is_dir())
    if args.to and len(folders) != 1:
        raise SystemExit("--to takes exactly one folder")
    for f in folders:
        upload(args.portal.rstrip("/"), f, args.bucket, args.to, not args.no_ready)


if __name__ == "__main__":
    main()
