#!/usr/bin/env python3
"""Download the demo datasets into data/raw/ (from the providers listed in data/sources.json), check them,
then stage the demo folders (scripts/stage_demo.py). About 150 MB of downloads; every licence allows
commercial use (see data/SOURCES.md for each one's licence and required attribution).

    python3 scripts/fetch_data.py

Whole files are checked against the sha256 in data/sources.json. Two are derived, as recorded there:
  - BBBC039: three images taken from the collection's images.zip (sha256 checked)
  - gnomAD chr21: the first 10 MiB of the 684 MB file, cut to the full header plus the first 3,323 records
    and re-compressed with gzip (record count checked; its sha256 differs from the original bgzip copy)
The files are data only: nothing downloaded is ever executed.
"""
import hashlib
import io
import json
import subprocess
import sys
import urllib.request
import zipfile
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw"
UA = {"User-Agent": "fair-demo-fetch/1.0 (research data demo)"}


def get(url, byte_range=None) -> bytes:
    headers = dict(UA)
    if byte_range:
        headers["Range"] = "bytes=%d-%d" % byte_range
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=600) as r:
        return r.read()


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def gnomad_slice(url, records=3323) -> bytes:
    data = get(url, (0, 10 * 1024 * 1024 - 1))
    out = []
    while data:  # BGZF is a series of gzip members; the last one is cut off by the range
        d = zlib.decompressobj(31)
        try:
            out.append(d.decompress(data))
        except zlib.error:
            break
        if not d.eof:
            break
        data = d.unused_data
    lines = b"".join(out).split(b"\n")
    header = [l for l in lines if l.startswith(b"#")]
    body = [l for l in lines if l and not l.startswith(b"#")][:records]
    if len(body) != records:
        raise SystemExit(f"gnomAD slice: expected {records} records, got {len(body)}")
    buf = io.BytesIO()
    import gzip
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as g:
        g.write(b"\n".join(header + body) + b"\n")
    return buf.getvalue()


def main():
    sources = json.loads((ROOT / "data/sources.json").read_text())["files"]
    zips = {}
    for s in sources:
        dest = RAW / s["domain"] / s["filename"]
        if dest.exists() and (s["domain"] == "genomics" or sha(dest.read_bytes()) == s["sha256"]):
            print(f"have    {dest.relative_to(ROOT)}")
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        url = s["source_url"].split()[0]
        if s["domain"] == "genomics":
            data = gnomad_slice(url)
        elif url.endswith(".zip"):
            if url not in zips:
                print(f"fetch   {url}")
                zips[url] = zipfile.ZipFile(io.BytesIO(get(url)))
            z = zips[url]
            member = next(n for n in z.namelist() if n.endswith("/" + s["filename"]) or n == s["filename"])
            data = z.read(member)
        else:
            print(f"fetch   {url}")
            data = get(url)
        if s["domain"] != "genomics" and sha(data) != s["sha256"]:
            raise SystemExit(f"{s['filename']}: sha256 {sha(data)} != {s['sha256']} (the provider's file changed?)")
        dest.write_bytes(data)
        print(f"wrote   {dest.relative_to(ROOT)}  {len(data):,} bytes")
    subprocess.run([sys.executable, str(ROOT / "scripts/stage_demo.py")], check=True)


if __name__ == "__main__":
    main()
