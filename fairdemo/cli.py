"""fairdemo: run the curation steps locally, on folders on disk.

  fairdemo profile FILE                          header metadata and checksum of one file
  fairdemo curate FOLDER [-o record.json]        proposed record (add --no-llm for facts only)
  fairdemo review record.json --auto 0.85        accept inferred fields at or above a confidence
  fairdemo score record.json [--published]       FAIR checks
  fairdemo publish record.json [--out DIR]       RO-Crate, Croissant, landing page (default: into the folder)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import curate, extract, fairscore, rocrate


def _load_env():
    env = Path(".env")
    if env.exists():
        import os
        for line in env.read_text().splitlines():
            k, sep, v = line.partition("=")
            if sep and not k.startswith("#"):
                os.environ.setdefault(k.strip(), v.strip())


def _dump(obj, path=None):
    text = json.dumps(obj, indent=2, default=str)
    if path:
        Path(path).write_text(text + "\n")
        print("wrote %s" % path, file=sys.stderr)
    else:
        print(text)


def _print_score(s, label=""):
    print("%sFAIR %d  (F %d · A %d · I %d · R %d)" % (label, s["total"], *[s["by_principle"][p] for p in "FAIR"]))
    for c in s["checks"]:
        print("  %s %-15s %s" % ("✓" if c["pass"] else "·", c["id"], c["label"]))


def main(argv=None):
    _load_env()
    ap = argparse.ArgumentParser(prog="fairdemo")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("profile"); p.add_argument("file")
    p = sub.add_parser("curate"); p.add_argument("folder"); p.add_argument("-o", "--out")
    p.add_argument("--no-llm", action="store_true"); p.add_argument("--model")
    p = sub.add_parser("review"); p.add_argument("record"); p.add_argument("--auto", type=float, required=True)
    p = sub.add_parser("score"); p.add_argument("record"); p.add_argument("--published", action="store_true")
    p.add_argument("--json", action="store_true")
    p = sub.add_parser("publish"); p.add_argument("record"); p.add_argument("--out")
    p.add_argument("--base-url", default="http://localhost:8080/id")
    p.add_argument("--s3-prefix", default=None)
    args = ap.parse_args(argv)

    if args.cmd == "profile":
        _dump(extract.profile(args.file))
    elif args.cmd == "curate":
        rec = curate.curate(args.folder, use_llm=not args.no_llm, model=args.model)
        rec["score_before"] = fairscore.score(curate.curate(args.folder, use_llm=False))["total"]
        _dump(rec, args.out)
        if rec["llm_error"]:
            print("warning: no model proposals: %s" % rec["llm_error"], file=sys.stderr)
    elif args.cmd == "review":
        rec = json.loads(Path(args.record).read_text())
        decisions = {k: {"action": "accept"} for k, f in rec["fields"].items()
                     if f["status"] == "proposed" and f["confidence"] >= args.auto}
        for v in rec["variables"]:
            d = v.get("description")
            if d and d["status"] == "proposed" and d["confidence"] >= args.auto:
                d["status"] = "accepted"
        rec = curate.apply_review(rec, decisions)
        _dump(rec, args.record)
        left = [k for k, f in rec["fields"].items() if f["status"] in ("proposed", "missing")]
        print("accepted %d; still needs a person: %s" % (len(decisions), ", ".join(left) or "nothing"), file=sys.stderr)
    elif args.cmd == "score":
        rec = json.loads(Path(args.record).read_text())
        s = fairscore.score(rec, published=args.published or bool(rec.get("pid")))
        _dump(s) if args.json else _print_score(s)
    elif args.cmd == "publish":
        rec = json.loads(Path(args.record).read_text())
        folder = Path(args.out or rec["folder"])
        folder.mkdir(parents=True, exist_ok=True)
        s3 = args.s3_prefix or "s3://fair-curated/%s" % rec["name"]
        try:
            docs = rocrate.build(rec, args.base_url, s3)
        except rocrate.NotReady as e:
            raise SystemExit("%s: %s" % (rec["name"], e))
        for name in ("ro-crate-metadata.json", "croissant.json"):
            (folder / name).write_text(json.dumps(docs[name], indent=2, default=str) + "\n")
        (folder / "ro-crate-preview.html").write_text(docs["ro-crate-preview.html"])
        # indexed stays False here: only the deployed publish function sets it, once the catalog row is written.
        rec.update(pid=docs["pid"], landing_page=docs["landing_page"], access_url=s3)
        rec["score_after"] = fairscore.score(rec, published=True)["total"]
        _dump(rec, args.record)
        print("published %s -> %s (FAIR %s -> %s)" % (rec["name"], docs["pid"], rec.get("score_before"), rec["score_after"]),
              file=sys.stderr)


if __name__ == "__main__":
    main()
