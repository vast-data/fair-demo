#!/usr/bin/env python3
"""Seed the estate bucket with "existing" research data for the backlog sweep: the demo datasets placed the
way data accumulates on a real system (project shares, scratch, instrument exports), with no _READY, so
nothing curates them until the sweep finds them in VAST Catalog. Uploads through the portal, so it works
for every target.

    python3 scripts/seed_estate.py --portal http://<portal>
"""
import argparse

from upload import bucket_of, upload

ESTATE = {
    "projects/ocean-heat/oisst-2024-09-01": "climate/oisst-v2.1-2024-09-01",
    "scratch/u4471/out_0901": "climate/scratch-run-0901",
    "missions/tess/s0001/tic261136679": "astronomy/tess-s0001-tic261136679",
    "groups/hep/skims/v7": "particle-physics/user-skims-07",
    "instruments/ixm-03/2010-03-05": "bioimaging/plate-br21381-export",
    "shared/gnomad/r2.1.1/chr21": "genomics/gnomad-v2.1.1-exomes-chr21-slice",
    "exports/dft/2025-q3": "public-sector/export-2025-q3",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--portal", default="http://localhost:8080")
    ap.add_argument("--bucket", help="default: the portal's estate bucket (fair-estate, or with its prefix)")
    args = ap.parse_args()
    bucket = args.bucket or bucket_of(args.portal.rstrip("/"), "estate")
    for dest, src in ESTATE.items():
        upload(args.portal.rstrip("/"), src, bucket, dest, ready=False)


if __name__ == "__main__":
    main()
