"""The datasets of "Site B (demo)", a second, fictional university repository for the catalog-of-catalogs demo.

Site B is another copy of this portal (deploy/de.py up site_b). Its datasets are subsets derived from the open
data in data/raw (see data/SOURCES.md), so nothing new is downloaded, and each one says what it was derived
from and credits the original. Site B publishes them through its own pipeline and review (portal/seed.py),
and this site harvests them over OAI-PMH.

    .venv/bin/python scripts/site_b_data.py [OUT]      # default deploy/build/site-b

Each dataset folder holds its data file, a README.md, and seed-review.json: the decisions Site B's curator
makes on review (the seed applies them as a person's edits).
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw"
CURATOR = "Site B data curator (demo)"


def write(folder: Path, readme: str, review: dict):
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "README.md").write_text(readme.strip() + "\n")
    (folder / "seed-review.json").write_text(json.dumps({"reviewer": CURATOR, "edits": review}, indent=1) + "\n")


def north_atlantic_sst(out: Path):
    import netCDF4
    src = netCDF4.Dataset(RAW / "climate/oisst-avhrr-v02r01.20240901.nc")
    lat, lon = src["lat"][:], src["lon"][:]
    li, lj = np.where((lat >= 0) & (lat <= 70))[0], np.where(lon >= 280)[0]   # 0-70°N, 80°W-0°
    folder = out / "ocean/north-atlantic-sst-2024-09-01"
    folder.mkdir(parents=True, exist_ok=True)
    with netCDF4.Dataset(folder / "north_atlantic_sst_20240901.nc", "w") as dst:
        for k in src.ncattrs():
            if k not in ("id", "history", "geospatial_lat_min", "geospatial_lat_max", "geospatial_lon_min", "geospatial_lon_max"):
                dst.setncattr(k, src.getncattr(k))
        dst.title = "North Atlantic sea surface temperature, 1 September 2024 (regional subset of NOAA OISST v2.1)"
        dst.summary = ("Daily sea surface temperature, anomaly, error and sea-ice concentration for the North Atlantic "
                       "(0-70°N, 80°W-0°) on 1 September 2024, on the 0.25-degree grid of NOAA's Optimum Interpolation "
                       "SST v2.1. A regional subset of the global analysis; values are unchanged.")
        dst.history = "Subset to 0-70N, 280-360E from oisst-avhrr-v02r01.20240901.nc (NOAA NCEI)."
        dst.geospatial_lat_min, dst.geospatial_lat_max, dst.geospatial_lon_min, dst.geospatial_lon_max = 0.0, 70.0, 280.0, 360.0
        sizes = {"time": None, "zlev": 1, "lat": len(li), "lon": len(lj)}
        for d in ("time", "zlev", "lat", "lon"):
            dst.createDimension(d, sizes[d])
        for name, v in src.variables.items():
            fill = v.getncattr("_FillValue") if "_FillValue" in v.ncattrs() else None
            nv = dst.createVariable(name, v.datatype, v.dimensions, fill_value=fill, zlib=True)
            nv.setncatts({k: v.getncattr(k) for k in v.ncattrs() if k != "_FillValue"})
            data = v[:]
            if name == "lat":
                data = data[li]
            elif name == "lon":
                data = data[lj]
            elif v.dimensions[-2:] == ("lat", "lon"):
                data = data[..., li, :][..., lj]
            nv[:] = data
    write(folder, """
# North Atlantic sea surface temperature, 1 September 2024

A regional subset (0-70°N, 80°W-0°) of NOAA's 0.25-degree Daily Optimum Interpolation Sea Surface
Temperature (OISST) v2.1 for 1 September 2024: sea surface temperature, its anomaly, estimated error and
sea-ice concentration. Values are unchanged from the global analysis.

Derived from: Huang, B. et al. (2020), NOAA 0.25-degree Daily Optimum Interpolation Sea Surface Temperature
(OISST), Version 2.1. NOAA National Centers for Environmental Information. https://doi.org/10.25921/RE9P-PT57
""", {"title": "North Atlantic sea surface temperature, 1 September 2024 (regional subset of NOAA OISST v2.1)",
      "description": ("Daily sea surface temperature, anomaly, estimated error and sea-ice concentration for the North "
                      "Atlantic (0-70°N, 80°W-0°) on 1 September 2024, on the 0.25-degree grid of NOAA's Optimum "
                      "Interpolation SST v2.1. A regional subset of the global analysis, with values unchanged."),
      "keywords": ["sea surface temperature", "North Atlantic", "ocean", "OISST", "satellite", "climate"],
      "domain": "climate & earth science",
      "creators": [{"name": "NOAA National Centers for Environmental Information"}],
      "license": "CC0-1.0", "citation": "10.25921/RE9P-PT57"})


def cardiff_traffic(out: Path):
    folder = out / "transport/cardiff-traffic-counts-2000-2025"
    folder.mkdir(parents=True, exist_ok=True)
    with open(RAW / "public-sector/dft_aadf_region_id_4.csv", newline="", encoding="utf-8") as f, \
            open(folder / "cardiff_aadf_2000_2025.csv", "w", newline="", encoding="utf-8") as g:
        r = csv.DictReader(f)
        w = csv.DictWriter(g, fieldnames=r.fieldnames)
        w.writeheader()
        for row in r:
            if row["local_authority_name"] == "Cardiff":
                w.writerow(row)
    write(folder, """
# Cardiff road traffic counts, 2000-2025

Annual average daily flow (AADF) by count point for Cardiff, by vehicle class, 2000-2025: the Cardiff rows of
the Department for Transport's road traffic statistics for Wales.

Contains public sector information licensed under the Open Government Licence v3.0. Source: Department for
Transport, Road traffic statistics (roadtraffic.dft.gov.uk).
""", {"title": "Cardiff road traffic counts: annual average daily flow by count point, 2000-2025",
      "description": ("Annual average daily flow (AADF) of traffic at each count point in Cardiff, by vehicle class "
                      "(pedal cycles, motorcycles, cars and taxis, buses, light vans and heavy goods vehicles), "
                      "2000-2025. The Cardiff rows of the Department for Transport's road traffic statistics for Wales."),
      "keywords": ["road traffic", "traffic counts", "Cardiff", "transport", "AADF", "vehicles"],
      "domain": "public sector & social statistics",
      "creators": [{"name": "Department for Transport"}], "license": "OGL-UK-3.0"})


def quiet_light_curve(out: Path):
    from astropy.io import fits
    t = fits.open(RAW / "astronomy/tess2018206045859-s0001-0000000261136679-0120-s_lc.fits")[1].data
    good = t[t["QUALITY"] == 0]
    folder = out / "astronomy/tic261136679-sector1-clean-light-curve"
    folder.mkdir(parents=True, exist_ok=True)
    cols = ["TIME", "SAP_FLUX", "SAP_FLUX_ERR", "PDCSAP_FLUX", "PDCSAP_FLUX_ERR"]
    with open(folder / "tic261136679_s0001_quality0.csv", "w", newline="") as g:
        w = csv.writer(g)
        w.writerow([c.lower() for c in cols])
        for row in good:
            if np.isfinite(row["PDCSAP_FLUX"]):
                w.writerow([repr(float(row[c])) for c in cols])
    write(folder, """
# TIC 261136679, TESS sector 1: light curve without flagged cadences

The 2-minute light curve of TIC 261136679 (the bright star pi Mensae) from TESS sector 1 (July-August 2018),
keeping only the cadences the SPOC pipeline did not flag (QUALITY = 0): time (BTJD), simple aperture and
PDC-corrected flux in electrons per second, with their errors.

Derived from the TESS SPOC light curve tess2018206045859-s0001-0000000261136679-0120-s_lc.fits. This includes
data collected with the TESS mission, obtained from the MAST data archive at the Space Telescope Science
Institute (STScI). https://doi.org/10.17909/t9-nmc8-f686
""", {"title": "TIC 261136679 (pi Mensae), TESS sector 1: light curve without flagged cadences",
      "description": ("The 2-minute-cadence light curve of the bright star TIC 261136679 (pi Mensae) from TESS sector 1, "
                      "July-August 2018, keeping only unflagged cadences (QUALITY = 0): time and simple-aperture and "
                      "PDC-corrected flux with errors, ready for transit and variability analysis."),
      "keywords": ["TESS", "light curve", "photometry", "exoplanet transits", "stellar variability", "pi Mensae"],
      "domain": "astronomy & astrophysics",
      "creators": [{"name": "TESS Science Processing Operations Center"}], "license": "CC0-1.0",
      "citation": "10.17909/t9-nmc8-f686"})


def main():
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "deploy/build/site-b"
    if not (RAW / "climate").exists():
        sys.exit("data/raw is missing: run scripts/fetch_data.py first")
    for make in (north_atlantic_sst, cardiff_traffic, quiet_light_curve):
        make(out)
    for f in sorted(out.rglob("*")):
        if f.is_file():
            print(f.relative_to(out), f.stat().st_size)


if __name__ == "__main__":
    main()
