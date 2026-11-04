"""Profile one file: what it is, what its header says, and a checksum.

Every extractor returns the same shape, so curation never needs to know the format:

  {"path", "name", "size", "sha256", "format", "media_type", "conventions",
   "attributes": {...},              # global/header key-values, as found
   "variables": [{"name", "units", "standard_name", "long_name", "shape", "dtype"}],
   "coverage": {"time": [start, end], "lat": [min, max], "lon": [min, max]},  # when the file says
   "notes": [...],                   # anything the extractor could not read
  }

Nothing here guesses: values come only from the file itself.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import itertools
import re
import warnings
from pathlib import Path
from typing import Any, Callable

warnings.filterwarnings("ignore")

MAX_ATTR_LEN = 2000
MAX_VARIABLES = 200


def sha256(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


def _s(value: Any) -> str:
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    text = str(value)
    return text if len(text) <= MAX_ATTR_LEN else text[:MAX_ATTR_LEN] + "…"


def _base(path: Path, fmt: str, media_type: str) -> dict:
    return {
        "path": str(path),
        "name": path.name,
        "size": path.stat().st_size,
        "sha256": sha256(path),
        "format": fmt,
        "media_type": media_type,
        "conventions": None,
        "attributes": {},
        "variables": [],
        "coverage": {},
        "notes": [],
    }


# ---------------------------------------------------------------- NetCDF / HDF5

def _netcdf(path: Path) -> dict:
    import netCDF4
    import numpy as np

    out = _base(path, "NetCDF", "application/x-netcdf")
    with netCDF4.Dataset(path, "r") as ds:
        out["attributes"] = {k: _s(ds.getncattr(k)) for k in ds.ncattrs()}
        out["conventions"] = out["attributes"].get("Conventions")
        for name, var in list(ds.variables.items())[:MAX_VARIABLES]:
            attrs = {k: var.getncattr(k) for k in var.ncattrs()}
            out["variables"].append({
                "name": name,
                "units": _s(attrs["units"]) if "units" in attrs else None,
                "standard_name": _s(attrs["standard_name"]) if "standard_name" in attrs else None,
                "long_name": _s(attrs["long_name"]) if "long_name" in attrs else None,
                "shape": list(var.shape),
                "dtype": str(var.dtype),
            })
        cov = out["coverage"]
        for axis, names in (("lat", ("lat", "latitude")), ("lon", ("lon", "longitude"))):
            for n in names:
                if n in ds.variables and ds.variables[n].size:
                    v = ds.variables[n][:]
                    cov[axis] = [float(np.min(v)), float(np.max(v))]
                    break
        if "time" in ds.variables and ds.variables["time"].size:
            t = ds.variables["time"]
            try:
                cal = getattr(t, "calendar", "standard")
                ends = netCDF4.num2date([t[0], t[-1]], t.units, cal)
                cov["time"] = [ends[0].isoformat(), ends[1].isoformat()]
            except Exception as e:  # unusual calendars or missing units
                out["notes"].append("time axis not decoded: %s" % e)
    a = out["attributes"]
    if "time" not in cov and a.get("time_coverage_start"):
        cov["time"] = [a["time_coverage_start"], a.get("time_coverage_end")]
    return out


def _hdf5(path: Path) -> dict:
    import h5py

    out = _base(path, "HDF5", "application/x-hdf5")
    with h5py.File(path, "r") as f:
        out["attributes"] = {k: _s(v) for k, v in f.attrs.items()}

        def visit(name, obj):
            if len(out["variables"]) >= MAX_VARIABLES:
                return
            if isinstance(obj, h5py.Dataset):
                at = dict(obj.attrs)
                out["variables"].append({
                    "name": name,
                    "units": _s(at["units"]) if "units" in at else None,
                    "standard_name": _s(at["standard_name"]) if "standard_name" in at else None,
                    "long_name": _s(at.get("long_name") or at.get("description")) if ("long_name" in at or "description" in at) else None,
                    "shape": list(obj.shape),
                    "dtype": str(obj.dtype),
                })

        f.visititems(visit)
    return out


# ---------------------------------------------------------------- FITS

def _fits(path: Path) -> dict:
    from astropy.io import fits

    out = _base(path, "FITS", "application/fits")
    out["conventions"] = "FITS"
    skip = re.compile(r"^(COMMENT|HISTORY|NAXIS\d*|BITPIX|SIMPLE|EXTEND|PCOUNT|GCOUNT|TFORM\d+|TDIM\d+|TDISP\d+|TNULL\d+|XTENSION|CHECKSUM|DATASUM)$")
    with fits.open(path, memmap=True) as hdul:
        for i, hdu in enumerate(hdul):
            prefix = "" if i == 0 else "%s." % (hdu.name or "HDU%d" % i)
            for k, v in hdu.header.items():
                if k and not skip.match(k) and len(out["attributes"]) < 400:
                    out["attributes"][prefix + k] = _s(v)
            cols = getattr(hdu, "columns", None)
            if cols is not None:
                for c in cols[:MAX_VARIABLES]:
                    out["variables"].append({
                        "name": "%s%s" % (prefix, c.name), "units": c.unit or None,
                        "standard_name": None, "long_name": None, "shape": None, "dtype": c.format,
                    })
            elif hdu.data is not None and getattr(hdu.data, "shape", None):
                out["variables"].append({
                    "name": prefix + "image" if prefix else "PRIMARY.image",
                    "units": hdu.header.get("BUNIT"), "standard_name": None, "long_name": None,
                    "shape": list(hdu.data.shape), "dtype": str(hdu.data.dtype),
                })
    a = out["attributes"]
    if a.get("RA_OBJ") or a.get("RA_TARG"):
        out["coverage"]["sky"] = {"ra": a.get("RA_OBJ") or a.get("RA_TARG"), "dec": a.get("DEC_OBJ") or a.get("DEC_TARG")}
    start = a.get("DATE-OBS") or a.get("TSTART")
    if start:
        out["coverage"]["time"] = [start, a.get("DATE-END") or a.get("TSTOP")]
    return out


# ---------------------------------------------------------------- ROOT

def _root(path: Path) -> dict:
    import uproot

    out = _base(path, "ROOT", "application/x-root")
    out["conventions"] = "ROOT"
    with uproot.open(path) as f:
        for key, cls in f.classnames().items():
            out["attributes"]["object:" + key] = cls
            if "TTree" in cls or "RNTuple" in cls:
                tree = f[key]
                out["attributes"]["entries:" + key] = str(getattr(tree, "num_entries", ""))
                try:
                    out["attributes"]["title:" + key] = _s(tree.title)
                except AttributeError:
                    pass
                for b in list(tree.keys())[:MAX_VARIABLES - len(out["variables"])]:
                    br = tree[b]
                    out["variables"].append({
                        "name": "%s/%s" % (key.split(";")[0], b), "units": None, "standard_name": None,
                        "long_name": _s(getattr(br, "title", "")) or None, "shape": None,
                        "dtype": _s(getattr(br, "typename", "")),
                    })
    return out


# ---------------------------------------------------------------- VCF

def _vcf(path: Path) -> dict:
    out = _base(path, "VCF", "text/x-vcf")
    with open(path, "rb") as f:
        gz = f.read(2) == b"\x1f\x8b"  # .gz and bgzip (.bgz) both
    opener = gzip.open if gz else open
    samples, records, chroms, positions = 0, 0, set(), []
    with opener(path, "rt", errors="replace") as f:
        for line in f:
            if line.startswith("##"):
                m = re.match(r"##(\w+)=<ID=([^,>]+)(.*)>", line.strip())
                if m:
                    kind, ident, rest = m.groups()
                    if kind in ("INFO", "FORMAT"):
                        desc = re.search(r'Description="([^"]*)"', rest)
                        out["variables"].append({
                            "name": "%s/%s" % (kind, ident), "units": None, "standard_name": None,
                            "long_name": desc.group(1) if desc else None, "shape": None,
                            "dtype": (re.search(r"Type=(\w+)", rest) or [None, None])[1],
                        })
                    elif kind == "contig" and len(out["attributes"]) < 200:
                        out["attributes"]["contig:" + ident] = rest.strip(",")
                else:
                    k, _, v = line[2:].strip().partition("=")
                    out["attributes"].setdefault(k, _s(v))
            elif line.startswith("#CHROM"):
                samples = max(0, len(line.rstrip("\n").split("\t")) - 9)
            elif line.strip():
                records += 1
                c, p = line.split("\t", 2)[:2]
                chroms.add(c)
                if records == 1 or records % 1000 == 0:
                    positions.append(int(p))
                last = int(p)
    out["conventions"] = out["attributes"].get("fileformat")
    out["attributes"]["samples"] = str(samples)
    out["attributes"]["records"] = str(records)
    if records:
        out["coverage"]["genomic"] = {"chromosomes": sorted(chroms), "start": positions[0], "end": last}
    return out


# ---------------------------------------------------------------- TIFF / OME-TIFF

def _tiff(path: Path) -> dict:
    import tifffile

    out = _base(path, "TIFF", "image/tiff")
    with tifffile.TiffFile(path) as tif:
        if tif.is_ome:
            out["format"] = "OME-TIFF"
            out["conventions"] = "OME"
            ome = tif.ome_metadata or ""
            out["attributes"]["ome_xml"] = _s(ome)
            for k in ("PhysicalSizeX", "PhysicalSizeY", "PhysicalSizeZ", "SizeC", "SizeT", "SizeZ", "DimensionOrder"):
                m = re.search(r'%s="([^"]+)"' % k, ome)
                if m:
                    out["attributes"][k] = m.group(1)
            for i, m in enumerate(re.finditer(r'<Channel [^>]*?Name="([^"]+)"', ome)):
                out["attributes"]["channel:%d" % i] = m.group(1)
        page = tif.pages[0]
        for tag in page.tags.values():
            if tag.name in ("ImageDescription", "Software", "DateTime", "Artist", "Make", "Model",
                            "XResolution", "YResolution", "ResolutionUnit"):
                out["attributes"].setdefault(tag.name, _s(tag.value))
        for i, s in enumerate(tif.series[:20]):
            out["variables"].append({
                "name": s.name or "series%d" % i, "units": None, "standard_name": None,
                "long_name": "axes " + s.axes, "shape": list(s.shape), "dtype": str(s.dtype),
            })
    return out


# ---------------------------------------------------------------- tabular

def _csv(path: Path) -> dict:
    out = _base(path, "CSV", "text/csv")
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
        sample = f.read(64 * 1024)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            dialect = csv.excel
        reader = csv.reader(f, dialect)
        header = next(reader, [])
        rows = [r for _, r in zip(range(2000), reader)]
        # Time columns get their full range in one pass, so coverage is a fact rather than a guess.
        time_cols = [i for i, c in enumerate(header) if re.fullmatch(r"(year|date|time|period|month)(_.*)?", c.strip().lower())]
        ranges = {i: [None, None] for i in time_cols}
        total = 0
        for r in itertools.chain(rows, reader):
            total += 1
            for i in time_cols:
                v = r[i].strip() if i < len(r) else ""
                if v:  # ISO dates and years order correctly as strings
                    lo, hi = ranges[i]
                    ranges[i] = [min(lo or v, v), max(hi or v, v)]
    out["attributes"]["rows"] = str(total)
    out["attributes"]["delimiter"] = dialect.delimiter
    for i, (lo, hi) in ranges.items():
        if lo is not None:
            out["coverage"]["time"] = [lo, hi]
            out["attributes"]["range:" + header[i].strip()] = "%s to %s" % (lo, hi)
            break
    for i, col in enumerate(header[:MAX_VARIABLES]):
        vals = [r[i] for r in rows if i < len(r) and r[i] != ""]
        out["variables"].append({
            "name": col.strip(), "units": None, "standard_name": None, "long_name": None,
            "shape": [total], "dtype": _infer_type(vals),
            "examples": list(dict.fromkeys(vals))[:5],
        })
    return out


def _infer_type(vals: list[str]) -> str:
    def all_match(rx):
        return vals and all(re.fullmatch(rx, v.strip()) for v in vals[:500])
    if all_match(r"[-+]?\d+"):
        return "integer"
    if all_match(r"[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?"):
        return "number"
    if all_match(r"\d{4}-\d{2}-\d{2}([T ][\d:.]+Z?)?"):
        return "date"
    return "string"


def _parquet(path: Path) -> dict:
    import pyarrow.parquet as pq

    out = _base(path, "Parquet", "application/vnd.apache.parquet")
    pf = pq.ParquetFile(path)
    out["attributes"]["rows"] = str(pf.metadata.num_rows)
    for k, v in (pf.schema_arrow.metadata or {}).items():
        out["attributes"][_s(k)] = _s(v)
    for field in pf.schema_arrow:
        out["variables"].append({"name": field.name, "units": None, "standard_name": None,
                                 "long_name": None, "shape": [pf.metadata.num_rows], "dtype": str(field.type)})
    return out


# ---------------------------------------------------------------- dispatch

MAGIC: list[tuple[bytes, Callable[[Path], dict]]] = [
    (b"CDF\x01", _netcdf), (b"CDF\x02", _netcdf),
    (b"\x89HDF\r\n\x1a\n", None),  # HDF5 or NetCDF-4: decided below
    (b"SIMPLE  =", _fits),
    (b"root", _root),
    (b"II*\x00", _tiff), (b"MM\x00*", _tiff), (b"II+\x00", _tiff), (b"MM\x00+", _tiff),
    (b"PAR1", _parquet),
    (b"##fileformat=VCF", _vcf),
]

SUFFIX = {
    ".nc": _netcdf, ".nc4": _netcdf, ".cdf": _netcdf,
    ".h5": _hdf5, ".hdf5": _hdf5, ".he5": _hdf5,
    ".fits": _fits, ".fit": _fits, ".fts": _fits,
    ".root": _root, ".vcf": _vcf,
    ".tif": _tiff, ".tiff": _tiff,
    ".csv": _csv, ".tsv": _csv, ".parquet": _parquet,
}


def _sniff(path: Path) -> Callable[[Path], dict] | None:
    with open(path, "rb") as f:
        head = f.read(32)
    if head[:2] == b"\x1f\x8b":  # gzip: look inside (vcf.gz, fits.gz)
        with gzip.open(path, "rb") as g:
            inner = g.read(32)
        if inner.startswith(b"##fileformat=VCF"):
            return _vcf
        return None
    for magic, fn in MAGIC:
        if head.startswith(magic):
            if magic.startswith(b"\x89HDF"):
                return _netcdf if path.suffix.lower() in (".nc", ".nc4") else _hdf5
            return fn
    return SUFFIX.get(path.suffix.lower())


def is_data_file(path: Path) -> bool:
    return _sniff(path) is not None


def profile(path: str | Path) -> dict:
    """Profile one file. Unknown formats still get size and checksum."""
    path = Path(path)
    fn = _sniff(path)
    if fn is None:
        out = _base(path, "unknown", "application/octet-stream")
        out["notes"].append("format not recognised")
        return out
    try:
        return fn(path)
    except Exception as e:
        out = _base(path, fn.__name__.strip("_").upper(), "application/octet-stream")
        out["notes"].append("could not read %s header: %s" % (out["format"], e))
        return out


def summary_text(p: dict, max_vars: int = 40) -> str:
    """A compact, factual description of a profile, for the LLM prompt."""
    lines = ["file %s: %s, %d bytes%s" % (p["name"], p["format"], p["size"],
                                          ", conventions %s" % p["conventions"] if p.get("conventions") else "")]
    for k, v in list(p["attributes"].items())[:60]:
        if k != "ome_xml":
            lines.append("  attr %s = %s" % (k, v[:300]))
    for v in p["variables"][:max_vars]:
        bits = [v["name"]]
        for key in ("long_name", "standard_name", "units", "dtype"):
            if v.get(key):
                bits.append("%s=%s" % (key, v[key]))
        if v.get("shape"):
            bits.append("shape=%s" % v["shape"])
        if v.get("examples"):
            bits.append("e.g. %s" % ", ".join(v["examples"][:3]))
        lines.append("  var " + "; ".join(bits))
    if len(p["variables"]) > max_vars:
        lines.append("  … %d more variables" % (len(p["variables"]) - max_vars))
    if p["coverage"]:
        lines.append("  coverage %s" % p["coverage"])
    return "\n".join(lines)
