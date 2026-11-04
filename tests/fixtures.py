"""Tiny synthetic files of every supported format, written on demand into a temp folder."""
from __future__ import annotations

import gzip
from pathlib import Path

import numpy as np


def netcdf(path: Path, documented: bool = True):
    import netCDF4
    with netCDF4.Dataset(path, "w") as ds:
        ds.createDimension("time", 3); ds.createDimension("lat", 4); ds.createDimension("lon", 5)
        t = ds.createVariable("time", "f8", ("time",)); t.units = "days since 2024-09-01"; t[:] = [0, 1, 2]
        la = ds.createVariable("lat", "f4", ("lat",)); la[:] = [-60, -20, 20, 60]
        lo = ds.createVariable("lon", "f4", ("lon",)); lo[:] = [0, 90, 180, 270, 359]
        v = ds.createVariable("sst" if documented else "v1", "f4", ("time", "lat", "lon"))
        v[:] = np.random.default_rng(0).normal(15, 5, (3, 4, 5))
        if documented:
            ds.Conventions = "CF-1.8, ACDD-1.3"
            ds.title = "Test daily sea surface temperature"
            ds.summary = "Synthetic SST analysis for tests."
            ds.creator_name = "Test Ocean Lab"
            ds.license = "CC-BY-4.0"
            v.units = "degC"; v.standard_name = "sea_surface_temperature"; v.long_name = "Sea surface temperature"


def hdf5(path: Path):
    import h5py
    with h5py.File(path, "w") as f:
        f.attrs["experiment"] = "beamline test"
        d = f.create_dataset("entry/data/counts", data=np.arange(12).reshape(3, 4))
        d.attrs["units"] = "counts"


def fits(path: Path):
    from astropy.io import fits as F
    hdr = F.Header(); hdr["TELESCOP"] = "TESS"; hdr["OBJECT"] = "TIC 1"; hdr["DATE-OBS"] = "2018-07-25"
    cols = F.ColDefs([F.Column(name="TIME", format="D", unit="d", array=np.arange(3.0)),
                      F.Column(name="PDCSAP_FLUX", format="E", unit="e-/s", array=np.ones(3))])
    F.HDUList([F.PrimaryHDU(header=hdr), F.BinTableHDU.from_columns(cols, name="LIGHTCURVE")]).writeto(path)


def root(path: Path):
    import uproot
    with uproot.recreate(path) as f:
        f["Events"] = {"Muon_pt": np.array([20.5, 31.2, 45.0]), "nMuon": np.array([1, 2, 1], dtype=np.int32)}


def vcf(path: Path):
    lines = ["##fileformat=VCFv4.1", "##source=test", "##reference=GRCh37",
             '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">',
             '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
             "##contig=<ID=21,length=48129895>",
             "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2"]
    lines += ["21\t%d\t.\tA\tG\t100\tPASS\tAF=0.1\tGT\t0|1\t0|0" % p for p in (9411239, 9411245, 9411300)]
    with gzip.open(path, "wt") as f:
        f.write("\n".join(lines) + "\n")


def tiff(path: Path):
    import tifffile
    tifffile.imwrite(path, np.zeros((2, 8, 8), dtype=np.uint16), ome=True,
                     metadata={"axes": "CYX", "PhysicalSizeX": 0.65, "Channel": {"Name": ["DAPI", "GFP"]}})


def csv(path: Path):
    path.write_text("date,region,count\n2024-01-01,North,12\n2024-01-02,South,7\n")


def parquet(path: Path):
    import pyarrow as pa, pyarrow.parquet as pq
    pq.write_table(pa.table({"id": [1, 2], "value": [0.5, 1.5]}), path)


ALL = {"t.nc": netcdf, "t.h5": hdf5, "t.fits": fits, "t.root": root, "t.vcf.gz": vcf,
       "t.ome.tif": tiff, "t.csv": csv, "t.parquet": parquet}
