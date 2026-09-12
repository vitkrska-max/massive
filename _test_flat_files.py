"""Test update_flat_files s falešným S3 klientem (bez sítě a klíčů).

Spuštění: python _test_flat_files.py
"""
import gzip
import shutil
import sys
import types
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
from dateutil.relativedelta import relativedelta

import massive_data as md

TODAY = date.today()
MISSING_FROM = TODAY - timedelta(days=1)  # poslední 2 dny simulujeme jako nezveřejněné

CSV = (
    "close,high,low,open,ticker,transactions,volume,window_start\n"
    "101.5,102.0,100.0,100.5,MSFT,1000,50000,1679994000000000000\n"
    "201.5,202.0,200.0,200.5,AAPL,2000,60000,1679990400000000000\n"
    "102.5,103.0,101.0,101.5,MSFT,1100,55000,1679990400000000000\n"
    "202.5,203.0,201.0,201.5,AAPL,2100,65000,1679994000000000000\n"
)


class FakeClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self):
        self.downloads = []

    def download_file(self, bucket, key, dest):
        day = date.fromisoformat(key.split("/")[-1].split(".")[0])
        if day >= MISSING_FROM:
            raise FakeClientError("NoSuchKey")
        self.downloads.append(day)
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(dest, "wt") as fh:
            fh.write(CSV)


def install_s3(fake):
    """Podstrčí falešný `boto3` modul — _s3_client je kvůli lazy importu
    zavolaný uvnitř update_flat_files, takže ho nelze monkeypatchovat."""
    mod = types.ModuleType("boto3")
    session = SimpleNamespace(client=lambda service: fake)
    mod.Session = lambda **kwargs: session
    sys.modules["boto3"] = mod


fake = FakeS3()
install_s3(fake)

root = Path("_test_flat_root")
shutil.rmtree(root, ignore_errors=True)

cfg = md.AppConfig(subscription=True, history_months=1,
                   markets=["stocks"], root_folder=root,
                   flatfiles_access_key="test-access",
                   flatfiles_secret_key="test-secret")
md.update_flat_files(cfg, "1D")

csvs = sorted(root.rglob("*.csv.gz"))
parquets = sorted(root.rglob("*.parquet"))
start = TODAY - relativedelta(months=1)
expected = (MISSING_FROM - timedelta(days=1) - start).days + 1
print(f"csv.gz: {len(csvs)} | parquet: {len(parquets)} | staženo: {len(fake.downloads)} | očekáváno: {expected}")
assert len(fake.downloads) == expected
assert len(csvs) == expected and len(parquets) == expected

# 2) parquet: zstd + řádky seřazené dle tickeru, uvnitř dle window_start
# (with — na Windows drží otevřený handle a brání smazání adresáře na konci)
with pq.ParquetFile(parquets[-1]) as pf:
    assert pf.metadata.row_group(0).column(0).compression == "ZSTD"
    t = pf.read()
assert t.column("ticker").to_pylist() == ["AAPL", "AAPL", "MSFT", "MSFT"]
ws = t.column("window_start").to_pylist()
assert ws[:2] == sorted(ws[:2]) and ws[2:] == sorted(ws[2:])
print("zstd + řazení ticker -> window_start: OK")

# 3) idempotence — druhý běh nestáhne nic nového
before = len(fake.downloads)
md.update_flat_files(cfg, "1D")
assert len(fake.downloads) == before
print("idempotence: OK")

# 4) subscription=False → nic se nestáhne (ani bez secret klíčů)
fake2 = FakeS3()
install_s3(fake2)
md.update_flat_files(md.AppConfig(subscription=False, root_folder=root), "1D")
assert not fake2.downloads
print("subscription=False: OK")

# 5) neplatný timeframe
try:
    md.update_flat_files(cfg, "5m")
except SystemExit as e:
    assert e.code == 1
    print("neplatný timeframe: OK")

shutil.rmtree(root, ignore_errors=True)
print("VŠE OK")
