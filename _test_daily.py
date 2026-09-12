"""Test update_daily s falešným REST klientem (bez sítě a API klíče).

Spuštění: python _test_daily.py
"""
import shutil
import sys
import time
import types
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq

import massive_data as md

TODAY = date.today()
END = TODAY - timedelta(days=1)               # update_daily končí včerejškem
MONDAY = END - timedelta(days=END.weekday())
HOLIDAY = MONDAY + timedelta(days=1)          # simulovaný svátek (prázdná odpověď)
START = TODAY - md.relativedelta(months=23)   # _REST_HISTORY_MONTHS

ROOTS = [Path("_test_daily_root_a"), Path("_test_daily_root_b")]
ROOT = ROOTS[0]

md._REST_DELAY_SECONDS = 0
sleeps = []
time.sleep = lambda s: sleeps.append(s)       # test nesmí reálně čekat


def fake_bars(day):
    """Dva tickery, záměrně v opačném pořadí než abecedně."""
    base = (day - date(1970, 1, 1)).days * 86_400_000
    return [
        SimpleNamespace(ticker="MSFT", timestamp=base, open=2.0, high=2.1,
                        low=1.9, close=2.05, volume=200, transactions=20, vwap=2.0),
        SimpleNamespace(ticker="AAPL", timestamp=base, open=1.0, high=1.1,
                        low=0.9, close=1.05, volume=100, transactions=10, vwap=1.0),
    ]


class FakeClient:
    def __init__(self, fail_days=(), error="429 Too Many Requests"):
        self.fail_days = set(fail_days)
        self.error = error
        self.attempts = []

    def get_grouped_daily_aggs(self, **kwargs):
        # parametry bereme přes **kwargs, aby klíč `date` nezastínil třídu date
        day = date.fromisoformat(kwargs["date"])
        self.attempts.append(day)
        if day in self.fail_days:
            raise RuntimeError(self.error)
        if day == HOLIDAY:
            return []          # svátek — prázdná odpověď, není to chyba
        return fake_bars(day)


def install_client(client):
    """Podstrčí falešný `massive` modul místo skutečné knihovny."""
    mod = types.ModuleType("massive")
    mod.RESTClient = lambda key: client
    sys.modules["massive"] = mod


def cfg_for(root):
    return md.AppConfig(subscription=False, markets=["stocks"], root_folder=root,
                        api_key="test-key", retries=3)


def parquets(root):
    return sorted((root / "market=stocks" / "timeframe=1D").rglob("*.parquet"))


for r in ROOTS:
    shutil.rmtree(r, ignore_errors=True)

# --- Test 1: první naplnění — od START, bez víkendů a svátků ---------------
cfg = cfg_for(ROOT)
client = FakeClient()
install_client(client)
md.update_daily(cfg)

weekdays = [
    d for d in (START + timedelta(days=i) for i in range((END - START).days + 1))
    if d.weekday() < 5
]
expected = [d for d in weekdays if d != HOLIDAY]
files = parquets(ROOT)
print(f"dotazy: {len(client.attempts)} | parquet: {len(files)} | očekáváno: {len(expected)}")
assert client.attempts == weekdays, "dotazy neodpovídají pracovním dnům"
assert len(files) == len(expected), "svátek se nemá ukládat"
assert not any(d.weekday() >= 5 for d in client.attempts), "dotaz na víkend"
print("první naplnění + přeskočení víkendů/svátku: OK")

# --- Test 2: hive cesta, zstd, řazení dle tickeru -------------------------
dest = files[-1]
rel = dest.relative_to(ROOT)
assert rel.parts == ("market=stocks", "timeframe=1D", f"year={dest.stem[:4]}",
                     f"month={dest.stem[5:7]}", f"{dest.stem}.parquet"), rel.parts
with pq.ParquetFile(dest) as pf:
    assert pf.metadata.row_group(0).column(0).compression == "ZSTD"
    t = pf.read()
# vstup fake_bars je [MSFT, AAPL] — výstup musí být seřazený podle tickeru
assert t.column("ticker").to_pylist() == ["AAPL", "MSFT"], t.column("ticker").to_pylist()
ws = t.column("window_start").to_pylist()
# v jednom dni mají všechny řádky stejný window_start, takže sekundární
# klíč není v datech pozorovatelný — kontrolujeme aspoň jeho přítomnost
assert len(ws) == 2 and ws[0] == ws[1]
assert set(t.column_names) == {"ticker", "window_start", "open", "high", "low",
                               "close", "volume", "transactions", "vwap"}
print(f"hive cesta + zstd + řazení: OK ({dest.name})")

# --- Test 3: idempotence — druhý běh nestahuje znovu ----------------------
before = len(client.attempts)
md.update_daily(cfg)
new = client.attempts[before:]
# případně se dotáhne jen poslední (sváteční) den, který se neukládá
assert len(new) <= 1, f"druhý běh stahoval znovu: {len(new)} dotazů"
print("idempotence: OK")

# --- Test 4: subscription=True → REST se nepoužije ------------------------
client2 = FakeClient()
install_client(client2)
md.update_daily(md.AppConfig(subscription=True, markets=["stocks"], root_folder=ROOT))
assert not client2.attempts, "subscription=True nesmí volat REST"
print("subscription=True: OK")

# --- Test 5: selhání po cfg.retries → konec, žádné díry v datech ----------
# Vlastní adresář: Windows drží otevřené parquet handly, takže nelze
# spolehlivě mazat pod běžícím testem.
ROOT_B = ROOTS[1]
cfg_b = cfg_for(ROOT_B)
fail_day = END - timedelta(days=5)
while fail_day.weekday() >= 5:
    fail_day -= timedelta(days=1)

sleeps.clear()
client3 = FakeClient(fail_days={fail_day})
install_client(client3)
md.update_daily(cfg_b)

names = [p.stem for p in parquets(ROOT_B)]
assert str(fail_day) not in names, "selhaný den se uložil"
later = [n for n in names if n > str(fail_day)]
assert not later, f"po selhání se uložily další dny: {later}"
assert client3.attempts.count(fail_day) == cfg_b.retries, client3.attempts.count(fail_day)
assert 60.0 in sleeps, f"u 429 se má čekat 60s, čekalo se {sorted(set(sleeps))}"
print(f"selhání po {cfg_b.retries} pokusech (429 -> 60s), zbytek nedotažen: OK")

for r in ROOTS:
    shutil.rmtree(r, ignore_errors=True)
print("VŠE OK")
