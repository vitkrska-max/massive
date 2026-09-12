# Massive data

Cíl je vytvořit skrypt na tvorbu a aktualizaci lokálního úložiště dat ze zdroje massive.

Předpokládané spouštění skryptu je jednou za týden.

Konfigurace bude v souboru *config.yaml*.

Přístupové klíče budou v souboru *secrets.yaml*. Tento soubor bude vyloučen z Git.

Skrypt musí být schopen obsloužit všechny chybové stavy. Chyby logovat do souboru.

Opakovat čtení v případě chyby. Počet opakování bude v konfiguračním souboru *retries=3*.

Historická data budeme stahovat pomocí:

1. [REST Api](https://github.com/massive-com/client-python) při použití přístupu zdarma.
2. [Flat files](https://massive.com/docs/flat-files/quickstart) při placeném přístupu.

Při aktualizaci dat si skript z již uložených dat zjistí kde má začít.

Při vytváření úložiště (prvním spuštění) ještě nejsou žádná data, bude v konfiguračním souboru nastaven počet mesíců pro zpětné načtení *history_months=120*. To platí jen pro Flat soubory. Pro REST api bude defautně historie jen jeden měsíc.

Jaká varianta stahování se použije bude v konfiguračním souboru *subscription=False*.

Jestliže nebude použito předplatné *subscription*, jsou API dotazy omezeny na 5 za minutu.

Stahovat budeme jen 1D a 1min data. Flat soubory jiné nedoporují.

U 1min dat se musíme dotazovat zlášť na každý ticker. Seznam tickerů bude dán jménem souboru *tikers_file* v konfiguračním souboru.

U 1min dat při použití REST api je nevýhodné stahovat data po dnech. Nejlépe je stádhnout potřebný počet dní najednou a pak je rozdělit po jednotlivých dnech. Z omezení REST api na 50_000 záznamů je možné stáhnout najednou 1 měsíc denních dat.

## Formát OHLCV souborů *Parquet*

Použít sloupcovou kompresi *zstd*.

Použít *row groups* a před uložením seřadit řádky podle *ticker* a uvnitř podle *timestamp*. Tím by se měly row groups shluknout po tickerech.

Po stažení flat souboru a jeho uložení se převede na parquet.

## Adresářová struktura lokálního úložiště

Cesta k lokálnímu úložišti je v konfiguračním souboru, položka *root_folder*.

market = stocks (, futures, crypto, options).

timeframe = 1D, 1min.

date = yyyy-mm-dd

Adresář pro logy. [root_folder]/logs

### OHLCV data *FlatFiles*

[root_folder]/flat_files/{market}/{timeframe}/{year}/{month}/{date}.csv.gz

### OHLCV data *Parquet*

[root_folder]/{market}/{timeframe}/{year}/{month}/{date}.parquet

Informace o *Tickerech*: [root_folder]/{market}/tickers.sqlite

### Referenční a korporátní data (SQLite)

Implementační prompt pro rozšíření projektu dataStacker o skripty stahující referenční a korporátní data z Massive API do jednoho SQLite souboru.

## Kontext

Projekt potřebuje uložit menší objem referenčních dat o tickerech a korporátních akcích, která se hodí spíš do relační DB než do Parquetu (viz Návrhová rozhodnutí níže).

Zdroj dat: Massive API (dříve Polygon.io), Python klient `massive`.

## Návrhová rozhodnutí a jejich zdůvodnění

- **Jeden sdílený SQLite soubor** (`reference.db`) pro všechny tabulky níže, ne separátní soubory. Objem dat je malý (řádově 10–100 MB), tabulky jsou vzájemně provázané přes `ticker` a je potřeba mezi nimi dělat JOIN (např. dividendy podle sektoru).
- **SQLite místo Parquet** pro tato data, protože:
  - přístupový vzor je point-lookup podle tickeru (backtest se ptá "dej mi splity/dividendy pro AAPL"), ne bulk analytický scan přes celý trh,
  - data se průběžně mění/doplňují (Massive je aktualizuje denně) – SQLite umí upsert, Parquet je prakticky immutable,
  - `ticker_events` má variantní/rozšiřitelné schéma (payload podle typu události), což se do pevného Parquet schématu nehodí dobře.
- **`sic_segments` je oddělená lookup tabulka**, ne sloučená do `tickers` – SIC kód→segment mapování je nezávislé na tickeru, malé (~1000 řádků) a mění se prakticky nikdy.
- **`market_cap_history` je oddělená time-series tabulka**, ne sloupec v `tickers` – `tickers` drží jen aktuální snapshot, zatímco market cap potřebujeme historicky (kvůli look-ahead bias při výběru univerza v backtestech).

## Struktura databáze (DDL)

```sql
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ------------------------------------------------------------
-- 1. tickers – aktuální snapshot metadat, 1 řádek na ticker
-- zdroj: GET /v3/reference/tickers/{ticker}  (Ticker Overview)
-- update: denně, batch přes celé univerzum (~15 000 tickerů)
-- ------------------------------------------------------------
CREATE TABLE tickers (
    ticker                          TEXT PRIMARY KEY,
    name                            TEXT,
    active                          BOOLEAN,
    market                          TEXT,
    locale                          TEXT,
    primary_exchange                TEXT,
    type                            TEXT,
    currency_name                   TEXT,
    cik                             TEXT,
    composite_figi                  TEXT,
    share_class_figi                TEXT,
    sic_code                        TEXT REFERENCES sic_segments(sic_code),
    sic_description                 TEXT,
    market_cap                      REAL,
    share_class_shares_outstanding  REAL,
    weighted_shares_outstanding     REAL,
    total_employees                 INTEGER,
    list_date                       TEXT,
    delisted_utc                    TEXT,
    homepage_url                    TEXT,
    phone_number                    TEXT,
    round_lot                       INTEGER,
    ticker_root                     TEXT,
    ticker_suffix                   TEXT,
    description                     TEXT,
    address_line1                   TEXT,
    address_city                    TEXT,
    address_state                   TEXT,
    address_postal_code             TEXT,
    logo_url                        TEXT,
    icon_url                        TEXT,
    last_updated                    TEXT NOT NULL   -- ISO timestamp posledního stažení
);
CREATE INDEX idx_tickers_sic ON tickers(sic_code);
CREATE INDEX idx_tickers_active ON tickers(active);

-- ------------------------------------------------------------
-- 2. sic_segments – SIC kód -> segment/sektor (malá lookup tabulka)
-- zdroj: SEC (existující mapování z projektu), mění se prakticky nikdy
-- ------------------------------------------------------------
CREATE TABLE sic_segments (
    sic_code    TEXT PRIMARY KEY,
    segment     TEXT NOT NULL
);

-- ------------------------------------------------------------
-- 3. splits – korporátní akce: štěpení akcií
-- zdroj: GET /stocks/v1/splits
-- update: denně
-- ------------------------------------------------------------
CREATE TABLE splits (
    id                              TEXT PRIMARY KEY,   -- id z Massive, přirozený upsert klíč
    ticker                          TEXT NOT NULL REFERENCES tickers(ticker),
    execution_date                  TEXT NOT NULL,       -- 'yyyy-mm-dd'
    adjustment_type                 TEXT,                 -- forward_split / reverse_split / stock_dividend
    split_from                      REAL,
    split_to                        REAL,
    historical_adjustment_factor    REAL
);
CREATE INDEX idx_splits_ticker_date ON splits(ticker, execution_date);

-- ------------------------------------------------------------
-- 4. dividends – korporátní akce: dividendy
-- zdroj: GET /stocks/v1/dividends
-- update: denně
-- ------------------------------------------------------------
CREATE TABLE dividends (
    id                              TEXT PRIMARY KEY,
    ticker                          TEXT NOT NULL REFERENCES tickers(ticker),
    ex_dividend_date                TEXT,
    declaration_date                TEXT,
    record_date                     TEXT,
    pay_date                        TEXT,
    cash_amount                     REAL,
    split_adjusted_cash_amount      REAL,
    currency                        TEXT,
    frequency                       INTEGER,
    distribution_type               TEXT,
    historical_adjustment_factor    REAL
);
CREATE INDEX idx_dividends_ticker_date ON dividends(ticker, ex_dividend_date);

-- ------------------------------------------------------------
-- 5. ticker_events – přejmenování/rebranding tickerů (experimentální endpoint)
-- zdroj: GET /vX/reference/tickers/{id}/events
-- update: denně nebo méně často (řídké události)
-- ------------------------------------------------------------
CREATE TABLE ticker_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker        TEXT NOT NULL,          -- ticker, pod kterým byl dotaz volán
    event_date    TEXT NOT NULL,
    event_type    TEXT NOT NULL,          -- 'ticker_change', případně další typy v budoucnu
    payload       TEXT,                   -- JSON s daty specifickými pro daný typ události
    UNIQUE(ticker, event_date, event_type)
);
CREATE INDEX idx_events_ticker ON ticker_events(ticker, event_date);

-- ------------------------------------------------------------
-- 6. market_cap_history – roční snapshoty tržní kapitalizace
-- zdroj: GET /v3/reference/tickers/{ticker}?date=YYYY-MM-DD  (point-in-time)
--        NEBO dopočet: close_price(date) x shares_outstanding(date) z vlastních OHLCV dat
-- update: ročně, k pevně danému datu (viz Otevřené otázky)
-- ------------------------------------------------------------
CREATE TABLE market_cap_history (
    ticker                          TEXT NOT NULL REFERENCES tickers(ticker),
    as_of_date                      TEXT NOT NULL,   -- konkrétní datum snapshotu
    market_cap                      REAL,
    weighted_shares_outstanding     REAL,
    share_class_shares_outstanding  REAL,
    close_price_used                REAL,             -- vyplnit, pokud se market_cap dopočítává sám
    source                          TEXT,             -- 'api' nebo 'computed' - pro dohledatelnost
    PRIMARY KEY (ticker, as_of_date)
);
CREATE INDEX idx_mc_history_ticker ON market_cap_history(ticker, as_of_date);
```

## Mapování zdroj → tabulka → frekvence

| Tabulka              | Endpoint                                              | Frekvence update | Upsert klíč                        |
|-----------------------|--------------------------------------------------------|-------------------|-------------------------------------|
| `tickers`              | `GET /v3/reference/tickers/{ticker}`                   | denně              | `ticker`                             |
| `sic_segments`         | SEC (stávající logika projektu)                        | jednorázově/zřídka | `sic_code`                           |
| `splits`               | `GET /stocks/v1/splits`                                | denně              | `id`                                 |
| `dividends`            | `GET /stocks/v1/dividends`                             | denně              | `id`                                 |
| `ticker_events`        | `GET /vX/reference/tickers/{id}/events`                | denně              | `(ticker, event_date, event_type)`   |
| `market_cap_history`   | `GET /v3/reference/tickers/{ticker}?date=...` nebo dopočet | ročně          | `(ticker, as_of_date)`               |

## Otevřené otázky k doladění před implementací

1. **Ověřit chování `date` parametru u `market_cap`** – Massive endpoint popisuje pole jako
   "most recent close price × weighted outstanding shares"; není jisté, zda "most recent"
   znamená k zadanému `date`, nebo vždy k dnešku. Otestovat na známém historickém tickeru
   před nasazením na celé univerzum.
2. **Rozhodnout mezi API hodnotou a vlastním dopočtem** market cap (`close_price(date) ×
   shares_outstanding`) – dopočet dá plnou kontrolu nad adjustmentem a nezávislost na
   nejasné API sémantice, ale vyžaduje mít shares outstanding platné k danému datu.
3. **Definovat pevné pravidlo pro `as_of_date`** u ročních snapshotů (např. "poslední
   obchodní den kalendářního roku") a použít ho konzistentně napříč celým univerzem, jinak
   nebudou cross-sectional srovnání mezi tickery korektní.
