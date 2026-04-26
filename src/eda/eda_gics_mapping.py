import pandas as pd
import yfinance as yf
from pathlib import Path
import time

SP500_GICS_PATH = Path("data/raw/market/sp500_gics.csv")
OUT_PATH = Path("data/raw/market/ticker_gics_mapping.csv")

# Normalización de nombres de sector (Yahoo Finance → estándar GICS)
SECTOR_MAPPING = {
    "Healthcare": "Health Care",
    "Financials": "Financial Services",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Basic Materials": "Materials",
    "Technology": "Information Technology",
}

def normalize_sector(sector):
    return SECTOR_MAPPING.get(sector, sector)

def main():
    # ---- 1. Cargar sector de los que ya están en sp500.csv ----
    print("Cargando sectores conocidos...")
    gics = pd.read_csv(SP500_GICS_PATH)
    gics["Symbol"] = gics["Symbol"].str.strip().str.upper()
    known = gics[["Symbol", "GICS Sector"]].rename(
        columns={"Symbol": "ticker", "GICS Sector": "sector"}
    ).drop_duplicates(subset="ticker")
    known["sector"] = known["sector"].apply(normalize_sector)
    print(f"Tickers con sector conocido: {len(known):,}")

    # ---- 2. Cargar tickers del dataset ----
    news = pd.read_parquet("data/processed/news/fnspid_sp500.parquet")
    all_tickers = set(news["ticker"].str.strip().str.upper().unique())
    missing = all_tickers - set(known["ticker"])
    print(f"Tickers sin sector: {len(missing)}")

    # ---- 3. Buscar en yfinance ----
    print("\nBuscando sectores en Yahoo Finance...")
    extra_rows = []
    for i, ticker in enumerate(sorted(missing)):
        try:
            info = yf.Ticker(ticker).info
            sector = info.get("sector", None)
            if sector:
                sector_normalizado = normalize_sector(sector)
                extra_rows.append({"ticker": ticker, "sector": sector_normalizado})
                print(f"  ✅ {ticker}: {sector} → {sector_normalizado}")
            else:
                print(f"  ❌ {ticker}: no encontrado")
        except Exception as e:
            print(f"  ❌ {ticker}: error — {e}")

        if i % 20 == 0 and i > 0:
            time.sleep(2)

    extra_df = pd.DataFrame(extra_rows) if extra_rows else pd.DataFrame(columns=["ticker", "sector"])
    print(f"\nTickers recuperados via yfinance: {len(extra_df)}")

    # ---- 4. Combinar, normalizar y guardar ----
    final = pd.concat([known, extra_df], ignore_index=True)
    final["sector"] = final["sector"].apply(normalize_sector)
    final = final.drop_duplicates(subset="ticker")

    final.to_csv(OUT_PATH, index=False)
    print(f"\n✅ Mapping guardado en: {OUT_PATH}")
    print(f"Total tickers con sector: {len(final):,}")
    print(f"\nDistribución por sector:")
    print(final["sector"].value_counts())

if __name__ == "__main__":
    main()