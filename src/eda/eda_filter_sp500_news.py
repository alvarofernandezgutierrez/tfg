import glob
import pandas as pd
import numpy as np
from pathlib import Path

PARQUET_DIR = Path("outputs/eda/ticker_quality_raw")
SP500_COMPONENTS_PATH = Path("data/raw/market/sp500_historical_components.csv")
OUT_PATH = Path("data/processed/news/fnspid_sp500.parquet")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

def main():
    # ---- 1. Cargar componentes históricos ----
    print("Cargando componentes históricos del S&P 500...")
    components = pd.read_csv(SP500_COMPONENTS_PATH)
    components.columns = ["date", "tickers"]
    components["date"] = pd.to_datetime(components["date"]).dt.normalize()
    components = components.dropna(subset=["date"])

    print("Expandiendo tickers por fecha...")
    rows = []
    for _, row in components.iterrows():
        date = row["date"]
        for ticker in str(row["tickers"]).split(","):
            ticker = ticker.strip().upper()
            if ticker:
                rows.append((date, ticker))

    valid = pd.DataFrame(rows, columns=["date", "ticker"])
    valid_set = set(zip(valid["date"], valid["ticker"]))

    # Mapa: fecha cualquiera → siguiente día de mercado
    market_dates_sorted = pd.Series(sorted(components["date"].unique()))
    print(f"Días de mercado disponibles: {len(market_dates_sorted):,}")
    print(f"Pares (date, ticker) válidos: {len(valid_set):,}")

    def next_market_day(date):
        idx = market_dates_sorted.searchsorted(date)
        if idx >= len(market_dates_sorted):
            return pd.NaT
        return market_dates_sorted.iloc[idx]

    # ---- 2. Cargar noticias ----
    print("\nCargando parquets...")
    files = sorted(glob.glob(str(PARQUET_DIR / "*.parquet")))
    news = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    print(f"Filas antes del filtro: {len(news):,}")

    # Limpiar columnas innecesarias
    cols_to_drop = ["Unnamed: 0", "Lsa_summary", "Luhn_summary",
                    "Textrank_summary", "Lexrank_summary"]
    news = news.drop(columns=cols_to_drop, errors="ignore")

    # Normalizar columnas
    col_mapping = {}
    for col in news.columns:
        c = col.lower()
        if 'date' in c: col_mapping[col] = 'date'
        elif 'symbol' in c or 'ticker' in c: col_mapping[col] = 'ticker'
        elif 'title' in c: col_mapping[col] = 'title'
    news = news.rename(columns=col_mapping)

    news["ticker"] = news["ticker"].astype(str).str.strip().str.upper()
    news["date"] = pd.to_datetime(news["date"], utc=True, errors="coerce").dt.normalize().dt.tz_localize(None)
    news = news.dropna(subset=["date", "ticker"])
    print(f"Filas tras limpiar nulos: {len(news):,}")

    # ---- 3. Mapear al siguiente día de mercado ----
    print("Mapeando fechas al siguiente día de mercado...")
    news["effective_date"] = news["date"].apply(next_market_day)
    news = news.dropna(subset=["effective_date"])
    print(f"Filas tras mapeo: {len(news):,}")

    # Cuántas se desplazaron
    shifted = (news["effective_date"] != news["date"]).sum()
    print(f"Noticias desplazadas al siguiente día de mercado: {shifted:,} ({shifted/len(news):.1%})")

    # ---- 4. Filtrar por (effective_date, ticker) en S&P 500 ----
    print("Filtrando por componentes históricos del S&P 500...")
    mask = [
        (d, t) in valid_set
        for d, t in zip(news["effective_date"], news["ticker"])
    ]
    news_filtered = news[mask].reset_index(drop=True)

    n_discarded = len(news) - len(news_filtered)
    print(f"Filas después del filtro: {len(news_filtered):,}")
    print(f"Filas descartadas: {n_discarded:,} ({n_discarded/len(news):.1%})")
    print(f"Tickers únicos en resultado: {news_filtered['ticker'].nunique():,}")

    # Usar effective_date como fecha definitiva
    news_filtered = news_filtered.drop(columns=["date"])
    news_filtered = news_filtered.rename(columns={"effective_date": "date"})

    # ---- 5. Guardar ----
    news_filtered.to_parquet(OUT_PATH, index=False)
    print(f"\n✅ Guardado en: {OUT_PATH}")
    print(news_filtered[["date", "ticker", "title"]].head(10))

if __name__ == "__main__":
    main()