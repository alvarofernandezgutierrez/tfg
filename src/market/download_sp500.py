import time
import yfinance as yf
import pandas as pd
from pathlib import Path

OUT_DIR = Path("data/raw/market")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def download_with_retries(ticker: str, start: str, retries: int = 3):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(
                ticker,
                start=start,
                progress=True,
                auto_adjust=False,
                threads=False,
            )
            if df is not None and len(df) > 0:
                return df
            else:
                last_err = "Empty dataframe returned"
        except Exception as e:
            last_err = e

        wait = 2 * attempt
        print(f"⚠️ Intento {attempt}/{retries} falló ({last_err}). Reintentando en {wait}s...")
        time.sleep(wait)

    raise RuntimeError(f"No se pudo descargar {ticker}. Último error: {last_err}")

def main():
    sp500 = download_with_retries("^GSPC", start="2000-01-01", retries=3)

    # Si viene con MultiIndex: aplanar
    if isinstance(sp500.columns, pd.MultiIndex):
        sp500.columns = sp500.columns.get_level_values(0)

    sp500 = sp500.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })

    sp500 = sp500.reset_index().rename(columns={"Date": "date"})
    sp500 = sp500[["date", "open", "high", "low", "close", "volume"]]

    out_path = OUT_DIR / "sp500_ohlc.csv"
    sp500.to_csv(out_path, index=False)

    print("\n✅ Guardado CSV limpio en:", out_path)
    print("\nPrimeras filas:")
    print(sp500.head())

if __name__ == "__main__":
    main()

