import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Paths
NEWS_PATH = Path("data/processed/news/fnspid_sp500.parquet")
MARKET_PATH = Path("data/raw/market/sp500_ohlc.csv")
OUT_DIR = Path("outputs/eda/market_news")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    # --- Noticias ---
    news = pd.read_parquet(NEWS_PATH)
    news["date"] = pd.to_datetime(news["date"]).dt.normalize()
    news_daily = (
        news
        .groupby("date")
        .size()
        .rename("news_count")
        .to_frame()
    )

    # --- Mercado ---
    mkt = pd.read_csv(MARKET_PATH, parse_dates=["date"])
    mkt = mkt.sort_values("date")
    mkt["ret"] = mkt["close"].pct_change()
    mkt["vol_21d"] = mkt["ret"].rolling(21).std()
    mkt = mkt.set_index("date")[["vol_21d"]]

    # --- Merge ---
    df = news_daily.join(mkt, how="inner").dropna()

    # --- Plot ---
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.plot(df.index, df["vol_21d"], color="tab:red", label="Volatilidad SP500 (21d)")
    ax1.set_ylabel("Volatilidad", color="tab:red")
    ax1.tick_params(axis="y", labelcolor="tab:red")

    ax2 = ax1.twinx()
    ax2.plot(df.index, df["news_count"], color="tab:blue", alpha=0.4, label="Nº noticias")
    ax2.set_ylabel("Número de noticias", color="tab:blue")
    ax2.tick_params(axis="y", labelcolor="tab:blue")

    plt.title("Volatilidad del SP500 vs intensidad informativa")
    fig.tight_layout()

    out_path = OUT_DIR / "volatility_vs_news.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

    print("✅ Gráfica guardada en:", out_path)
    print(df.describe())

if __name__ == "__main__":
    main()