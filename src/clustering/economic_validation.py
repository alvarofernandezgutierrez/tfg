import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

# ---- DEBUG MODE ----
DEBUG_MODE = False
CLUSTERED_PATH = Path("outputs/clustering_debug/clustered_data.parquet") if DEBUG_MODE else Path("outputs/clustering/clustered_data.parquet")
MARKET_PATH = Path("data/raw/market/sp500_ohlc.csv")
SENTIMENT_PATH = Path("data/processed/news/fnspid_sentiment_debug.parquet") if DEBUG_MODE else Path("data/processed/news/fnspid_sentiment.parquet")
GICS_PATH = Path("data/raw/market/ticker_gics_mapping.csv")
OUT_DIR = Path("outputs/clustering_debug/validation") if DEBUG_MODE else Path("outputs/clustering/validation")
OUT_DIR.mkdir(parents=True, exist_ok=True)

RISK_FREE_RATE = 0.02
TRADING_DAYS = 252

# FIX 1: incluidos los modelos C (kmeans_sent_smooth, hmm_sent_smooth)
CLUSTER_COLS = [
    "kmeans_tech",
    "kmeans_tech_sent",
    "kmeans_sent_smooth",
    "hmm_tech",
    "hmm_tech_sent",
    "hmm_sent_smooth",
]

def sharpe_ratio(returns, rf=RISK_FREE_RATE):
    """Ratio de Sharpe anualizado usando retorno medio diario."""
    daily_rf = rf / TRADING_DAYS
    excess = returns - daily_rf
    if excess.std() == 0:
        return np.nan
    return (excess.mean() / excess.std()) * np.sqrt(TRADING_DAYS)

def validate_clusters(df, cluster_col, returns_col="ret"):
    """Calcula métricas económicas por cluster."""
    results = []
    for cluster in sorted(df[cluster_col].dropna().unique()):
        mask = df[cluster_col] == cluster
        rets = df.loc[mask, returns_col].dropna()
        results.append({
            "cluster": int(cluster),
            "n_dias": len(rets),
            "ret_medio_diario_pct": round(rets.mean() * 100, 4),
            "volatilidad_diaria_pct": round(rets.std() * 100, 4),
            "volatilidad_anualizada": round(rets.std() * np.sqrt(TRADING_DAYS), 4),
            "sharpe": round(sharpe_ratio(rets), 4),
            "pct_dias_positivos": round((rets > 0).mean() * 100, 2),
            # FIX 3: max_drawdown reemplazado por peor retorno diario individual.
            # El drawdown acumulado no tiene sentido económico cuando los días
            # del cluster no son consecutivos en el tiempo.
            "peor_dia_pct": round(rets.min() * 100, 4),
            "mejor_dia_pct": round(rets.max() * 100, 4),
        })
    return pd.DataFrame(results).set_index("cluster")

def main():
    # ---- 1. Cargar datos ----
    print("Cargando datos...")
    df = pd.read_parquet(CLUSTERED_PATH)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()

    mkt = pd.read_csv(MARKET_PATH)
    mkt.columns = [c.lower() for c in mkt.columns]
    mkt["date"] = pd.to_datetime(mkt["date"]).dt.normalize()
    mkt["ret"] = np.log(mkt["close"] / mkt["close"].shift(1))
    mkt = mkt[["date", "ret", "close"]].dropna()

    df = df.merge(mkt[["date", "ret", "close"]], on="date", how="left")
    print(f"Filas: {len(df):,}")

    # ---- 2. Validación económica por modelo ----
    print("\n" + "="*60)
    print("VALIDACIÓN ECONÓMICA DE CLUSTERS")
    print("="*60)

    all_results = {}
    for cluster_col in CLUSTER_COLS:
        if cluster_col not in df.columns:
            print(f"\n⚠️  {cluster_col} no encontrado, saltando...")
            continue

        print(f"\n--- {cluster_col} ---")
        metrics = validate_clusters(df, cluster_col)
        print(metrics)
        all_results[cluster_col] = metrics
        metrics.to_csv(OUT_DIR / f"metrics_{cluster_col}.csv")

        # Gráfico retorno medio diario por cluster
        plt.figure(figsize=(8, 5))
        colors = ["green" if x > 0 else "red" for x in metrics["ret_medio_diario_pct"]]
        plt.bar(metrics.index.astype(str), metrics["ret_medio_diario_pct"], color=colors)
        plt.axhline(0, color="black", linewidth=0.8)
        plt.title(f"Retorno medio diario (%) por cluster — {cluster_col}")
        plt.xlabel("Cluster")
        plt.ylabel("Retorno medio diario (%)")
        plt.tight_layout()
        plt.savefig(OUT_DIR / f"ret_diario_{cluster_col}.png", dpi=200)
        plt.close()

        # Gráfico Sharpe por cluster
        plt.figure(figsize=(8, 5))
        colors_sharpe = ["green" if x > 0 else "red" for x in metrics["sharpe"]]
        plt.bar(metrics.index.astype(str), metrics["sharpe"], color=colors_sharpe)
        plt.axhline(0, color="black", linewidth=0.8)
        plt.title(f"Ratio de Sharpe por cluster — {cluster_col}")
        plt.xlabel("Cluster")
        plt.ylabel("Sharpe ratio")
        plt.tight_layout()
        plt.savefig(OUT_DIR / f"sharpe_{cluster_col}.png", dpi=200)
        plt.close()

    # ---- 3. Comparativa diferenciación de Sharpe entre modelos ----
    print("\n" + "="*60)
    print("COMPARATIVA DIFERENCIACIÓN DE SHARPE ENTRE MODELOS")
    print("="*60)
    for col, metrics in all_results.items():
        print(f"\n{col}:")
        print(f"  Sharpe medio:              {metrics['sharpe'].mean():.4f}")
        print(f"  Sharpe std:                {metrics['sharpe'].std():.4f}")
        print(f"  Diferenciación (max-min):  {metrics['sharpe'].max() - metrics['sharpe'].min():.4f}")

    # ---- 4. Validación por sector ----
    # FIX 2: parametrizado para poder validar por sector en cualquier modelo,
    # no solo kmeans_tech_sent
    print("\n" + "="*60)
    print("VALIDACIÓN POR SECTOR")
    print("="*60)

    sent = pd.read_parquet(SENTIMENT_PATH)
    sent["date"] = pd.to_datetime(sent["date"], errors="coerce").dt.normalize()
    sent["ticker"] = sent["ticker"].astype(str).str.strip().str.upper()

    gics = pd.read_csv(GICS_PATH)
    gics["ticker"] = gics["ticker"].astype(str).str.strip().str.upper()
    sent = sent.merge(gics[["ticker", "sector"]], on="ticker", how="inner")
    sent = sent.merge(mkt[["date", "ret"]], on="date", how="left")

    sector_cluster_cols = [c for c in CLUSTER_COLS if c in df.columns]
    cluster_dates = df[["date"] + sector_cluster_cols].dropna(subset=sector_cluster_cols, how="all")
    sent = sent.merge(cluster_dates, on="date", how="inner")

    for cluster_col in sector_cluster_cols:
        if cluster_col not in sent.columns:
            continue
        print(f"\n--- Sector stats por {cluster_col} ---")
        sector_stats = (
            sent.groupby([cluster_col, "sector"])
            .agg(
                sentiment_avg=("sentiment_score", "mean"),
                n_noticias=("sentiment_score", "count"),
                ret_medio_diario_pct=("ret", lambda x: round(x.mean() * 100, 4))
            )
            .reset_index()
        )
        print(sector_stats.to_string())
        sector_stats.to_csv(OUT_DIR / f"sector_by_{cluster_col}.csv", index=False)

    # ---- 5. Gráfico comparativa Sharpe todos los modelos ----
    if len(all_results) >= 2:
        fig, axes = plt.subplots(1, len(all_results), figsize=(5 * len(all_results), 5))
        if len(all_results) == 1:
            axes = [axes]
        for ax, (col, metrics) in zip(axes, all_results.items()):
            colors = ["green" if x > 0 else "red" for x in metrics["sharpe"]]
            ax.bar(metrics.index.astype(str), metrics["sharpe"], color=colors)
            ax.set_title(f"{col}", fontsize=9)
            ax.set_xlabel("Cluster")
            ax.set_ylabel("Sharpe ratio")
            ax.axhline(0, color="black", linewidth=0.5)
        plt.suptitle("Comparativa Sharpe entre modelos", fontsize=11)
        plt.tight_layout()
        plt.savefig(OUT_DIR / f"sharpe_comparativa.png", dpi=200)
        plt.close()

    print(f"\n✅ Validación guardada en: {OUT_DIR}")

if __name__ == "__main__":
    main()