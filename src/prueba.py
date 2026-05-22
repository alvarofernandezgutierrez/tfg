import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

DEBUG_MODE = False
CLUSTERED_PATH = Path("outputs/clustering_debug/clustered_data.parquet") if DEBUG_MODE else Path("outputs/clustering/clustered_data.parquet")
MARKET_PATH = Path("data/raw/market/sp500_ohlc.csv")
OUT_DIR = Path("outputs/clustering_debug/validation") if DEBUG_MODE else Path("outputs/clustering/validation")
OUT_DIR.mkdir(parents=True, exist_ok=True)

CLUSTER_COL = "kmeans_tech_sent"

CLUSTER_LABELS = {
    0: "Bull market",
    1: "Bear market",
    2: "Crisis / Recuperación"
}

CLUSTER_COLORS = {
    0: "#0D7A3E",  # verde
    1: "#C0392B",  # rojo
    2: "#E67E22",  # naranja
}

def main():
    # ---- Cargar datos ----
    print("Cargando datos...")
    df = pd.read_parquet(CLUSTERED_PATH)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()

    mkt = pd.read_csv(MARKET_PATH)
    mkt.columns = [c.lower() for c in mkt.columns]
    mkt["date"] = pd.to_datetime(mkt["date"]).dt.normalize()
    mkt["ret"] = np.log(mkt["close"] / mkt["close"].shift(1))
    mkt = mkt[["date", "ret"]].dropna()

    df = df.merge(mkt, on="date", how="left").dropna(subset=["ret", CLUSTER_COL])
    df = df.sort_values("date").reset_index(drop=True)

    # ---- Calcular retorno acumulado por cluster ----
    fig, ax = plt.subplots(figsize=(11, 5.5))

    for cluster in sorted(df[CLUSTER_COL].unique()):
        mask = df[CLUSTER_COL] == cluster
        rets = df.loc[mask, "ret"].values
        cum_ret = (np.cumprod(1 + rets) - 1) * 100
        # Normalizar eje X a 0-100% del cluster
        x = np.linspace(0, 100, len(cum_ret))

        ax.plot(
            x, cum_ret,
            label=f"Cluster {int(cluster)} — {CLUSTER_LABELS.get(int(cluster), 'N/A')} ({len(rets)} días)",
            color=CLUSTER_COLORS.get(int(cluster), "gray"),
            linewidth=2.5
        )

    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_xlabel("% de días del cluster transcurridos", fontsize=11, color="#64748B")
    ax.set_ylabel("Retorno acumulado (%)", fontsize=11, color="#64748B")
    ax.set_title(
        f"Retorno acumulado por cluster — {CLUSTER_COL}",
        fontsize=13, fontweight="bold", color="#1F4E79", pad=15
    )
    ax.set_xlim(0, 100)

    ax.legend(loc="upper left", fontsize=10, frameon=True, framealpha=0.95)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(colors="#64748B")

    plt.tight_layout()
    out_path = OUT_DIR / f"cumret_{CLUSTER_COL}.png"
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()

    print(f"✅ Gráfico guardado en: {out_path}")

    # ---- Resumen ----
    print("\nResumen retorno acumulado final por cluster:")
    for cluster in sorted(df[CLUSTER_COL].unique()):
        mask = df[CLUSTER_COL] == cluster
        rets = df.loc[mask, "ret"].values
        final_ret = (np.prod(1 + rets) - 1) * 100
        print(f"  Cluster {int(cluster)} ({CLUSTER_LABELS.get(int(cluster))}): {final_ret:+.1f}%  ({len(rets)} días)")

if __name__ == "__main__":
    main()