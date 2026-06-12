import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")

CLUSTERED_PATH = Path("outputs/clustering/clustered_data.parquet")
OUT_DIR = Path("outputs/clustering/validation")
OUT_DIR.mkdir(parents=True, exist_ok=True)

VARIANCE_THRESHOLD = 0.80  # Guillermo dijo quedarnos con componentes que expliquen ~80%

FEATURES_TECH = [
    "RSI_sp500", "MA50_ratio_sp500", "MA200_ratio_sp500",
    "mom21_sp500", "mom126_sp500",
    "vol30d_sp500", "vol252d_sp500",
    "skew30d_sp500", "skew252d_sp500"
]

CLUSTER_COLS = {
    "kmeans_tech":       "Modelo A (solo técnicos)",
    "kmeans_tech_sent":  "Modelo B (técnicos + sentimiento)",
    "kmeans_sent_smooth": "Modelo C (solo sentimiento)",
}


def get_smooth_cols(df):
    return [c for c in df.columns if c.startswith("sentiment_avg_") and c.endswith("_smooth")]


def main():
    print("Cargando clustered_data...")
    df = pd.read_parquet(CLUSTERED_PATH)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    print(f"Filas: {len(df):,} | Rango: {df['date'].min().date()} → {df['date'].max().date()}")

    smooth_cols = get_smooth_cols(df)
    ALL_FEATURES = FEATURES_TECH + smooth_cols
    print(f"Features totales para PCA: {len(ALL_FEATURES)} ({len(FEATURES_TECH)} técnicos + {len(smooth_cols)} sentimiento suavizado)")

    # ---- 1. PCA sobre TODAS las features (espacio común) ----
    data = df[ALL_FEATURES].dropna()
    idx  = data.index

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(data.values)

    pca_full = PCA(random_state=42)
    pca_full.fit(X_scaled)
    cum_var = np.cumsum(pca_full.explained_variance_ratio_)

    # Número de componentes para llegar al 80%
    n_components = int(np.argmax(cum_var >= VARIANCE_THRESHOLD)) + 1
    var_reached   = cum_var[n_components - 1]
    print(f"\nComponentes para {VARIANCE_THRESHOLD:.0%} varianza: {n_components} (varianza real: {var_reached:.1%})")

    pca = PCA(n_components=n_components, random_state=42)
    X_pca = pca.fit_transform(X_scaled)
    print(f"Espacio PCA reducido: {X_pca.shape}")

    # ---- 2. Silhouette de cada modelo en el espacio PCA común ----
    print("\n" + "="*60)
    print(f"SILHOUETTE EN ESPACIO PCA COMÚN ({n_components} componentes, {var_reached:.1%} varianza)")
    print("="*60)

    results = {}
    for col, name in CLUSTER_COLS.items():
        if col not in df.columns:
            print(f"\n⚠️  {col} no encontrado, saltando...")
            continue

        labels = df.loc[idx, col].dropna()
        common_idx = labels.index.intersection(pd.Index(idx))
        labels_aligned = labels.loc[common_idx].values
        X_aligned = X_pca[pd.Index(idx).get_indexer(common_idx)]

        if len(set(labels_aligned)) < 2:
            print(f"\n⚠️  {col} — menos de 2 clusters, saltando...")
            continue

        sil = silhouette_score(X_aligned, labels_aligned)
        results[col] = {"name": name, "silhouette_pca": sil}
        print(f"\n{name}")
        print(f"  Silhouette en espacio PCA: {sil:.4f}")

    # ---- 3. Tabla comparativa ----
    print("\n" + "="*60)
    print("COMPARATIVA FINAL — SILHOUETTE EN ESPACIO PCA COMÚN")
    print("="*60)
    print(f"{'Modelo':<45} {'Silhouette PCA':>15}")
    print("-"*62)
    for col, res in results.items():
        print(f"{res['name']:<45} {res['silhouette_pca']:>15.4f}")

    # ---- 4. Gráfico comparativo ----
    names  = [res["name"].replace(" (", "\n(") for res in results.values()]
    scores = [res["silhouette_pca"] for res in results.values()]
    colors = ["#2980B9", "#E67E22", "#27AE60"]

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(names, scores, color=colors[:len(names)], alpha=0.85, edgecolor="white")
    ax.bar_label(bars, fmt="%.4f", padding=3, fontsize=10, fontweight="bold")
    ax.set_ylabel("Silhouette score", fontsize=11)
    ax.set_title(
        f"Silhouette en espacio PCA común\n"
        f"({n_components} componentes — {var_reached:.1%} varianza explicada)",
        fontsize=12, fontweight="bold"
    )
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_ylim(min(0, min(scores)) - 0.05, max(scores) + 0.08)
    ax.grid(True, alpha=0.2, axis="y")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "silhouette_pca_comparison.png", dpi=200, bbox_inches="tight")
    plt.close()
    print(f"\n✅ Gráfico guardado en: {OUT_DIR / 'silhouette_pca_comparison.png'}")

    # ---- 5. Scree plot del PCA común ----
    fig, ax = plt.subplots(figsize=(10, 4))
    x_vals = np.arange(1, len(cum_var) + 1)
    ax.bar(x_vals, pca_full.explained_variance_ratio_ * 100,
           color="#2980B9", alpha=0.6, label="Varianza por componente")
    ax.plot(x_vals, cum_var * 100, color="#C0392B", marker="o",
            markersize=3, linewidth=1.5, label="Varianza acumulada")
    ax.axhline(80, color="gray", linestyle="--", linewidth=0.8, label="80% umbral")
    ax.axvline(n_components, color="#E67E22", linestyle="--", linewidth=1.2,
               label=f"PC{n_components} ({var_reached:.1%})")
    ax.set_xlabel("Número de componentes", fontsize=10)
    ax.set_ylabel("Varianza explicada (%)", fontsize=10)
    ax.set_title("Scree plot — Espacio PCA común (técnicos + sentimiento)", fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
    ax.set_xlim(0.5, min(len(cum_var) + 0.5, 25))
    ax.grid(True, alpha=0.2)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "silhouette_pca_scree.png", dpi=200, bbox_inches="tight")
    plt.close()
    print(f"✅ Scree plot guardado en: {OUT_DIR / 'silhouette_pca_scree.png'}")


if __name__ == "__main__":
    main()