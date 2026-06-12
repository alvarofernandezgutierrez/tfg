import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.dates as mdates
import warnings
warnings.filterwarnings("ignore")

DEBUG_MODE = False

CLUSTERED_PATH = Path("outputs/clustering_debug/clustered_data.parquet") if DEBUG_MODE else Path("outputs/clustering/clustered_data.parquet")
TECH_PATH = Path("data/processed/market/technical_indicators.parquet")
OUT_DIR = Path("outputs/clustering/validation")
OUT_DIR.mkdir(parents=True, exist_ok=True)

CLUSTER_COLORS = {0: "#0D7A3E", 1: "#C0392B", 2: "#E67E22"}
CLUSTER_NAMES  = {0: "Bull", 1: "Bear", 2: "Crisis/Volátil"}

FEATURES_TECH = [
    "RSI_sp500", "MA50_ratio_sp500", "MA200_ratio_sp500",
    "mom21_sp500", "mom126_sp500",
    "vol30d_sp500", "vol252d_sp500",
    "skew30d_sp500", "skew252d_sp500"
]

def get_model_cols(df):
    return [c for c in df.columns if c.startswith("kmeans_") or c.startswith("hmm_")]

def get_smooth_cols(df):
    return [c for c in df.columns if c.startswith("sentiment_avg_") and c.endswith("_smooth")]

def get_tech_features(df):
    return [c for c in FEATURES_TECH if c in df.columns]

# ---------------------------------------------------------------
# 1. PCA 2D
# ---------------------------------------------------------------
def plot_pca_clusters(df, feature_cols, label_col, out_path, title_suffix=""):
    valid = df[feature_cols + [label_col]].dropna()
    X = valid[feature_cols].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # PCA completo para scree plot
    pca_full = PCA(random_state=42)
    pca_full.fit(X_scaled)
    cum_var = np.cumsum(pca_full.explained_variance_ratio_)

    # PCA 2D para scatter y loadings
    pca = PCA(n_components=2, random_state=42)
    X_pca = pca.fit_transform(X_scaled)
    var = pca.explained_variance_ratio_

    labels = valid[label_col].values
    unique_labels = sorted(set(labels))

    fig, axes = plt.subplots(1, 3, figsize=(20, 5))

    # --- Panel izquierdo: scatter PCA ---
    ax = axes[0]
    for lbl in unique_labels:
        mask = labels == lbl
        ax.scatter(
            X_pca[mask, 0], X_pca[mask, 1],
            c=CLUSTER_COLORS.get(int(lbl), "#999999"),
            label=f"Cluster {int(lbl)} — {CLUSTER_NAMES.get(int(lbl), '')}",
            alpha=0.45, s=12, edgecolors="none"
        )
    ax.set_xlabel(f"PC1 ({var[0]:.1%} var. explicada)", fontsize=10)
    ax.set_ylabel(f"PC2 ({var[1]:.1%} var. explicada)", fontsize=10)
    ax.set_title(f"PCA 2D — {label_col}{title_suffix}", fontsize=11, fontweight="bold")
    ax.legend(fontsize=9, framealpha=0.8)
    ax.grid(True, alpha=0.2)

    # --- Panel central: loadings ---
    ax2 = axes[1]
    loadings = pca.components_.T  # shape: (n_features, 2)
    short = [c.replace("sentiment_avg_", "avg_").replace("sentiment_std_", "std_")
               .replace("_sp500", "").replace("_smooth", "_sm") for c in feature_cols]
    y_pos = np.arange(len(feature_cols))

    ax2.barh(y_pos - 0.18, loadings[:, 0], height=0.35, label="PC1", color="#2980B9", alpha=0.8)
    ax2.barh(y_pos + 0.18, loadings[:, 1], height=0.35, label="PC2", color="#E67E22", alpha=0.8)
    ax2.set_yticks(y_pos)
    ax2.set_yticklabels(short, fontsize=7)
    ax2.axvline(0, color="black", linewidth=0.8)
    ax2.set_xlabel("Loading", fontsize=10)
    ax2.set_title("Loadings de PC1 y PC2", fontsize=11, fontweight="bold")
    ax2.legend(fontsize=9)
    ax2.grid(True, alpha=0.2, axis="x")

    # --- Panel derecho: scree plot (varianza acumulada) ---
    ax3 = axes[2]
    n_components = len(cum_var)
    x_vals = np.arange(1, n_components + 1)
    ax3.bar(x_vals, pca_full.explained_variance_ratio_ * 100,
            color="#2980B9", alpha=0.6, label="Varianza por componente")
    ax3.plot(x_vals, cum_var * 100, color="#C0392B", marker="o",
             markersize=4, linewidth=1.5, label="Varianza acumulada")
    ax3.axhline(80, color="gray", linestyle="--", linewidth=0.8, label="80% umbral")
    ax3.axhline(95, color="gray", linestyle=":", linewidth=0.8, label="95% umbral")

    # Marcar cuántos componentes para llegar al 80% y 95%
    n80 = int(np.argmax(cum_var >= 0.80)) + 1
    n95 = int(np.argmax(cum_var >= 0.95)) + 1
    ax3.axvline(n80, color="#E67E22", linestyle="--", linewidth=0.8, alpha=0.8)
    ax3.axvline(n95, color="#8E44AD", linestyle="--", linewidth=0.8, alpha=0.8)
    ax3.text(n80 + 0.1, 5, f"PC{n80}\n(80%)", fontsize=7, color="#E67E22")
    ax3.text(n95 + 0.1, 5, f"PC{n95}\n(95%)", fontsize=7, color="#8E44AD")

    ax3.set_xlabel("Número de componentes", fontsize=10)
    ax3.set_ylabel("Varianza explicada (%)", fontsize=10)
    ax3.set_title("Scree plot — Varianza acumulada", fontsize=11, fontweight="bold")
    ax3.legend(fontsize=8, framealpha=0.8)
    ax3.set_xlim(0.5, min(n_components + 0.5, 20))  # máximo 20 componentes en pantalla
    ax3.grid(True, alpha=0.2)

    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"  Varianza explicada: PC1={var[0]:.1%}, PC2={var[1]:.1%}, Total={sum(var):.1%}")
    print(f"  Componentes para 80% varianza: {n80} | Para 95%: {n95}")


# ---------------------------------------------------------------
# 2. S&P 500 con regímenes
# ---------------------------------------------------------------
def plot_sp500_regimes(df_clustered, df_tech, label_col, out_path):
    df_tech = df_tech.copy()
    df_tech["date"] = pd.to_datetime(df_tech["date"]).dt.normalize()
    merged = (
        df_clustered[["date", label_col]]
        .merge(df_tech[["date", "close"]], on="date", how="inner")
        .dropna(subset=[label_col])
        .sort_values("date")
        .reset_index(drop=True)
    )

    labels = merged[label_col].values
    dates  = merged["date"].values
    close  = merged["close"].values

    fig, ax = plt.subplots(figsize=(14, 5))

    i = 0
    while i < len(labels):
        lbl = int(labels[i])
        j = i + 1
        while j < len(labels) and labels[j] == lbl:
            j += 1
        ax.axvspan(
            dates[i], dates[min(j, len(dates) - 1)],
            alpha=0.22, color=CLUSTER_COLORS.get(lbl, "#999999"), zorder=1
        )
        i = j

    ax.plot(merged["date"], close, color="#1a1a2e", linewidth=0.9, zorder=2)

    patches = [
        mpatches.Patch(color=CLUSTER_COLORS[k], alpha=0.7,
                       label=f"Cluster {k} — {CLUSTER_NAMES[k]}")
        for k in sorted(CLUSTER_COLORS.keys())
    ]
    ax.legend(handles=patches, fontsize=9, loc="upper left", framealpha=0.85)
    ax.set_title(f"S&P 500 con regímenes de mercado ({label_col})", fontsize=12, fontweight="bold")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Precio de cierre")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.grid(True, alpha=0.2)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()


# ---------------------------------------------------------------
# 3. Matriz de correlación
# ---------------------------------------------------------------
def plot_correlation_matrix(df, feature_cols, out_path, title=""):
    data = df[feature_cols].dropna()
    corr = data.corr()
    n = len(feature_cols)

    fig_size = max(8, n * 0.55)
    fig, ax = plt.subplots(figsize=(fig_size, fig_size * 0.85))
    im = ax.imshow(corr.values, cmap="RdYlGn", vmin=-1, vmax=1, aspect="auto")
    cbar = plt.colorbar(im, ax=ax, shrink=0.75, pad=0.02)
    cbar.set_label("Correlación de Pearson", fontsize=9)

    short = [c.replace("sentiment_avg_", "avg_").replace("sentiment_std_", "std_")
               .replace("_sp500", "").replace("_smooth", "_sm") for c in feature_cols]
    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(short, rotation=45, ha="right", fontsize=7)
    ax.set_yticklabels(short, fontsize=7)

    font_size = max(4, min(7, 60 // n))
    for i in range(n):
        for j in range(n):
            val = corr.values[i, j]
            ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=font_size,
                    color="white" if abs(val) > 0.65 else "black")

    ax.set_title(f"Matriz de correlación — {title}", fontsize=12, fontweight="bold")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()


# ---------------------------------------------------------------
# main
# ---------------------------------------------------------------
def main():
    print(f"{'[DEBUG MODE]' if DEBUG_MODE else '[FULL MODE]'}")

    print("Cargando clustered_data...")
    df = pd.read_parquet(CLUSTERED_PATH)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    print(f"Filas: {len(df):,}  Columnas: {len(df.columns)}")

    print("Cargando indicadores técnicos...")
    tech = pd.read_parquet(TECH_PATH)

    model_cols   = get_model_cols(df)
    tech_feats   = get_tech_features(df)
    smooth_cols  = get_smooth_cols(df)
    feats_B      = tech_feats + smooth_cols  # Modelo B: técnicos + sentimiento suavizado

    print(f"Modelos disponibles: {model_cols}")
    print(f"Features técnicos: {len(tech_feats)} | Sentimiento suavizado: {len(smooth_cols)}")

    # Modelo principal del TFG: técnicos + sentimiento suavizado
    primary_label = "kmeans_tech_sent" if "kmeans_tech_sent" in df.columns else model_cols[0]

    # ---- 1. PCA — Modelo A (solo técnicos) como baseline ----
    print(f"\n[1/5] PCA Modelo A (solo técnicos) — kmeans_tech...")
    if "kmeans_tech" in df.columns:
        plot_pca_clusters(
            df, tech_feats, "kmeans_tech",
            OUT_DIR / "pca_kmeans_tech.png",
            title_suffix=" | solo técnicos (baseline)"
        )

    # ---- 2. PCA — Modelo B (técnicos + sentimiento suavizado) ----
    if "kmeans_tech_sent" in df.columns and smooth_cols:
        print(f"\n[2/5] PCA Modelo B (técnicos + sentimiento suavizado)...")
        plot_pca_clusters(
            df, feats_B, "kmeans_tech_sent",
            OUT_DIR / "pca_kmeans_tech_sent.png",
            title_suffix=" | técnicos + sentimiento suavizado"
        )

    # ---- 3. S&P 500 con regímenes — modelo principal + baseline ----
    print(f"\n[3/5] S&P 500 con regímenes...")
    for label_col in ["kmeans_tech_sent", "kmeans_tech", "hmm_tech_sent"]:
        if label_col in df.columns:
            plot_sp500_regimes(df, tech, label_col, OUT_DIR / f"sp500_regimes_{label_col}.png")
            print(f"  -> sp500_regimes_{label_col}.png")

    # ---- 4. Matriz de correlación — solo técnicos ----
    print(f"\n[4/5] Matriz de correlación (solo técnicos)...")
    plot_correlation_matrix(
        df, tech_feats,
        OUT_DIR / "correlation_tech.png",
        title="Indicadores técnicos"
    )

    # ---- 5. Matriz de correlación — técnicos + sentimiento suavizado ----
    if smooth_cols:
        print(f"\n[5/5] Matriz de correlación (técnicos + sentimiento suavizado)...")
        plot_correlation_matrix(
            df, feats_B,
            OUT_DIR / "correlation_tech_sent.png",
            title="Técnicos + sentimiento suavizado"
        )

    print(f"\n✅ Visualizaciones guardadas en: {OUT_DIR}")


if __name__ == "__main__":
    main()