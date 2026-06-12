import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")

DEBUG_MODE = False
IN_PATH = Path("data/processed/clustering_dataset_debug.parquet") if DEBUG_MODE else Path("data/processed/clustering_dataset.parquet")
OUT_DIR = Path("outputs/clustering_debug") if DEBUG_MODE else Path("outputs/clustering")
OUT_DIR.mkdir(parents=True, exist_ok=True)

N_CLUSTERS = 3

FEATURES_TECH = [
    "RSI_sp500", "MA50_ratio_sp500", "MA200_ratio_sp500",
    "mom21_sp500", "mom126_sp500",
    "vol30d_sp500", "vol252d_sp500",
    "skew30d_sp500", "skew252d_sp500"
]

def get_sentiment_cols(df):
    return [c for c in df.columns
            if (c.startswith("sentiment_avg_") or c.startswith("sentiment_std_"))
            and not c.endswith("_smooth")]

def get_smooth_cols(df):
    return [c for c in df.columns if c.startswith("sentiment_avg_") and c.endswith("_smooth")]

def run_kmeans(X_scaled, df, label_prefix, profile_cols, n_clusters=N_CLUSTERS):
    print(f"\n--- K-Means ({label_prefix}) ---")

    inertias, silhouettes = [], []
    ks = range(2, 8)
    for k in ks:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X_scaled)
        inertias.append(km.inertia_)
        silhouettes.append(silhouette_score(X_scaled, labels))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    ax1.plot(ks, inertias, marker="o")
    ax1.set_title(f"K-Means Elbow — {label_prefix}")
    ax1.set_xlabel("Número de clusters")
    ax1.set_ylabel("Inertia")
    ax2.plot(ks, silhouettes, marker="o", color="orange")
    ax2.set_title(f"K-Means Silhouette — {label_prefix}")
    ax2.set_xlabel("Número de clusters")
    ax2.set_ylabel("Silhouette score")
    plt.tight_layout()
    plt.savefig(OUT_DIR / f"kmeans_elbow_silhouette_{label_prefix}.png", dpi=200)
    plt.close()

    km_final = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = km_final.fit_predict(X_scaled)
    sil = silhouette_score(X_scaled, labels)
    print(f"Silhouette score (k={n_clusters}): {sil:.4f}")
    print("Distribución de clusters:")
    print(pd.Series(labels).value_counts().sort_index())
    print("\nPerfil de clusters (medias de features del modelo):")
    profile = df.copy()
    profile[f"kmeans_{label_prefix}"] = labels
    cols_to_show = [c for c in profile_cols if c in profile.columns]
    print(profile.groupby(f"kmeans_{label_prefix}")[cols_to_show].mean().round(4))
    return sil, labels


def main():
    print(f"{'[DEBUG MODE]' if DEBUG_MODE else '[FULL MODE]'}")
    print("Cargando dataset...")
    df = pd.read_parquet(IN_PATH)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.dropna(subset=FEATURES_TECH).sort_values("date").reset_index(drop=True)
    print(f"Filas para clustering: {len(df):,}")
    print(f"Columnas: {len(df.columns)}")

    sentiment_cols = get_sentiment_cols(df)
    smooth_cols    = get_smooth_cols(df)
    FEATURES_ALL   = FEATURES_TECH + sentiment_cols

    print(f"Features técnicos: {len(FEATURES_TECH)}")
    print(f"Features sentimiento crudas: {len(sentiment_cols)}")
    print(f"Features sentimiento suavizado: {len(smooth_cols)}")

    rows_per_date = df.groupby("date").size()
    print(f"\nFilas por fecha — media: {rows_per_date.mean():.2f}, max: {rows_per_date.max()}")
    print(f"Días únicos: {df['date'].nunique():,}")

    # ============================
    # MODELO A — Solo técnicos
    # ============================
    print("\n" + "="*50)
    print("MODELO A — Solo indicadores técnicos")
    print("="*50)
    scaler_A = StandardScaler()
    X_A = scaler_A.fit_transform(df[FEATURES_TECH].values)
    sil_A, labels_A = run_kmeans(X_A, df, label_prefix="tech", profile_cols=FEATURES_TECH)

    # ============================
    # MODELO B — Técnicos + sentimiento crudo
    # ============================
    print("\n" + "="*50)
    print("MODELO B — Indicadores técnicos + sentimiento sectorial")
    print("="*50)
    print(f"Features: {len(FEATURES_TECH)} técnicos + {len(sentiment_cols)} sentimiento = {len(FEATURES_ALL)} total")
    scaler_B = StandardScaler()
    X_B = scaler_B.fit_transform(df[FEATURES_ALL].values)
    sil_B, labels_B = run_kmeans(X_B, df, label_prefix="tech_sent", profile_cols=FEATURES_TECH)

    # ============================
    # MODELO C — Solo sentimiento suavizado (MA21)
    # ============================
    print("\n" + "="*50)
    print("MODELO C — Solo sentimiento suavizado (MA21)")
    print("="*50)
    if smooth_cols:
        print(f"Features: {len(smooth_cols)} columnas sentimiento suavizado")
        scaler_C = StandardScaler()
        X_C = scaler_C.fit_transform(df[smooth_cols].values)
        sil_C, labels_C = run_kmeans(X_C, df, label_prefix="sent_smooth", profile_cols=smooth_cols)
    else:
        print("  [SKIP] No hay columnas de sentimiento suavizado.")
        sil_C, labels_C = None, None

    # ============================
    # COMPARATIVA
    # ============================
    print("\n" + "="*50)
    print("COMPARATIVA DE MODELOS")
    print("="*50)
    print(f"Silhouette K-Means Modelo A (solo técnicos):          {sil_A:.4f}")
    print(f"Silhouette K-Means Modelo B (técnicos + sentimiento): {sil_B:.4f}")
    if sil_C is not None:
        print(f"Silhouette K-Means Modelo C (solo sentimiento MA21):  {sil_C:.4f}")
    print(f"Mejora al añadir sentimiento: {sil_B - sil_A:+.4f}")

    # ============================
    # GUARDAR
    # ============================
    df["kmeans_tech"]      = labels_A
    df["kmeans_tech_sent"] = labels_B
    if labels_C is not None:
        df["kmeans_sent_smooth"] = labels_C

    df.to_parquet(OUT_DIR / "clustered_data.parquet", index=False)
    print(f"\n✅ Guardado en: {OUT_DIR / 'clustered_data.parquet'}")
    print(f"Columnas finales: {df.columns.tolist()}")

if __name__ == "__main__":
    main()