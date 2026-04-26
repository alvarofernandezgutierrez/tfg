import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from hmmlearn.hmm import GaussianHMM
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
    return [c for c in df.columns if c.startswith("sentiment_avg_") or c.startswith("sentiment_std_")]

def run_kmeans(X_scaled, df, label_prefix, n_clusters=N_CLUSTERS):
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
    print("\nPerfil de clusters (medias de indicadores técnicos):")
    profile = df.copy()
    profile[f"kmeans_{label_prefix}"] = labels
    print(profile.groupby(f"kmeans_{label_prefix}")[FEATURES_TECH].mean().round(4))
    return sil, labels

def run_hmm(df_daily, features, label_prefix, n_clusters=N_CLUSTERS):
    print(f"\n--- HMM ({label_prefix}) ---")

    df_hmm = df_daily[["date"] + features].copy().sort_values("date").reset_index(drop=True)

    scaler = StandardScaler()
    X_hmm = scaler.fit_transform(df_hmm[features].values)

    hmm = GaussianHMM(
        n_components=n_clusters,
        covariance_type="full",
        n_iter=1000,
        random_state=42,
        min_covar=1e-3
    )
    hmm.fit(X_hmm)
    df_hmm[f"hmm_{label_prefix}"] = hmm.predict(X_hmm)

    print("Distribución de estados HMM:")
    print(df_hmm[f"hmm_{label_prefix}"].value_counts().sort_index())
    print(f"\nPerfil de estados HMM (medias de features del modelo):")
    print(df_hmm.groupby(f"hmm_{label_prefix}")[FEATURES_TECH].mean().round(4))

    colors = ["tab:blue", "tab:orange", "tab:red"]
    plt.figure(figsize=(14, 4))
    for state in range(n_clusters):
        mask = df_hmm[f"hmm_{label_prefix}"] == state
        plt.scatter(
            df_hmm.loc[mask, "date"],
            df_hmm.loc[mask, "vol30d_sp500"],
            label=f"Estado {state}",
            s=10, alpha=0.6,
            color=colors[state]
        )
    plt.title(f"Estados HMM a lo largo del tiempo — {label_prefix}")
    plt.xlabel("Fecha")
    plt.ylabel("Volatilidad 30d")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR / f"hmm_states_time_{label_prefix}.png", dpi=200)
    plt.close()

    return df_hmm[["date", f"hmm_{label_prefix}"]]

def main():
    print(f"{'[DEBUG MODE]' if DEBUG_MODE else '[FULL MODE]'}")
    print("Cargando dataset...")
    df = pd.read_parquet(IN_PATH)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.dropna(subset=FEATURES_TECH).reset_index(drop=True)
    print(f"Filas para clustering: {len(df):,}")
    print(f"Columnas: {len(df.columns)}")

    sentiment_cols = get_sentiment_cols(df)
    FEATURES_ALL = FEATURES_TECH + sentiment_cols

    rows_per_date = df.groupby("date").size()
    print(f"\nFilas por fecha — media: {rows_per_date.mean():.2f}, max: {rows_per_date.max()}")

    df_daily = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    print(f"Días únicos para HMM: {len(df_daily):,}")

    # ============================
    # MODELO A — Solo técnicos
    # ============================
    print("\n" + "="*50)
    print("MODELO A — Solo indicadores técnicos")
    print("="*50)
    scaler_A = StandardScaler()
    X_A = scaler_A.fit_transform(df[FEATURES_TECH].values)
    sil_A, labels_A = run_kmeans(X_A, df, label_prefix="tech")
    hmm_A = run_hmm(df_daily, features=FEATURES_TECH, label_prefix="tech")

    # ============================
    # MODELO B — Técnicos + sentimiento
    # ============================
    print("\n" + "="*50)
    print("MODELO B — Indicadores técnicos + sentimiento sectorial")
    print("="*50)
    scaler_B = StandardScaler()
    X_B = scaler_B.fit_transform(df[FEATURES_ALL].values)
    sil_B, labels_B = run_kmeans(X_B, df, label_prefix="tech_sent")
    hmm_B = run_hmm(df_daily, features=FEATURES_ALL, label_prefix="tech_sent")

    # ============================
    # COMPARATIVA
    # ============================
    print("\n" + "="*50)
    print("COMPARATIVA DE MODELOS")
    print("="*50)
    print(f"Silhouette K-Means Modelo A (solo técnicos):          {sil_A:.4f}")
    print(f"Silhouette K-Means Modelo B (técnicos + sentimiento): {sil_B:.4f}")
    print(f"Mejora al añadir sentimiento: {sil_B - sil_A:+.4f}")

    # ============================
    # GUARDAR
    # ============================
    df["kmeans_tech"] = labels_A
    df["kmeans_tech_sent"] = labels_B
    df = df.merge(hmm_A, on="date", how="left")
    df = df.merge(hmm_B, on="date", how="left")

    df.to_parquet(OUT_DIR / "clustered_data.parquet", index=False)
    print(f"\n✅ Guardado en: {OUT_DIR / 'clustered_data.parquet'}")
    print(f"Columnas finales: {df.columns.tolist()}")

if __name__ == "__main__":
    main()