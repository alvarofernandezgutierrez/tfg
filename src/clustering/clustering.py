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

IN_PATH = Path("data/processed/market_sentiment.parquet")
OUT_DIR = Path("outputs/clustering")
OUT_DIR.mkdir(parents=True, exist_ok=True)

FEATURES = ["sentiment_score_mean", "ret", "vol_30d"]
N_CLUSTERS = 3  # regímenes: pánico, calma, euforia

def main():
    # ---- 1. Cargar datos ----
    print("Cargando datos...")
    df = pd.read_parquet(IN_PATH)
    df = df.dropna(subset=FEATURES).reset_index(drop=True)
    print(f"Filas para clustering: {len(df):,}")

    # ---- 2. Features y escalado ----
    X = df[FEATURES].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # ============================
    # K-MEANS
    # ============================
    print("\n--- K-Means ---")

    # Elegir número óptimo de clusters (elbow + silhouette)
    inertias = []
    silhouettes = []
    ks = range(2, 8)
    for k in ks:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X_scaled)
        inertias.append(km.inertia_)
        silhouettes.append(silhouette_score(X_scaled, labels))

    # Gráfico elbow
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    ax1.plot(ks, inertias, marker="o")
    ax1.set_title("K-Means — Elbow")
    ax1.set_xlabel("Número de clusters")
    ax1.set_ylabel("Inertia")
    ax2.plot(ks, silhouettes, marker="o", color="orange")
    ax2.set_title("K-Means — Silhouette")
    ax2.set_xlabel("Número de clusters")
    ax2.set_ylabel("Silhouette score")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "kmeans_elbow_silhouette.png", dpi=200)
    plt.close()

    # Ajustar con N_CLUSTERS
    km_final = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=10)
    df["kmeans_cluster"] = km_final.fit_predict(X_scaled)
    print(f"Silhouette score (k={N_CLUSTERS}): {silhouette_score(X_scaled, df['kmeans_cluster']):.4f}")
    print("Distribución de clusters:")
    print(df["kmeans_cluster"].value_counts().sort_index())

    # Perfil de cada cluster
    print("\nPerfil de clusters (medias):")
    print(df.groupby("kmeans_cluster")[FEATURES].mean().round(4))

    # ============================
    # HMM
    # ============================
    print("\n--- HMM ---")

    # HMM necesita serie temporal ordenada — agregar por fecha
    df_hmm = (
        df.groupby("date")[FEATURES]
        .mean()
        .sort_index()
        .reset_index()
    )
    X_hmm = df_hmm[FEATURES].values
    X_hmm_scaled = scaler.fit_transform(X_hmm)

    hmm = GaussianHMM(
        n_components=N_CLUSTERS,
        covariance_type="full",
        n_iter=200,
        random_state=42
    )
    hmm.fit(X_hmm_scaled)
    df_hmm["hmm_state"] = hmm.predict(X_hmm_scaled)

    print("Distribución de estados HMM:")
    print(df_hmm["hmm_state"].value_counts().sort_index())
    print("\nPerfil de estados HMM (medias):")
    print(df_hmm.groupby("hmm_state")[FEATURES].mean().round(4))

    # Gráfico HMM — estados a lo largo del tiempo
    plt.figure(figsize=(14, 4))
    for state in range(N_CLUSTERS):
        mask = df_hmm["hmm_state"] == state
        plt.scatter(df_hmm.loc[mask, "date"], df_hmm.loc[mask, "ret"],
                    label=f"Estado {state}", s=10, alpha=0.6)
    plt.title("Estados HMM a lo largo del tiempo")
    plt.xlabel("Fecha")
    plt.ylabel("Retorno diario")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR / "hmm_states_time.png", dpi=200)
    plt.close()

    # ============================
    # GUARDAR RESULTADOS
    # ============================
    # Merge HMM states de vuelta al df principal
    df = df.merge(df_hmm[["date", "hmm_state"]], on="date", how="left")
    df.to_parquet(OUT_DIR / "clustered_data.parquet", index=False)
    print(f"\n✅ Guardado en: {OUT_DIR / 'clustered_data.parquet'}")

if __name__ == "__main__":
    main()