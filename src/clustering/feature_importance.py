import pandas as pd
import numpy as np
from pathlib import Path
from scipy.stats import f_oneway
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import warnings
warnings.filterwarnings("ignore")

DEBUG_MODE = False

CLUSTERED_PATH = Path("outputs/clustering_debug/clustered_data.parquet") if DEBUG_MODE else Path("outputs/clustering/clustered_data.parquet")
OUT_DIR = Path("outputs/clustering/validation")
OUT_DIR.mkdir(parents=True, exist_ok=True)

FEATURES_TECH = [
    "RSI_sp500", "MA50_ratio_sp500", "MA200_ratio_sp500",
    "mom21_sp500", "mom126_sp500",
    "vol30d_sp500", "vol252d_sp500",
    "skew30d_sp500", "skew252d_sp500"
]


def get_model_cols(df):
    return [c for c in df.columns if c.startswith("kmeans_") or c.startswith("hmm_")]

def get_feature_cols(df):
    exclude = {"date"} | set(get_model_cols(df))
    return [c for c in df.columns if c not in exclude]

def feature_type(feat):
    if feat in FEATURES_TECH:
        return "Técnico"
    if feat.endswith("_smooth"):
        return "Sentimiento suavizado"
    return "Sentimiento"

def compute_importance(df, features, label_col):
    unique_labels = sorted(df[label_col].dropna().unique())
    groups = {lbl: df[df[label_col] == lbl][features] for lbl in unique_labels}

    records = []
    for feat in features:
        centroid_vals = [g[feat].mean() for g in groups.values()]
        centroid_spread = float(np.std(centroid_vals))

        arrays = [g[feat].dropna().values for g in groups.values()]
        if all(len(a) > 1 for a in arrays):
            try:
                f_stat, p_val = f_oneway(*arrays)
            except Exception:
                f_stat, p_val = np.nan, np.nan
        else:
            f_stat, p_val = np.nan, np.nan

        records.append({
            "feature": feat,
            "tipo": feature_type(feat),
            "centroid_spread": centroid_spread,
            "f_statistic": float(f_stat) if not np.isnan(f_stat) else 0.0,
            "p_value": float(p_val) if not np.isnan(p_val) else 1.0,
            **{f"centroid_cluster_{lbl}": float(g[feat].mean()) for lbl, g in groups.items()},
        })

    result = pd.DataFrame(records).sort_values("f_statistic", ascending=False).reset_index(drop=True)
    return result


def plot_importance(result, label_col, out_path):
    top_n = min(25, len(result))
    top_f = result.head(top_n)
    top_spread = result.sort_values("centroid_spread", ascending=False).head(top_n)

    type_colors = {"Técnico": "#0D7A3E", "Sentimiento": "#2196F3", "Sentimiento suavizado": "#9C27B0"}

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, max(6, top_n * 0.35)))

    # Panel izquierdo: F-statistic
    colors_f = [type_colors.get(t, "#999") for t in top_f["tipo"]]
    bars1 = ax1.barh(range(len(top_f)), top_f["f_statistic"].values, color=colors_f, alpha=0.85)
    ax1.set_yticks(range(len(top_f)))
    ax1.set_yticklabels(top_f["feature"].values, fontsize=7)
    ax1.set_xlabel("F-statistic (ANOVA entre clusters)", fontsize=9)
    ax1.set_title("Importancia por F-statistic", fontsize=11, fontweight="bold")
    ax1.invert_yaxis()
    ax1.grid(True, alpha=0.25, axis="x")

    # Panel derecho: dispersión de centroides
    colors_s = [type_colors.get(t, "#999") for t in top_spread["tipo"]]
    ax2.barh(range(len(top_spread)), top_spread["centroid_spread"].values, color=colors_s, alpha=0.85)
    ax2.set_yticks(range(len(top_spread)))
    ax2.set_yticklabels(top_spread["feature"].values, fontsize=7)
    ax2.set_xlabel("Dispersión de centroides (std)", fontsize=9)
    ax2.set_title("Importancia por dispersión de centroides", fontsize=11, fontweight="bold")
    ax2.invert_yaxis()
    ax2.grid(True, alpha=0.25, axis="x")

    legend_handles = [mpatches.Patch(color=c, label=t) for t, c in type_colors.items()]
    fig.legend(handles=legend_handles, loc="lower center", ncol=3, fontsize=9,
               bbox_to_anchor=(0.5, -0.04), framealpha=0.85)

    fig.suptitle(f"Importancia de variables — {label_col}", fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()


def main():
    print(f"{'[DEBUG MODE]' if DEBUG_MODE else '[FULL MODE]'}")

    print("Cargando clustered_data...")
    df = pd.read_parquet(CLUSTERED_PATH)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    print(f"Filas: {len(df):,}  Columnas: {len(df.columns)}")

    model_cols = get_model_cols(df)
    feature_cols = get_feature_cols(df)
    print(f"Modelos disponibles: {model_cols}")
    print(f"Features analizadas: {len(feature_cols)}")

    primary_label = "kmeans_tech_sent" if "kmeans_tech_sent" in df.columns else model_cols[0]
    print(f"Modelo principal: {primary_label}")

    result = compute_importance(df, feature_cols, primary_label)

    # ---- Resumen en consola ----
    print("\n" + "=" * 80)
    print(f"IMPORTANCIA DE VARIABLES — {primary_label}")
    print("=" * 80)
    print(f"{'#':<4} {'Feature':<45} {'Tipo':<22} {'F-stat':>8} {'p-value':>10}")
    print("-" * 95)
    for i, row in result.iterrows():
        sig = "***" if row["p_value"] < 0.001 else "**" if row["p_value"] < 0.01 else "*" if row["p_value"] < 0.05 else ""
        print(f"{i+1:<4} {row['feature']:<45} {row['tipo']:<22} {row['f_statistic']:>8.2f} {row['p_value']:>10.4f} {sig}")

    # ---- Top features por tipo ----
    print("\n" + "=" * 55)
    print("TOP 3 POR TIPO DE VARIABLE")
    print("=" * 55)
    for tipo in result["tipo"].unique():
        top3 = result[result["tipo"] == tipo].head(3)
        print(f"\n  {tipo}:")
        for _, r in top3.iterrows():
            print(f"    {r['feature']:<40} F={r['f_statistic']:.2f}")

    # ---- Guardar CSV ----
    result.to_csv(OUT_DIR / "feature_importance.csv", index=False)
    print(f"\n✅ CSV guardado en: {OUT_DIR / 'feature_importance.csv'}")

    # ---- Guardar gráfico ----
    plot_importance(result, primary_label, OUT_DIR / "feature_importance.png")
    print(f"✅ Gráfico guardado en: {OUT_DIR / 'feature_importance.png'}")


if __name__ == "__main__":
    main()
