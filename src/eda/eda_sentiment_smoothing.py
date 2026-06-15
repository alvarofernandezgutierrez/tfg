import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

DEBUG_MODE = False

DATA_PATH = Path("data/processed/news/fnspid_sentiment_by_sector.parquet")
OUT_DIR = Path("outputs/eda/sentiment_smoothing")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Consistente con el resto del pipeline
DATE_CUTOFF = "2024-12-31"

WINDOWS = [5, 10, 21]
WINDOW_COLORS = {"5": "#2196F3", "10": "#FF9800", "21": "#4CAF50"}


def main():
    print(f"{'[DEBUG MODE]' if DEBUG_MODE else '[FULL MODE]'}")

    # ---- 1. Cargar datos ----
    df = pd.read_parquet(DATA_PATH)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values("date").reset_index(drop=True)

    # FIX: filtro temporal consistente con el pipeline de clustering
    df = df[df["date"] <= DATE_CUTOFF].copy()
    print(f"Filas cargadas: {len(df):,}")
    print(f"Rango de fechas: {df['date'].min().date()} -> {df['date'].max().date()}")

    # ---- 2. Detectar columnas de sectores ----
    sentiment_cols = [c for c in df.columns if c.startswith("sentiment_avg_")
                      and not c.endswith("_smooth")]
    sectors = [c.replace("sentiment_avg_", "") for c in sentiment_cols]
    print(f"\nSectores encontrados ({len(sectors)}):")
    for s in sectors:
        print(f"  - {s.replace('_', ' ')}")

    # ---- 3. Graficar y calcular ruido ----
    print("\n" + "=" * 65)
    print("ESTADÍSTICAS DE RUIDO (std) — ORIGINAL vs SUAVIZADO")
    print("=" * 65)

    noise_records = []

    for col, sector in zip(sentiment_cols, sectors):
        series = df[col].copy()
        std_orig = series.std()

        stds_smooth = {}
        smooth_series = {}
        for w in WINDOWS:
            s = series.rolling(window=w, min_periods=1).mean()
            smooth_series[w] = s
            stds_smooth[w] = s.std()

        noise_records.append({
            "sector": sector.replace("_", " "),
            "std_original": std_orig,
            **{f"std_MA{w}": stds_smooth[w] for w in WINDOWS},
            **{f"noise_red_MA{w}_pct": (1 - stds_smooth[w] / std_orig) * 100 for w in WINDOWS},
        })

        # Gráfico de la serie + medias móviles
        fig, axes = plt.subplots(2, 1, figsize=(14, 7), sharex=True)
        fig.suptitle(
            f"Sentimiento sectorial — {sector.replace('_', ' ')}",
            fontsize=13, fontweight="bold"
        )

        axes[0].plot(df["date"], series, alpha=0.35, color="#888888",
                     linewidth=0.9, label="Original")
        for w in WINDOWS:
            axes[0].plot(df["date"], smooth_series[w],
                         linewidth=1.8, color=WINDOW_COLORS[str(w)],
                         label=f"MA{w}")
        axes[0].set_ylabel("Sentimiento promedio")
        axes[0].legend(loc="upper right", fontsize=9)
        axes[0].grid(True, alpha=0.25)
        axes[0].axhline(0, color="black", linewidth=0.7, linestyle="--")

        residual = series - smooth_series[21]
        axes[1].fill_between(df["date"], residual, 0,
                             where=(residual >= 0), color="#4CAF50", alpha=0.4, label="Ruido +")
        axes[1].fill_between(df["date"], residual, 0,
                             where=(residual < 0), color="#F44336", alpha=0.4, label="Ruido –")
        axes[1].set_ylabel("Residual (orig – MA21)")
        axes[1].set_xlabel("Fecha")
        axes[1].legend(loc="upper right", fontsize=9)
        axes[1].grid(True, alpha=0.25)
        axes[1].axhline(0, color="black", linewidth=0.7)

        axes[1].xaxis.set_major_locator(mdates.YearLocator())
        axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        plt.xticks(rotation=0)

        plt.tight_layout()
        out_path = OUT_DIR / f"{sector}_smooth.png"
        plt.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close()

    # ---- 4. Resumen de ruido ----
    df_noise = pd.DataFrame(noise_records).set_index("sector")
    print(f"\n{'Sector':<35} {'std_orig':>10} {'std_MA5':>10} {'std_MA10':>10} {'std_MA21':>10}")
    print("-" * 75)
    for idx, row in df_noise.iterrows():
        print(f"{idx:<35} {row['std_original']:>10.6f} {row['std_MA5']:>10.6f} "
              f"{row['std_MA10']:>10.6f} {row['std_MA21']:>10.6f}")

    print("\n" + "=" * 65)
    print("REDUCCIÓN DE RUIDO MEDIA POR VENTANA")
    print("=" * 65)
    for w in WINDOWS:
        col = f"noise_red_MA{w}_pct"
        mean_red = df_noise[col].mean()
        min_red  = df_noise[col].min()
        max_red  = df_noise[col].max()
        print(f"  MA{w:2d}: media={mean_red:5.1f}%  min={min_red:5.1f}%  max={max_red:5.1f}%")

    print("\n" + "=" * 65)
    print("JUSTIFICACIÓN DE VENTANA GANADORA")
    print("=" * 65)
    best_reduction = df_noise[[f"noise_red_MA{w}_pct" for w in WINDOWS]].mean()
    for w in WINDOWS:
        print(f"  MA{w:2d}: {best_reduction[f'noise_red_MA{w}_pct']:.1f}% reducción promedio de std")
    print(
        "\n  >> MA21 elegida: equilibrio optimo entre suavizado (~max reduccion de ruido)"
        "\n     y preservacion de tendencias de medio plazo relevantes para clustering."
    )

    # ---- 5. Gráfico resumen comparativo (todos los sectores, MA21) ----
    # FIX: calcular nrows dinámicamente para evitar IndexError
    n = len(sectors)
    ncols = 3
    nrows = int(np.ceil(n / ncols))

    fig, axes = plt.subplots(nrows, ncols, figsize=(18, nrows * 4), sharex=True)
    axes = axes.flatten()

    for i, (col, sector) in enumerate(zip(sentiment_cols, sectors)):
        ax = axes[i]
        series = df[col]
        ma21 = series.rolling(window=21, min_periods=1).mean()
        ax.plot(df["date"], series, alpha=0.3, color="#888888", linewidth=0.7)
        ax.plot(df["date"], ma21, color="#4CAF50", linewidth=1.5, label="MA21")
        ax.set_title(sector.replace("_", " "), fontsize=9)
        ax.axhline(0, color="black", linewidth=0.5, linestyle="--")
        ax.grid(True, alpha=0.2)
        ax.xaxis.set_major_locator(mdates.YearLocator(2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

    # FIX: ocultar subplots sobrantes correctamente
    for j in range(n, len(axes)):
        axes[j].set_visible(False)

    fig.suptitle(
        f"Sentimiento por sector — Serie original vs MA21 (hasta {DATE_CUTOFF[:4]})",
        fontsize=14, fontweight="bold"
    )
    plt.tight_layout()
    plt.savefig(OUT_DIR / "_resumen_todos_sectores_MA21.png", dpi=150, bbox_inches="tight")
    plt.close()

    print(f"\n✅ {n} gráficos individuales + 1 resumen guardados en: {OUT_DIR}")


if __name__ == "__main__":
    main()