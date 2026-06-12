import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import yfinance as yf
from pathlib import Path

# ─── Rutas ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
CLUSTERED_PATH  = ROOT / "outputs/clustering/clustered_data.parquet"
GICS_PATH       = ROOT / "data/raw/market/ticker_gics_mapping.csv"
CACHE_PRICES    = ROOT / "data/processed/market/ticker_prices_daily.parquet"
OUT_BASE        = ROOT / "outputs/clustering/validation"

# ─── Modelos a procesar ───────────────────────────────────────────────────────
MODELS = {
    "modelA": {
        "col":    "kmeans_tech",
        "label":  "Model A (Técnico)",
        "folder": OUT_BASE / "modelA",
    },
    "modelB": {
        "col":    "kmeans_tech_sent",
        "label":  "Model B (Técnico + Sentimiento)",
        "folder": OUT_BASE / "modelB",
    },
    "modelC": {
        "col":    "kmeans_sent_smooth",
        "label":  "Model C (Sentimiento Suavizado)",
        "folder": OUT_BASE / "modelC",
    },
}

# ─── Parámetros ───────────────────────────────────────────────────────────────
REGIME_MAP   = {0: "Bull", 1: "Bear", 2: "Crisis"}
REGIME_ORDER = ["Bull", "Bear", "Crisis"]
RF           = 0.02
TD           = 252
BATCH_SIZE   = 100

METRICS_CONFIG = {
    "ret_anual":  ("Retorno Anualizado",       "RdYlGn",   ".1%"),
    "vol_anual":  ("Volatilidad Anualizada",    "RdYlGn_r", ".1%"),
    "sharpe":     ("Sharpe Ratio (rf=2%)",      "RdYlGn",   ".2f"),
    "max_dd":     ("Drawdown Máximo",           "RdYlGn",   ".1%"),
    "pct_pos":    ("% Días Positivos",          "RdYlGn",   ".1%"),
    "best_day":   ("Mejor Día (retorno)",       "RdYlGn",   ".1%"),
    "worst_day":  ("Peor Día (retorno)",        "RdYlGn",   ".1%"),
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def download_prices(tickers, start, end):
    frames = []
    n_batches = (len(tickers) - 1) // BATCH_SIZE + 1
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i: i + BATCH_SIZE]
        print(f"  Lote {i // BATCH_SIZE + 1}/{n_batches}: {len(batch)} tickers")
        try:
            raw = yf.download(batch, start=start, end=end, auto_adjust=True, progress=False)
            if isinstance(raw.columns, pd.MultiIndex):
                close = raw["Close"]
            else:
                col = "Close" if "Close" in raw.columns else raw.columns[0]
                close = raw[[col]].rename(columns={col: batch[0]})
            frames.append(close)
        except Exception as exc:
            print(f"    Error en lote: {exc}")
    if not frames:
        return pd.DataFrame()
    prices = pd.concat(frames, axis=1)
    prices.index = pd.to_datetime(prices.index)
    prices.index.name = "date"
    return prices


def max_drawdown(cum_returns):
    roll_max = cum_returns.cummax()
    dd = (cum_returns - roll_max) / roll_max
    return float(dd.min())


def compute_metrics(ret):
    r = ret.dropna()
    n = len(r)
    base = dict(ret_anual=np.nan, vol_anual=np.nan, sharpe=np.nan,
                max_dd=np.nan, pct_pos=np.nan, best_day=np.nan,
                worst_day=np.nan, n_dias=n)
    if n < 10:
        return base
    cum       = (1 + r).cumprod()
    total_ret = cum.iloc[-1] - 1
    ann_ret   = (1 + total_ret) ** (TD / n) - 1
    ann_vol   = r.std() * np.sqrt(TD)
    sharpe    = (ann_ret - RF) / ann_vol if ann_vol > 0 else np.nan
    return dict(
        ret_anual = ann_ret,
        vol_anual = ann_vol,
        sharpe    = sharpe,
        max_dd    = max_drawdown(cum),
        pct_pos   = float((r > 0).mean()),
        best_day  = float(r.max()),
        worst_day = float(r.min()),
        n_dias    = n,
    )


def generate_heatmaps(results_df, model_label, out_dir):
    """Genera los 7 heatmaps para un modelo dado."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for metric, (label, cmap, fmt) in METRICS_CONFIG.items():
        pivot = results_df.pivot(index="sector", columns="regime", values=metric)
        pivot = pivot.reindex(columns=[r for r in REGIME_ORDER if r in pivot.columns])

        fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(
            pivot,
            annot=True,
            fmt=fmt,
            cmap=cmap,
            ax=ax,
            linewidths=0.5,
            linecolor="white",
            cbar_kws={"label": label, "shrink": 0.8},
            annot_kws={"fontsize": 9},
        )
        ax.set_title(
            f"{label} por Sector y Régimen\n({model_label})",
            fontsize=12, fontweight="bold", pad=12
        )
        ax.set_xlabel("Régimen de Mercado", fontsize=11)
        ax.set_ylabel("Sector GICS", fontsize=11)
        ax.tick_params(axis="x", labelsize=10)
        ax.tick_params(axis="y", labelsize=9, rotation=0)
        plt.tight_layout()

        out_path = out_dir / f"heatmap_sector_{metric}.png"
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"    Guardado: {out_path.relative_to(ROOT)}")


# ─── 1. Cargar clustering y GICS ──────────────────────────────────────────────
print("=" * 60)
print("Cargando datos de clustering y GICS...")
clustered = pd.read_parquet(CLUSTERED_PATH)
clustered["date"] = pd.to_datetime(clustered["date"])
clustered = clustered.sort_values("date").reset_index(drop=True)

gics     = pd.read_csv(GICS_PATH)
sectors  = sorted(gics["sector"].unique())
tickers  = gics["ticker"].tolist()
print(f"  {len(tickers)} tickers | {len(sectors)} sectores")
print(f"  Rango fechas clustering: {clustered['date'].min().date()} → {clustered['date'].max().date()}")

START = clustered["date"].min().strftime("%Y-%m-%d")
END   = (clustered["date"].max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")


# ─── 2. Precios diarios (con caché) ───────────────────────────────────────────
if CACHE_PRICES.exists():
    print(f"\nCargando precios cacheados desde {CACHE_PRICES.name}...")
    prices = pd.read_parquet(CACHE_PRICES)
else:
    print(f"\nDescargando precios de {len(tickers)} tickers ({START} → {END})...")
    prices = download_prices(tickers, START, END)
    if not prices.empty:
        CACHE_PRICES.parent.mkdir(parents=True, exist_ok=True)
        prices.to_parquet(CACHE_PRICES)
        print(f"  Precios guardados en {CACHE_PRICES}")

available_tickers = [t for t in tickers if t in prices.columns]
prices = prices[available_tickers]
print(f"  {len(available_tickers)}/{len(tickers)} tickers disponibles")


# ─── 3. Retornos diarios ──────────────────────────────────────────────────────
daily_ret = prices.pct_change().dropna(how="all")
daily_ret.index = pd.to_datetime(daily_ret.index)
daily_ret.index.name = "date"


# ─── 4. Retorno EW por sector ─────────────────────────────────────────────────
print("\nCalculando retornos EW por sector...")
sector_ret = {}
for sector in sectors:
    ticks_s = [t for t in gics[gics["sector"] == sector]["ticker"] if t in daily_ret.columns]
    if ticks_s:
        sector_ret[sector] = daily_ret[ticks_s].mean(axis=1)

sector_ret_df = pd.DataFrame(sector_ret)
sector_ret_df.index.name = "date"
print(f"  {len(sector_ret_df.columns)} sectores con datos de precios")


# ─── 5. Bucle por modelo ──────────────────────────────────────────────────────
for model_key, model_cfg in MODELS.items():
    col_name    = model_cfg["col"]
    model_label = model_cfg["label"]
    out_dir     = model_cfg["folder"]

    print(f"\n{'=' * 60}")
    print(f"Procesando {model_label}  [columna: {col_name}]")

    if col_name not in clustered.columns:
        print(f"  AVISO: columna '{col_name}' no encontrada en clustered_data.parquet. Saltando.")
        continue

    # ── Asignar regímenes al calendario de precios ────────────────────────────
    regime_sparse = clustered.set_index("date")[col_name]
    regime_full = (
        regime_sparse
        .reindex(sector_ret_df.index, method=None)
        .ffill()
        .dropna()
        .astype(int)
    )

    print(f"  Distribución de regímenes:")
    for rid, rname in REGIME_MAP.items():
        print(f"    {rname}: {(regime_full == rid).sum()} días")

    # ── Métricas por (sector, régimen) ────────────────────────────────────────
    records = []
    for sector in sectors:
        if sector not in sector_ret_df.columns:
            continue
        for regime_id, regime_name in REGIME_MAP.items():
            mask    = regime_full == regime_id
            dates_in = mask[mask].index
            r       = sector_ret_df.loc[sector_ret_df.index.isin(dates_in), sector]
            metrics = compute_metrics(r)
            records.append({"sector": sector, "regime": regime_name, **metrics})

    results_df = pd.DataFrame(records)

    # Guardar CSV
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "sector_regime_metrics.csv"
    results_df.to_csv(csv_path, index=False)
    print(f"  CSV guardado: {csv_path.relative_to(ROOT)}")

    # ── Generar heatmaps ──────────────────────────────────────────────────────
    print(f"  Generando heatmaps...")
    generate_heatmaps(results_df, model_label, out_dir)

print(f"\n{'=' * 60}")
print("✓ Análisis completado para todos los modelos.")