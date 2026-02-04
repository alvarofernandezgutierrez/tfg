import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

MARKET_PATH = Path("data/raw/market/sp500_ohlc.csv")
OUT_DIR = Path("outputs/eda/market")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    df = pd.read_csv(MARKET_PATH)

    # Normaliza columnas por si acaso
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "close"])
    df = df.sort_values("date").reset_index(drop=True)

    # ---- Retornos diarios (log) ----
    df["ret"] = np.log(df["close"] / df["close"].shift(1))

    # ---- Volatilidad rolling (30 días, anualizada) ----
    window = 30
    df["vol_30d"] = df["ret"].rolling(window).std() * np.sqrt(252)

    # ---- Drawdown ----
    df["cum_max"] = df["close"].cummax()
    df["drawdown"] = (df["close"] - df["cum_max"]) / df["cum_max"]

    # =========================================================
    # NUEVO: Métricas anuales
    # =========================================================

    # Año calendario
    df["year"] = df["date"].dt.year

    # Retorno anual (simple): close_last / close_first - 1
    annual_close = df.groupby("year")["close"].agg(first="first", last="last")
    annual_close["annual_return_simple"] = annual_close["last"] / annual_close["first"] - 1.0

    # Retorno anual (log): suma de log-returns del año
    annual_log_return = df.groupby("year")["ret"].sum(min_count=1).rename("annual_return_log")

    # Volatilidad anual (por año): std diaria del año * sqrt(252)
    annual_vol = (df.groupby("year")["ret"].std() * np.sqrt(252)).rename("annual_volatility")

    # Junta todo
    annual_metrics = (
        annual_close[["annual_return_simple"]]
        .join(annual_log_return)
        .join(annual_vol)
        .reset_index()
        .dropna(subset=["annual_return_simple", "annual_volatility"])  # por seguridad
    )

    # Guarda CSV anual
    annual_metrics.to_csv(OUT_DIR / "annual_metrics.csv", index=False)

    # Volatilidad anual global (todo el periodo)
    annual_vol_global = df["ret"].std() * np.sqrt(252)

    # ---- Estadísticas descriptivas ----
    stats = {
        "start_date": df["date"].min(),
        "end_date": df["date"].max(),
        "mean_daily_return": df["ret"].mean(),
        "std_daily_return": df["ret"].std(),
        "annualized_volatility_global": annual_vol_global,
        "annualized_volatility_mean_30d": df["vol_30d"].mean(),
        "max_drawdown": df["drawdown"].min(),
        "best_year_simple_return": annual_metrics.loc[annual_metrics["annual_return_simple"].idxmax(), "year"]
            if len(annual_metrics) else None,
        "worst_year_simple_return": annual_metrics.loc[annual_metrics["annual_return_simple"].idxmin(), "year"]
            if len(annual_metrics) else None,
    }

    with open(OUT_DIR / "summary.txt", "w", encoding="utf-8") as f:
        for k, v in stats.items():
            f.write(f"{k}: {v}\n")

    # ---- Gráfico 1: Nivel del índice ----
    plt.figure()
    plt.plot(df["date"], df["close"])
    plt.title("S&P 500 – Nivel del índice")
    plt.xlabel("Fecha")
    plt.ylabel("Índice")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "sp500_level.png", dpi=200)
    plt.close()

    # ---- Gráfico 2: Retornos diarios ----
    plt.figure()
    plt.hist(df["ret"].dropna(), bins=100)
    plt.title("Distribución de retornos diarios (log)")
    plt.xlabel("Retorno")
    plt.ylabel("Frecuencia")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "returns_hist.png", dpi=200)
    plt.close()

    # ---- Gráfico 3: Volatilidad rolling ----
    plt.figure()
    plt.plot(df["date"], df["vol_30d"])
    plt.title("Volatilidad anualizada (rolling 30 días)")
    plt.xlabel("Fecha")
    plt.ylabel("Volatilidad")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "volatility_30d.png", dpi=200)
    plt.close()

    # ---- Gráfico 4: Drawdown ----
    plt.figure()
    plt.plot(df["date"], df["drawdown"])
    plt.title("Drawdown del S&P 500")
    plt.xlabel("Fecha")
    plt.ylabel("Drawdown")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "drawdown.png", dpi=200)
    plt.close()

    # ---- NUEVO Gráfico 5: Retornos anuales (simple) ----
    plt.figure()
    plt.bar(annual_metrics["year"], annual_metrics["annual_return_simple"])
    plt.title("Retornos anuales del S&P 500 (simple)")
    plt.xlabel("Año")
    plt.ylabel("Retorno anual")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "annual_returns.png", dpi=200)
    plt.close()

    # ---- NUEVO Gráfico 6: Volatilidad anual (por año) ----
    plt.figure()
    plt.plot(annual_metrics["year"], annual_metrics["annual_volatility"], marker="o")
    plt.title("Volatilidad anualizada por año (a partir de retornos diarios)")
    plt.xlabel("Año")
    plt.ylabel("Volatilidad anualizada")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "annual_volatility.png", dpi=200)
    plt.close()

    print("✅ Bloque 2 completado (incluye anual)")
    print("Outputs en:", OUT_DIR)
    print("Resumen:", OUT_DIR / "summary.txt")
    print("CSV anual:", OUT_DIR / "annual_metrics.csv")

if __name__ == "__main__":
    main()
