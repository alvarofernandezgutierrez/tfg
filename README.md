# Market Regime Detection via Financial News Sentiment Analysis and Clustering

**Bachelor's Thesis (TFG) — Mathematical Engineering & Artificial Intelligence**  
Universidad Pontificia Comillas (ICAI) · 2025–2026  
**Author:** Álvaro Fernández Gutiérrez · **Supervisor:** Guillermo Mestre Marcos

---

## Overview

This project designs and implements an end-to-end pipeline that combines **S&P 500 technical indicators** with **sector-level sentiment extracted from financial news via FinBERT**, with the goal of identifying market regimes through unsupervised clustering (K-Means) and evaluating whether incorporating sentiment improves the economic interpretability of the resulting regimes.

The pipeline spans the period **May 2009 – February 2024** and identifies three market regimes: **Bull**, **Bear**, and **High-Volatility/Recovery**, validated both internally (silhouette in a common PCA space) and economically (annualized return, volatility, Sharpe ratio, and maximum drawdown across 11 GICS sectors).

---

## Repository Structure

```
├── data/
│   ├── raw/
│   │   └── market/
│   │       ├── sp500_ohlc.csv                  S&P 500 daily OHLC prices
│   │       ├── sp500_historical_components.csv  Historical S&P 500 constituents
│   │       └── ticker_gics_mapping.csv          Ticker → GICS sector mapping
│   └── processed/
│       ├── market/
│       │   └── technical_indicators.parquet     9 daily technical indicators
│       ├── news/
│       │   ├── fnspid_sentiment.parquet         FinBERT scores per ticker/date
│       │   └── fnspid_sentiment_by_sector.parquet  Aggregated by GICS sector
│       └── clustering_dataset.parquet           Final clustering dataset (1,132 obs.)
│
├── outputs/
│   ├── clustering/
│   │   ├── clustered_data.parquet               Cluster labels for all models
│   │   └── validation/                          Economic validation outputs
│   │       ├── metrics_*.csv                    Sharpe, vol, drawdown by regime
│   │       ├── sector_regime_analysis.csv       11 sectors × 3 regimes metrics
│   │       ├── heatmap_*.png                    Sector heatmaps
│   │       ├── pca_*.png                        PCA scatter + loadings + scree
│   │       ├── sp500_regimes_*.png              S&P 500 colored by regime
│   │       ├── correlation_*.png                Correlation matrices
│   │       └── silhouette_pca_*.png             Silhouette in common PCA space
│   └── eda/
│       └── sentiment_smoothing/                 Sentiment series visualizations
│
└── src/
    ├── market/
    │   ├── download_sp500.py                    Download S&P 500 OHLC data
    │   ├── sp500_historical.py                  Historical constituent lookup
    │   └── technical_indicators.py              Compute 9 technical indicators
    ├── news/
    │   └── load_fnspid.py                       Load and cache FNSPID corpus
    ├── sentiment/
    │   ├── sentiment_finbert.py                 Batch FinBERT inference (GPU)
    │   ├── aggregate_sentiment.py               Aggregate to (date, ticker)
    │   ├── aggregate_by_sector.py               Aggregate to (date, sector) + MA21
    │   └── build_clustering_dataset.py          Merge technicals + sentiment
    ├── clustering/
    │   ├── clustering.py                        K-Means Models A, B, C
    │   ├── silhouette_pca.py                    Silhouette in common PCA space
    │   ├── feature_importance.py                ANOVA F-statistic importance
    │   ├── economic_validation.py               Market-level economic metrics
    │   ├── sector_regime_analysis.py            Sector-level economic metrics
    │   └── plot_clusters.py                     PCA, S&P 500, correlation plots
    └── eda/
        └── eda_sentiment_smoothing.py           Sentiment series + MA comparison
```

---

## Pipeline

The pipeline runs in the following order:

```
1. src/market/download_sp500.py
2. src/market/sp500_historical.py
3. src/market/technical_indicators.py
4. src/news/load_fnspid.py
5. src/sentiment/sentiment_finbert.py        ← requires GPU (RTX 3060 or equivalent)
6. src/sentiment/aggregate_sentiment.py
7. src/sentiment/aggregate_by_sector.py
8. src/sentiment/build_clustering_dataset.py
9. src/clustering/clustering.py
10. src/clustering/silhouette_pca.py
11. src/clustering/feature_importance.py
12. src/clustering/economic_validation.py
13. src/clustering/sector_regime_analysis.py
14. src/clustering/plot_clusters.py
15. src/eda/eda_sentiment_smoothing.py
```

---

## Models

Three K-Means configurations ($K=3$) are trained and compared:

| Model | Features | Dim | Column |
|-------|----------|-----|--------|
| A | Technical indicators only | 9 | `kmeans_tech` |
| B | Technical + raw sector sentiment (mean + std) | 31 | `kmeans_tech_sent` |
| C | Smoothed sentiment only (MA21) | 11 | `kmeans_sent_smooth` |

**Silhouette in common PCA space (9 components, 82.0% variance):**

| Model | Silhouette (own space) | Silhouette (PCA common) |
|-------|----------------------|------------------------|
| A — Technical only | 0.2969 | 0.1928 |
| B — Technical + sentiment | 0.1213 | 0.1778 |
| C — Smoothed sentiment only | 0.2005 | 0.1623 |

---

## Key Results

- **Model B** produces a Bear regime with negative Sharpe ratios in **10 out of 11 GICS sectors**, with the sharpest distinction between cyclical sectors (Energy: −8.5% annualized, max drawdown −74.9%) and defensive sectors (Utilities: +9.8%, Consumer Staples: +7.3%).
- **Model C** (sentiment only) fails to produce economically interpretable regimes — Sharpe range of only 2.18 vs 6.88 for Model B.
- **Sentiment adds incremental value** as a complement to technical indicators, not as a substitute. The 4 most discriminant sentiment variables (by ANOVA F-statistic) are the standard deviation of sentiment in IT, Consumer Staples, Consumer Discretionary and Energy sectors.

---

## Requirements

```bash
pip install pandas numpy pyarrow scikit-learn matplotlib torch transformers
pip install yfinance hmmlearn
```

FinBERT inference requires a CUDA-compatible GPU. The project was developed with:
- Python 3.11
- PyTorch 2.1.1 + CUDA 12.1
- `ProsusAI/finbert` checkpoint from Hugging Face

---

## Data Sources

| Source | Description |
|--------|-------------|
| [FNSPID](https://huggingface.co/datasets/Zihan1004/FNSPID) | ~58M financial news headlines with timestamps (2009–2024) |
| [yfinance](https://github.com/ranaroussi/yfinance) | S&P 500 OHLC prices and constituent ticker prices |
| S&P 500 historical components | Wikipedia / manual curation to avoid survivorship bias |

---

## Sentiment Scoring

Each news headline is scored with `ProsusAI/finbert`:

$$s = p_{\text{pos}} - p_{\text{neg}} \in [-1, 1]$$

Scores are aggregated daily by GICS sector (mean + std) and smoothed with a 21-session moving average (MA21), which reduces daily noise by an average of **66.4%** across the 11 sectors while preserving medium-term trends.

---

## License

This project is submitted as a Bachelor's Thesis at Universidad Pontificia Comillas. All code is original unless otherwise noted. The FNSPID dataset is publicly available under its own license.
