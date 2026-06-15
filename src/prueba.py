import pandas as pd
df = pd.read_parquet("data/processed/news/fnspid_sentiment_by_sector.parquet")
print(f"Filas: {len(df):,}")
print(f"Rango: {df['date'].min()} -> {df['date'].max()}")

df2 = pd.read_parquet("data/processed/clustering_dataset.parquet")
print(f"Filas clustering: {len(df2):,}")
print(f"Rango: {df2['date'].min()} -> {df2['date'].max()}")
print(df2.shape[1])
print(df2.columns.tolist())
