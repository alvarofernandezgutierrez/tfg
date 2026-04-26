import pandas as pd
df = pd.read_parquet("data/processed/news/fnspid_sentiment.parquet")
print(len(df))