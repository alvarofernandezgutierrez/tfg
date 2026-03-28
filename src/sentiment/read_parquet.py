import pandas as pd
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", 80)
df = pd.read_parquet("data/processed/news/fnspid_sentiment_aggregated.parquet")
print(df.head(20))
print(df.dtypes)