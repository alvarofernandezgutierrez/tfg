import pandas as pd
from pathlib import Path

IN_PATH = Path("data/raw/news/fnspid_headlines.csv")
OUT_PATH = Path("data/processed/news/fnspid_headlines_200k.csv")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

TARGET_ROWS = 200_000
CHUNKSIZE = 1_000_000
PER_CHUNK = 10_000     # mejor representatividad (más chunks)
SEED = 42

def clean_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    for col in ["date", "title", "ticker", "publisher", "url"]:
        if col in chunk.columns:
            chunk[col] = chunk[col].astype(str).str.strip()
            chunk.loc[chunk[col].isin(["", "None", "nan", "NaN"]), col] = pd.NA

    chunk["date"] = pd.to_datetime(chunk["date"], errors="coerce")
    chunk = chunk.dropna(subset=["date", "title", "url"])
    chunk = chunk[chunk["title"].astype(str).str.contains(r"[A-Za-z]", regex=True, na=False)]

    return chunk

def main():
    parts = []
    kept = 0

    for chunk_id, chunk in enumerate(pd.read_csv(IN_PATH, chunksize=CHUNKSIZE)):
        chunk = clean_chunk(chunk)
        if len(chunk) == 0:
            continue

        remaining = TARGET_ROWS - kept
        if remaining <= 0:
            break

        take = min(PER_CHUNK, len(chunk), remaining)
        sampled = chunk.sample(n=take, random_state=SEED + chunk_id)

        parts.append(sampled)
        kept += len(sampled)
        print("Limpias seleccionadas:", kept)

    df = pd.concat(parts, ignore_index=True).sample(frac=1, random_state=SEED)
    df = df.head(TARGET_ROWS)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    df.to_csv(OUT_PATH, index=False)
    print("Guardado:", OUT_PATH, "Filas:", len(df))

if __name__ == "__main__":
    main()
