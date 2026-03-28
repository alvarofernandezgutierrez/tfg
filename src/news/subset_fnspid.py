import pandas as pd
from pathlib import Path

IN_PATH = Path("data/raw/news/fnspid_headlines.csv")
OUT_PATH = Path("data/processed/news/fnspid_headlines_2m.csv")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

TARGET_ROWS = 2_000_000
CHUNKSIZE = 1_000_000
PER_CHUNK = 50_000
SEED = 42

COLS = ["date", "title", "ticker", "publisher", "url"]

def clean_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    for col in COLS:
        if col in chunk.columns:
            chunk[col] = chunk[col].astype(str).str.strip()
            chunk.loc[chunk[col].isin(["", "None", "nan", "NaN"]), col] = pd.NA

    if "date" in chunk.columns:
        chunk["date"] = pd.to_datetime(chunk["date"], errors="coerce")

    needed = [c for c in ["date", "title", "url"] if c in chunk.columns]
    chunk = chunk.dropna(subset=needed)

    if "title" in chunk.columns:
        chunk = chunk[chunk["title"].astype(str).str.contains(r"[A-Za-z]", regex=True, na=False)]

    return chunk

def main():
    kept = 0

    if OUT_PATH.exists():
        OUT_PATH.unlink()

    for chunk_id, chunk in enumerate(pd.read_csv(IN_PATH, chunksize=CHUNKSIZE)):
        chunk = clean_chunk(chunk)
        if len(chunk) == 0:
            continue

        remaining = TARGET_ROWS - kept
        if remaining <= 0:
            break

        take = min(PER_CHUNK, len(chunk), remaining)
        sampled = chunk.sample(n=take, random_state=SEED + chunk_id).copy()

        if "date" in sampled.columns:
            sampled["date"] = pd.to_datetime(sampled["date"], errors="coerce").dt.date

        present_cols = [c for c in COLS if c in sampled.columns]
        sampled = sampled[present_cols]

        header = (kept == 0)
        sampled.to_csv(OUT_PATH, index=False, mode="a", header=header)

        kept += len(sampled)
        print(f"Chunk {chunk_id}: añadidas {len(sampled)} | total {kept}/{TARGET_ROWS}")

    print("Guardado:", OUT_PATH, "Filas:", kept)

if __name__ == "__main__":
    main()
