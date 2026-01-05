import pandas as pd
from pathlib import Path

IN_PATH = Path("data/raw/news/fnspid_headlines.csv")
OUT_PATH = Path("data/processed/news/fnspid_headlines_200k.csv")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

TARGET_ROWS = 200_000
CHUNKSIZE = 1_000_000
PER_CHUNK = 20_000   # subimos para compensar filas que se eliminan por limpieza
SEED = 42

def clean_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    # Normaliza strings y convierte vacíos a NA
    for col in ["date", "title", "ticker", "publisher", "url"]:
        if col in chunk.columns:
            chunk[col] = chunk[col].astype(str).str.strip()
            chunk.loc[chunk[col].isin(["", "None", "nan", "NaN"]), col] = pd.NA

    # Parse de fecha (si viene con hora/UTC también lo entiende)
    chunk["date"] = pd.to_datetime(chunk["date"], errors="coerce")

    # Requisitos mínimos para que sirva
    chunk = chunk.dropna(subset=["date", "title", "url"])

    # Si quieres obligar ticker también, descomenta:
    # chunk = chunk.dropna(subset=["ticker"])

    return chunk

def main():
    parts = []
    kept = 0

    for chunk in pd.read_csv(IN_PATH, chunksize=CHUNKSIZE):
        chunk = clean_chunk(chunk)

        if len(chunk) == 0:
            continue

        remaining = TARGET_ROWS - kept
        if remaining <= 0:
            break

        # Muestra aleatoria del chunk ya limpio
        take = min(PER_CHUNK, len(chunk), remaining)
        sampled = chunk.sample(n=take, random_state=SEED)

        parts.append(sampled)
        kept += len(sampled)

        print("Limpias seleccionadas:", kept)

    df = pd.concat(parts, ignore_index=True).sample(frac=1, random_state=SEED)

    # deja exactamente 200k (por si te pasas por algún ajuste)
    df = df.head(TARGET_ROWS)

    # deja solo la fecha (sin hora) si quieres
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # dedup por url (opcional). Si lo activas, podrías bajar de 200k.
    # df = df.drop_duplicates(subset=["url"]).head(TARGET_ROWS)

    df.to_csv(OUT_PATH, index=False)
    print("Guardado:", OUT_PATH, "Filas:", len(df))

if __name__ == "__main__":
    main()
