import os
import time
from collections import Counter, defaultdict
from datasets import load_dataset
import pandas as pd

MAX_ROWS = 60_000_000
LOG_EVERY = 200_000
CHUNK_SIZE = 100_000
MAX_RETRIES = 10

def main():
    os.makedirs("outputs/eda/ticker_quality_raw", exist_ok=True)
    out_dir = "outputs/eda/ticker_quality_raw"

    for f in os.listdir(out_dir):
        if f.endswith(".parquet"):
            os.remove(os.path.join(out_dir, f))

    total = 0
    missing = 0
    ticker_counts = Counter()
    news_per_day = Counter()
    ticker_day_counts = defaultdict(int)
    valid_rows_buffer = []
    chunk_id = 0
    retries = 0

    while retries < MAX_RETRIES:
        try:
            ds = load_dataset("Zihan1004/FNSPID", split="train", streaming=True)

            # Saltarse las filas ya procesadas
            print(f"Retomando desde fila {total:,}...")
            for i, row in enumerate(ds):
                if i < total:
                    continue  # Saltar hasta donde nos quedamos

                total += 1
                ticker = row.get("Stock_symbol")
                date = row.get("Date")

                if ticker is None or str(ticker).strip() == "":
                    missing += 1
                else:
                    ticker = str(ticker).strip()
                    ticker_counts[ticker] += 1
                    valid_rows_buffer.append(row)

                    if date:
                        date = str(date)[:10]
                        news_per_day[date] += 1
                        ticker_day_counts[(date, ticker)] += 1

                if len(valid_rows_buffer) >= CHUNK_SIZE:
                    df_chunk = pd.DataFrame(valid_rows_buffer)
                    df_chunk.to_parquet(f"{out_dir}/chunk_{chunk_id:04d}.parquet", index=False)
                    chunk_id += 1
                    valid_rows_buffer.clear()

                if total % LOG_EVERY == 0:
                    print(f"Processed {total:,} | missing {missing:,} ({missing/total:.3%})")

                if total >= MAX_ROWS:
                    break

            break  # Si llegó hasta aquí sin error, salimos del while

        except Exception as e:
            retries += 1
            wait = retries * 5
            print(f"\n⚠️  Conexión perdida en fila {total:,}: {e}")
            print(f"Reintentando en {wait}s... (intento {retries}/{MAX_RETRIES})")
            
            # Guardar lo que hay en el buffer antes de reintentar
            if valid_rows_buffer:
                df_chunk = pd.DataFrame(valid_rows_buffer)
                df_chunk.to_parquet(f"{out_dir}/chunk_{chunk_id:04d}.parquet", index=False)
                chunk_id += 1
                valid_rows_buffer.clear()
            
            time.sleep(wait)

    # Guardar el resto
    if valid_rows_buffer:
        df_chunk = pd.DataFrame(valid_rows_buffer)
        df_chunk.to_parquet(f"{out_dir}/chunk_{chunk_id:04d}.parquet", index=False)

    # ---- SUMMARY ----
    print("\n===== SUMMARY =====")
    print("Total:", total)
    print("Missing:", missing)
    print("Missing ratio:", missing / total)
    print("With symbol:", total - missing)
    print("With symbol ratio:", (total - missing) / total)

    print("\nTop tickers:")
    for t, c in ticker_counts.most_common(20):
        print(t, c)

    pd.DataFrame(news_per_day.items(), columns=["date", "news_count"]).to_csv(
        "outputs/eda/news_per_day_raw.csv", index=False
    )
    pd.DataFrame(
        [(d, t, c) for (d, t), c in ticker_day_counts.items()],
        columns=["date", "ticker", "news_count"]
    ).to_csv("outputs/eda/news_per_day_ticker_raw.csv", index=False)

    print(f"\n¡Listo! Chunks guardados en: {out_dir}")

if __name__ == "__main__":
    main()