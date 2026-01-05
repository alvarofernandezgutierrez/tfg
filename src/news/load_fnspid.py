from datasets import load_dataset
from pathlib import Path
import csv

OUT = Path("data/raw/news")
OUT.mkdir(parents=True, exist_ok=True)

def main():
    ds = load_dataset("Zihan1004/FNSPID", split="train", streaming=True)

    MAX_ROWS = 100000000  # prueba
    out_path = OUT / "fnspid_headlines.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date", "title", "ticker", "publisher", "url"])
        w.writeheader()

        for i, row in enumerate(ds, start=1):
            w.writerow({
                "date": row.get("Date"),
                "title": row.get("Article_title"),
                "ticker": row.get("Stock_symbol"),
                "publisher": row.get("Publisher"),
                "url": row.get("Url"),
            })

            if i % 50_000 == 0:
                print("Leídas:", i)

            if i >= MAX_ROWS:
                break

if __name__ == "__main__":
    main()
