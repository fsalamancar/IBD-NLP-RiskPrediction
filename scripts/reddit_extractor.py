#Programa para extraer datos volcados de Reddit en formato .zst (zstandard)

#Datos extraidos de:
#1)Volcado 2005-06 to 2024-12
#https://academictorrents.com/details/1614740ac8c94505e4ecb9d88be8bed7b6afddd4
#r/UlcerativeColitis r/IBD r/CrohnsDisease


import os
import json
import csv
import zstandard as zstd
from pathlib import Path

# Carpeta de salida
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # asegura ./output/ [1][3]

SUBREDDITS = {
    "crohnsdisease", "ulcerativecolitis", "ibd",
    "crohnsandcolitis", "crohnsdiseasediet",
    "ulcerativecolitisrdla", "ibddiet", "ulcerativecolitisdiet"
}

FIELDS_SUB = ["id","subreddit","author","title","selftext","created_utc","score","num_comments"]
FIELDS_COM = ["id","subreddit","author","body","created_utc","score","parent_id","link_id"]

def stream_zst_ndjson(path, chunk_size=1<<20):
    dctx = zstd.ZstdDecompressor(max_window_size=2**31)
    with open(path, "rb") as fh, dctx.stream_reader(fh) as reader:
        buf = b""
        while True:
            chunk = reader.read(chunk_size)
            if not chunk:
                break
            buf += chunk
            while True:
                nl = buf.find(b"\n")
                if nl == -1:
                    break
                line = buf[:nl]
                buf = buf[nl+1:]
                if line:
                    yield line
        if buf:
            yield buf

def valid_row(kind, obj):
    if kind == "comment":
        return bool(obj.get("body"))
    else:
        return bool(obj.get("title") or obj.get("selftext"))

def process_file(path, kind, out_csv_name):
    assert kind in ("submission", "comment"), "kind debe ser 'submission' o 'comment'"
    fields = FIELDS_SUB if kind == "submission" else FIELDS_COM

    out_csv = OUTPUT_DIR / out_csv_name  # escribe en ./output/... [1][3]
    exists = out_csv.exists()
    written = 0
    skipped = 0

    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        for raw in stream_zst_ndjson(path):
            try:
                obj = json.loads(raw)
            except Exception:
                skipped += 1
                continue

            sr = obj.get("subreddit")
            if not sr:
                skipped += 1
                continue

            sr_norm = str(sr).lower()
            if sr_norm not in SUBREDDITS:
                continue

            if not valid_row(kind, obj):
                skipped += 1
                continue

            row = {k: obj.get(k) for k in fields}
            w.writerow(row)
            written += 1

    print(f"Archivo: {path} | tipo: {kind} | escritos: {written} | saltados: {skipped} | salida: {out_csv}")








#PPara subreddit CrohnsDisease
process_file("/Users/fjosesala/Documents/GitHub/IBD-NLP-RiskPrediction/data/CrohnsDisease_comments.zst",
             "comment",
             "CD_historical_comments.csv")

process_file("/Users/fjosesala/Documents/GitHub/IBD-NLP-RiskPrediction/data/CrohnsDisease_submissions.zst",
             "submission",
             "CD_historical_submissions.csv")







#Para Subreddit IBD
process_file("/Users/fjosesala/Documents/GitHub/IBD-NLP-RiskPrediction/data/IBD_comments.zst",
             "comment",
             "IBD_historical_comments.csv")

process_file("/Users/fjosesala/Documents/GitHub/IBD-NLP-RiskPrediction/data/IBD_submissions.zst",
             "submission",
             "IBD_historical_submissions.csv")






#Para Subreddit UlcerativeColitis
process_file("/Users/fjosesala/Documents/GitHub/IBD-NLP-RiskPrediction/data/UlcerativeColitis_comments.zst",
             "comment",
             "UC_historical_comments.csv")

process_file("/Users/fjosesala/Documents/GitHub/IBD-NLP-RiskPrediction/data/UlcerativeColitis_submissions.zst",
             "submission",
             "UC_historical_submissions.csv")

