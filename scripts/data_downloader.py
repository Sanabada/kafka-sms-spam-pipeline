# scripts/data_downloader.py
import io, zipfile, pathlib, requests, pandas as pd

DATA_DIR = pathlib.Path(__file__).resolve().parents[1] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ZIP_URL = "https://cdn.uci-ics-mlr-prod.aws.uci.edu/228/sms%2Bspam%2Bcollection.zip"  # UCI's new download
RAW_TXT = DATA_DIR / "smsspamcollection.txt"
CSV_OUT = DATA_DIR / "sms.csv"

print(f"Downloading ZIP from UCI to memory ...")
r = requests.get(ZIP_URL, timeout=60)
r.raise_for_status()

with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
    raw_bytes = zf.read("SMSSpamCollection")  # inside the ZIP
RAW_TXT.write_bytes(raw_bytes)
print(f"Saved {RAW_TXT}")

# Also emit a CSV the rest of the pipeline can read
import io as _io
df = pd.read_csv(_io.StringIO(raw_bytes.decode("utf-8", "ignore")),
                 sep="\t", header=None, names=["label", "text"])
df.to_csv(CSV_OUT, index=False)
print(f"Saved {CSV_OUT} ({len(df)} rows)")
