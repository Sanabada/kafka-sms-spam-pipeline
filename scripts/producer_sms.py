import argparse, json, time, uuid, pathlib, random
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime, timezone

DATA_CSV = pathlib.Path(__file__).resolve().parent.parent / "data" / "sms.csv"

def make_producer(bootstrap):
    return KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        retries=3,
        max_in_flight_requests_per_connection=5,
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="sms_raw")
    ap.add_argument("--rate", type=int, default=20, help="messages per second")
    ap.add_argument("--shuffle", action="store_true", help="shuffle rows before streaming")
    args = ap.parse_args()

    df = pd.read_csv(DATA_CSV)
    df = df.dropna(subset=["text","label"])
    # map to {ham:0, spam:1}
    df["label"] = (df["label"].str.lower() == "spam").astype(int)

    if args.shuffle:
        df = df.sample(frac=1.0, random_state=42).reset_index(drop=True)

    prod = make_producer(args.bootstrap)

    delay = 1.0 / max(1, args.rate)
    sent = 0

    print(f"Streaming {len(df)} messages to topic '{args.topic}' at ~{args.rate}/s ... Ctrl+C to stop")
    for _, row in df.iterrows():
        msg = {
            "id": str(uuid.uuid4()),
            "text": str(row["text"]),
            "label": int(row["label"]),
            "ts": datetime.now(timezone.utc).isoformat()
        }
        prod.send(args.topic, value=msg)
        sent += 1
        if sent % 100 == 0:
            prod.flush()
            print(f"Sent: {sent}")
        time.sleep(delay)
    prod.flush()
    print(f"Done. Sent {sent} messages.")

if __name__ == "__main__":
    main()
