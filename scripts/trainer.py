import argparse, json, os, pathlib, re, time
from collections import deque
from datetime import datetime, timezone

import joblib
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import precision_recall_fscore_support, accuracy_score

MODELS_DIR = pathlib.Path(__file__).resolve().parent.parent / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

def clean_text(s: str) -> str:
    s = s.lower()
    s = re.sub(r"http\S+|www\S+", " ", s)
    s = re.sub(r"[^a-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def vectorizer():
    # HashingVectorizer is stateless & streaming-friendly
    return HashingVectorizer(
        n_features=2**20,
        alternate_sign=False,
        norm=None,
        analyzer="word",
        ngram_range=(1,2),
        preprocessor=clean_text
    )

def make_consumer(bootstrap, topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap],
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="trainer",
        consumer_timeout_ms=10000,
        max_poll_records=500
    )

def make_producer(bootstrap):
    return KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        retries=5
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--in-topic", default="sms_raw")
    ap.add_argument("--out-topic", default="sms_scored")
    ap.add_argument("--metrics-topic", default="sms_metrics")
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--checkpoint-every", type=int, default=1000)
    args = ap.parse_args()

    consumer = make_consumer(args.bootstrap, args.in_topic)
    producer = make_producer(args.bootstrap)

    vec = vectorizer()
    clf = SGDClassifier(loss="log_loss", alpha=1e-5, random_state=42)
    classes = np.array([0,1], dtype=np.int64)
    initialized = False
    seen = 0
    version = 0

    # Buffers for metrics over a sliding window
    y_true_buf, y_pred_buf = deque(maxlen=args.batch_size), deque(maxlen=args.batch_size)

    print("Trainer is running. Ctrl+C to stop.")
    try:
        while True:
            polled = consumer.poll(timeout_ms=1000)
            any_records = False
            for tp, messages in polled.items():
                for m in messages:
                    any_records = True
                    msg = m.value
                    text = msg.get("text","")
                    y_true = int(msg.get("label", 0))

                    X = vec.transform([text])

                    if not initialized:
                        # first call to partial_fit must include 'classes'
                        clf.partial_fit(X, np.array([y_true]), classes=classes)
                        initialized = True
                    else:
                        clf.partial_fit(X, np.array([y_true]))

                    # Predict with current model
                    p_spam = float(clf.predict_proba(X)[0,1])
                    y_pred = int(p_spam >= 0.5)

                    # emit scored record
                    out = {
                        "id": msg.get("id"),
                        "text": text,
                        "y_true": y_true,
                        "y_pred": y_pred,
                        "p_spam": round(p_spam, 6),
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "model_version": f"v{version}"
                    }
                    producer.send(args.out_topic, value=out)

                    # metrics buffer
                    y_true_buf.append(y_true)
                    y_pred_buf.append(y_pred)

                    seen += 1

                    # emit rolling metrics once buffer fills
                    if len(y_true_buf) == args.batch_size:
                        acc = accuracy_score(y_true_buf, y_pred_buf)
                        pr, rc, f1, _ = precision_recall_fscore_support(
                            y_true_buf, y_pred_buf, average="binary", zero_division=0
                        )
                        metrics = {
                            "count_seen": seen,
                            "window_size": args.batch_size,
                            "accuracy": round(float(acc), 6),
                            "precision": round(float(pr), 6),
                            "recall": round(float(rc), 6),
                            "f1": round(float(f1), 6),
                            "model_version": f"v{version}",
                            "ts": datetime.now(timezone.utc).isoformat()
                        }
                        producer.send(args.metrics_topic, value=metrics)
                        print(f"[seen={seen}] acc={acc:.3f} prec={pr:.3f} rec={rc:.3f} f1={f1:.3f} version=v{version}")

                    # checkpoint
                    if seen % max(1, args.checkpoint_every) == 0:
                        version += 1
                        path = MODELS_DIR / f"sgd_spam_{version}.joblib"
                        joblib.dump({"clf": clf, "version": version}, path)
                        print(f"Saved model checkpoint: {path}")

            if not any_records:
                time.sleep(0.2)  # brief idle

    except KeyboardInterrupt:
        print("Stopping trainer...")

if __name__ == "__main__":
    main()
