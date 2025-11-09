import argparse, json, os
from kafka import KafkaConsumer

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9094")
    )
    ap.add_argument("--topic", default="sms_metrics")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[args.bootstrap],
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="metrics_printer",
        client_id="metrics_consumer",
    )
    print(f"Listening for metrics on {args.bootstrap} (Ctrl+C to stop).")
    try:
        for m in consumer:
            v = m.value
            print(
                f"[{v.get('ts')}] seen={v.get('count_seen')} win={v.get('window_size')} "
                f"acc={v.get('accuracy')} prec={v.get('precision')} rec={v.get('recall')} f1={v.get('f1')} "
                f"model={v.get('model_version')}"
            )
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
