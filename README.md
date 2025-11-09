# Real-time SMS Spam Streaming ML with Apache Kafka (Python)

This project streams the **UCI SMS Spam Collection** dataset through Kafka, **trains an online logistic regression model in real time** with `partial_fit`, and produces per-message predictions plus rolling metrics. It’s designed to satisfy a Level 3 (Advanced) pipeline with real-time ML.

## Architecture
- **Producer (`producer_sms.py`)** reads labeled SMS messages and publishes JSON to `sms_raw`.
- **Trainer/Scorer (`trainer.py`)** consumes from `sms_raw`, performs text cleaning + HashingVectorizer, **online-trains** an SGDClassifier (`log_loss`) via `partial_fit`, emits predictions to `sms_scored`, and rolling metrics to `sms_metrics`. It also **checkpoints** the model periodically in `./models/`.
- **Metrics Viewer (`metrics_consumer.py`)** prints streaming metrics from `sms_metrics`.

Docker spins up: **Zookeeper, Kafka (Bitnami), Kafdrop**.

## Quickstart

### 0) Prereqs
- Docker Desktop (or Docker Engine) + Compose
- Python 3.10+

### 1) Start Kafka
```bash
docker compose up -d
# Kafdrop UI at http://localhost:19000
```

### 2) Create a virtualenv & install deps
```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3) Download the dataset
```bash
python scripts/data_downloader.py
# writes ./data/smsspamcollection.txt and ./data/sms.csv
```

### 4) Run the pipeline (3 terminals)
**Terminal A – Producer:**
```bash
python scripts/producer_sms.py --rate 40
```
**Terminal B – Trainer/Scorer:**
```bash
python scripts/trainer.py --batch-size 200 --checkpoint-every 1000
```
**Terminal C – Metrics viewer:**
```bash
python scripts/metrics_consumer.py
```

### 5) Optional: Explore topics in Kafdrop
- Open http://localhost:19000 → topics `sms_raw`, `sms_scored`, `sms_metrics`

## Topics & Schemas
- **sms_raw** (input): 
```json
{
  "id": "uuid",
  "text": "string",
  "label": 0 or 1,  # ham=0, spam=1 (training only)
  "ts": "ISO-8601"
}
```
- **sms_scored** (output, per message):
```json
{
  "id": "uuid",
  "text": "string",
  "y_true": 0 or 1,
  "y_pred": 0 or 1,
  "p_spam": float in [0,1],
  "ts": "ISO-8601",
  "model_version": "v{int}"
}
```
- **sms_metrics** (output, per batch):
```json
{
  "count_seen": int,
  "window_size": int,
  "accuracy": float,
  "precision": float,
  "recall": float,
  "f1": float,
  "model_version": "v{int}",
  "ts": "ISO-8601"
}
```

## Notes
- Kafka auto-creates topics because the container sets `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true`.
- If you want to slow down or speed up the stream: `--rate N` on the producer.
- The model is **online-trained**, so metrics will evolve as data arrives. Checkpoints appear under `./models/`.
- To stop all containers: `docker compose down`.

