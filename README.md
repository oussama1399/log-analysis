# Log Analysis (consuming)

This repository contains tools to ingest Mozilla logs, simulate Kafka ingestion, and a small consumer pipeline that cleans logs, does a placeholder anomaly detection, and indexes results into Elasticsearch.

## Structure

- `consuming/` – package that implements the consumer pipeline.
  - `main.py` – entry point (supports `--dry-run` to test without Kafka/ES).
  - `kafka_consumer.py` – wraps `kafka.KafkaConsumer` or falls back to `data/kafka/` files.
  - `es_client.py` – wrapper for Elasticsearch with a safe fallback.
  - `log_cleaner.py` – basic cleaning utilities.
  - `anomaly_detector.py`, `model_loader.py` – placeholder model wiring.
  - `config.py` – default configuration.

## Quick start (local/dry-run)

1. Create and activate a Python virtual environment (optional):

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
```

2. Install optional dependencies (only if you want Kafka/ES integration):

```powershell
pip install -r requirements.txt
```

3. Run the consumer in dry-run mode (will read from `data/kafka/*`):

```powershell
python -m consuming.main --dry-run --limit 50
```

This will process up to 50 messages from your local `data/kafka` dataset and log simulated indexing operations.

## Notes

- The modules are intentionally small and easy to extend. Replace the placeholder detector with your ML model in `model_loader.py` and wire a real Elasticsearch cluster in `es_client.py`.
- `data_ingestion.py` is a separate script to simulate log ingestion (already present in the repo).
