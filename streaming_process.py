import json
import logging
import os
import re
import time
import threading
from typing import Dict, Tuple, Optional

import torch
import torch.nn as nn
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from elasticsearch import Elasticsearch

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==========================
# Configuration
# ==========================
CHECKPOINT_DIR = "checkpoints"
MODEL_FILE = "model.pth"
VOCAB_FILE = os.path.join(CHECKPOINT_DIR, "vocab_full.json")

TOPIC_NAME = "mozilla-logs"
BOOTSTRAP_SERVERS = ["localhost:9092"]
ES_HOSTS = [{"host": "localhost", "port": 9200, "scheme": "http"}]
ES_INDEX = "mozilla-clogs-index"

MAX_LEN = 128
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
ANOMALY_THRESHOLD = None  # set to a float to flag anomalies, otherwise scores only

PATTERNS_PY = {
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z": "[TIMESTAMP_ISO]",
    r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}": "[TIMESTAMP]",
    r"PID\s+\d+": "PID [PID_NUM]",
    r"\[\d+\]": "[ID]",
    r"(?:\d{1,3}\.){3}\d{1,3}": "[IP_ADDR]",
    r"\s+": " "
}


class RunningStats:
    """Online mean/std using Welford's algorithm."""

    def __init__(self):
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0

    def update(self, x: float) -> Tuple[float, float]:
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        self.m2 += delta * (x - self.mean)
        variance = self.m2 / (self.n - 1) if self.n > 1 else 0.0
        std = variance ** 0.5
        return self.mean, std

# ==========================
# Model definition
# ==========================
class LogAutoEncoder(nn.Module):
    def __init__(self, vocab_size: int, embed_dim: int = 48, latent_dim: int = 24, max_len: int = 128):
        super().__init__()
        self.embedding = nn.Embedding(vocab_size, embed_dim, padding_idx=0)
        self.encoder = nn.LSTM(embed_dim, latent_dim, batch_first=True, num_layers=1)
        self.decoder = nn.LSTM(latent_dim, embed_dim, batch_first=True, num_layers=1)
        self.output_linear = nn.Linear(embed_dim, vocab_size)
        self.max_len = max_len
        self.embed_dim = embed_dim
        self.latent_dim = latent_dim

    def forward(self, x):
        embedded = self.embedding(x)
        _, (h_n, c_n) = self.encoder(embedded)
        latent = h_n.squeeze(0)
        latent_repeated = latent.unsqueeze(1).expand(-1, self.max_len, -1)
        h0 = torch.zeros(1, x.size(0), self.embed_dim, device=x.device)
        c0 = torch.zeros(1, x.size(0), self.embed_dim, device=x.device)
        decoded, _ = self.decoder(latent_repeated, (h0, c0))
        reconstruction = self.output_linear(decoded)
        return reconstruction, latent

# ==========================
# Helpers
# ==========================
def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    for pat, repl in PATTERNS_PY.items():
        text = re.sub(pat, repl, text)
    return text

def text_to_ids(text: str, vocab: Dict[str, int], unk_id: int, pad_id: int, max_len: int) -> list:
    if not isinstance(text, str):
        return [pad_id] * max_len
    tokens = text.split()
    ids = [vocab.get(tok, unk_id) for tok in tokens]
    if len(ids) > max_len:
        ids = ids[:max_len]
    else:
        ids += [pad_id] * (max_len - len(ids))
    return ids

def calculate_reconstruction_loss(model: LogAutoEncoder, input_ids: list, vocab_size: int, criterion, device) -> Tuple[float, torch.Tensor]:
    model.eval()
    with torch.no_grad():
        input_tensor = torch.tensor(input_ids, dtype=torch.long, device=device).unsqueeze(0)
        reconstruction, latent = model(input_tensor)
        loss = criterion(reconstruction.view(-1, vocab_size), input_tensor.view(-1))
    return loss.item(), latent

def load_model_and_vocab() -> Tuple[LogAutoEncoder, Dict, int, int, int]:
    with open(VOCAB_FILE, "r") as f:
        vocab_data = json.load(f)
    vocab = vocab_data["vocab"]
    vocab_size = vocab_data["size"]
    unk_id = vocab[vocab_data["unk_token"]]
    pad_id = vocab[vocab_data["pad_token"]]

    model = LogAutoEncoder(vocab_size=vocab_size, max_len=MAX_LEN).to(DEVICE)
    checkpoint = torch.load(MODEL_FILE, map_location=DEVICE)
    if isinstance(checkpoint, dict) and "model_state_dict" in checkpoint:
        model.load_state_dict(checkpoint["model_state_dict"])
    else:
        model.load_state_dict(checkpoint)

    criterion = nn.CrossEntropyLoss(ignore_index=pad_id)
    logger.info("Model and vocab loaded")
    return model, vocab, vocab_size, unk_id, pad_id, criterion

# ==========================
# Kafka topic management
# ==========================
def ensure_topic_exists(topic_name: str, num_partitions: int = 1, replication_factor: int = 1, bootstrap_servers=BOOTSTRAP_SERVERS):
    if isinstance(bootstrap_servers, list):
        bootstrap_servers = ",".join(bootstrap_servers)

    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    try:
        logger.info("Creating topic %s", topic_name)
        admin_client.create_topics([topic], validate_only=False)
        logger.info("Topic %s created", topic_name)
    except TopicAlreadyExistsError:
        logger.info("Topic %s already exists; recreating", topic_name)
        try:
            admin_client.delete_topics([topic_name])
            logger.info("Deleted old topic %s", topic_name)
        except UnknownTopicOrPartitionError:
            logger.warning("Topic %s disappeared before deletion", topic_name)
        admin_client.create_topics([topic], validate_only=False)
        logger.info("Topic %s recreated", topic_name)
    finally:
        admin_client.close()


def clear_topic_and_index(topic_name: str = TOPIC_NAME, es_hosts=ES_HOSTS, es_index: str = ES_INDEX, bootstrap_servers=BOOTSTRAP_SERVERS):
    # Delete topic if exists
    if isinstance(bootstrap_servers, list):
        bs = ",".join(bootstrap_servers)
    else:
        bs = bootstrap_servers

    admin_client = KafkaAdminClient(bootstrap_servers=bs)
    try:
        admin_client.delete_topics([topic_name])
        logger.info("Topic %s deleted", topic_name)
    except Exception as e:
        logger.warning("Could not delete topic %s: %s", topic_name, e)
    finally:
        admin_client.close()

    # Delete Elasticsearch index if exists
    es = Elasticsearch(es_hosts)
    try:
        es.indices.delete(index=es_index, ignore=[404])
        logger.info("Elasticsearch index %s deleted", es_index)
    except Exception as e:
        logger.warning("Could not delete ES index %s: %s", es_index, e)

# ==========================
# Producer: stream log files into Kafka
# ==========================
def simulate_log_ingestion(base_dir: str = "data/kafka", topic_name: str = TOPIC_NAME, bootstrap_servers=BOOTSTRAP_SERVERS, delay_seconds: float = 0.01, batch_size: int = 1000, add_timestamp: bool = True):
    logger.info("Starting ingestion from %s", base_dir)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        batch_size=16384,
        linger_ms=500,
    )
    try:
        log_dirs = [d for d in os.listdir(base_dir) if d.startswith("log-")]
        log_dirs.sort()
        for log_dir in log_dirs:
            dir_path = os.path.join(base_dir, log_dir)
            if not os.path.isdir(dir_path):
                continue
            files = [f for f in os.listdir(dir_path) if f.endswith(".txt")]
            files.sort()
            for filename in files:
                file_path = os.path.join(dir_path, filename)
                with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                    batch = []
                    for line_num, line in enumerate(f, start=1):
                        line = line.rstrip("\n")
                        if not line.strip():
                            continue
                        msg = {
                            "raw_log": line,
                            "source_file": filename,
                            "source_directory": log_dir,
                            "line_number": line_num,
                        }
                        if add_timestamp:
                            msg["timestamp"] = time.time()
                        batch.append(msg)
                        if len(batch) >= batch_size:
                            for m in batch:
                                producer.send(topic_name, value=m)
                            producer.flush()
                            batch.clear()
                        time.sleep(delay_seconds)
                    if batch:
                        for m in batch:
                            producer.send(topic_name, value=m)
                        producer.flush()
    finally:
        producer.close()
        logger.info("Kafka producer closed")

# ==========================
# Consumer: score and push to Elasticsearch
# ==========================
def consume_score_and_index(topic_name: str = TOPIC_NAME, bootstrap_servers=BOOTSTRAP_SERVERS, es_hosts=ES_HOSTS, es_index: str = ES_INDEX, anomaly_threshold: Optional[float] = ANOMALY_THRESHOLD, factor: float = 3.0, status_every: int = 500, quiet: bool = False):
    model, vocab, vocab_size, unk_id, pad_id, criterion = load_model_and_vocab()
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="log_analysis_group",
    )
    es = Elasticsearch(es_hosts)

    if not quiet:
        logger.info("Consumer started; scoring and indexing")
    processed = 0
    anomalies = 0
    stats = RunningStats()
    try:
        for message in consumer:
            payload = message.value
            raw_log = payload.get("raw_log", "")
            cleaned = clean_text(raw_log)
            ids = text_to_ids(cleaned, vocab, unk_id, pad_id, MAX_LEN)
            loss, _ = calculate_reconstruction_loss(model, ids, vocab_size, criterion, DEVICE)

            mean, std = stats.update(loss)
            dynamic_threshold = None
            if anomaly_threshold is not None:
                dynamic_threshold = anomaly_threshold
            elif stats.n > 1:
                dynamic_threshold = mean + factor * std

            doc = {
                "raw_log": raw_log,
                "cleaned_log": cleaned,
                "source_file": payload.get("source_file", "unknown"),
                "source_directory": payload.get("source_directory", "unknown"),
                "line_number": payload.get("line_number", -1),
                "timestamp_received": payload.get("timestamp"),
                "anomaly_score": loss,
            }
            is_anomaly = False
            if dynamic_threshold is not None:
                is_anomaly = loss > dynamic_threshold
                doc["is_anomaly"] = is_anomaly
                doc["threshold_used"] = dynamic_threshold
                doc["mean_score"] = mean
                doc["std_score"] = std
                anomalies += int(is_anomaly)

            processed += 1


            try:
                es.index(index=es_index, body=doc)
            except Exception as e:
                logger.error("Elasticsearch index error: %s", e)

            if status_every and processed % status_every == 0:
                msg = f"Processed {processed} messages"
                if dynamic_threshold is not None:
                    msg += f" | anomalies flagged: {anomalies}"
                print(msg)
    except KeyboardInterrupt:
        if not quiet:
            logger.info("Consumer interrupted by user")
    finally:
        consumer.close()
        if not quiet:
            logger.info("Kafka consumer closed")


def stream_all(base_dir: str = "data/kafka", delay_seconds: float = 0.01, batch_size: int = 1000, add_timestamp: bool = True, anomaly_threshold: Optional[float] = None, factor: float = 3.0):
    ensure_topic_exists(TOPIC_NAME, num_partitions=1, replication_factor=1, bootstrap_servers=BOOTSTRAP_SERVERS)

    consumer_thread = threading.Thread(
        target=consume_score_and_index,
        kwargs={
            "topic_name": TOPIC_NAME,
            "bootstrap_servers": BOOTSTRAP_SERVERS,
            "es_hosts": ES_HOSTS,
            "es_index": ES_INDEX,
            "anomaly_threshold": anomaly_threshold,
            "factor": factor,
            "status_every": 200,
            "quiet": True,
        },
        daemon=True,
    )
    consumer_thread.start()

    print("[stream] Topic ready; consumer running. Starting ingestion...")
    simulate_log_ingestion(
        base_dir=base_dir,
        topic_name=TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        delay_seconds=delay_seconds,
        batch_size=batch_size,
        add_timestamp=add_timestamp,
    )
    print("[stream] Ingestion finished. Consumer continues to index. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[stream] Stopping stream.")

# ==========================
# Main entrypoints
# ==========================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Kafka streaming with anomaly scoring to Elasticsearch")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_topic = sub.add_parser("topic", help="Ensure topic exists (recreate)")

    p_ingest = sub.add_parser("ingest", help="Stream log files into Kafka")
    p_ingest.add_argument("--base-dir", default="data/kafka")
    p_ingest.add_argument("--delay", type=float, default=0.01)
    p_ingest.add_argument("--batch", type=int, default=1000)
    p_ingest.add_argument("--no-timestamp", action="store_true")

    p_consume = sub.add_parser("consume", help="Consume, score, and index to Elasticsearch")
    p_consume.add_argument("--threshold", type=float, default=None, help="Anomaly threshold; if omitted, dynamic threshold is used")
    p_consume.add_argument("--factor", type=float, default=3.0, help="Multiplier for std when using dynamic threshold (mean + factor*std)")

    p_stream = sub.add_parser("stream", help="Ensure topic, ingest, consume, and show minimal status")
    p_stream.add_argument("--base-dir", default="data/kafka")
    p_stream.add_argument("--delay", type=float, default=0.01)
    p_stream.add_argument("--batch", type=int, default=1000)
    p_stream.add_argument("--no-timestamp", action="store_true")
    p_stream.add_argument("--threshold", type=float, default=None, help="Fixed anomaly threshold; if omitted, dynamic threshold is used")
    p_stream.add_argument("--factor", type=float, default=3.0, help="Multiplier for std when using dynamic threshold (mean + factor*std)")

    p_clear = sub.add_parser("clear", help="Delete Kafka topic and Elasticsearch index")

    args = parser.parse_args()

    if args.cmd == "topic":
        ensure_topic_exists(TOPIC_NAME, num_partitions=1, replication_factor=1, bootstrap_servers=BOOTSTRAP_SERVERS)
    elif args.cmd == "ingest":
        simulate_log_ingestion(base_dir=args.base_dir, topic_name=TOPIC_NAME, bootstrap_servers=BOOTSTRAP_SERVERS, delay_seconds=args.delay, batch_size=args.batch, add_timestamp=not args.no_timestamp)
    elif args.cmd == "consume":
        consume_score_and_index(topic_name=TOPIC_NAME, bootstrap_servers=BOOTSTRAP_SERVERS, es_hosts=ES_HOSTS, es_index=ES_INDEX, anomaly_threshold=args.threshold, factor=args.factor)
    elif args.cmd == "stream":
        stream_all(
            base_dir=args.base_dir,
            delay_seconds=args.delay,
            batch_size=args.batch,
            add_timestamp=not args.no_timestamp,
            anomaly_threshold=args.threshold,
            factor=args.factor,
        )
    elif args.cmd == "clear":
        clear_topic_and_index(topic_name=TOPIC_NAME, es_hosts=ES_HOSTS, es_index=ES_INDEX, bootstrap_servers=BOOTSTRAP_SERVERS)
