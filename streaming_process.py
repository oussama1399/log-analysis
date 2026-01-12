import json
import logging
import os
import re
import time
import threading
from collections import deque, Counter
from math import sqrt
from typing import Dict, Tuple, Optional

import requests

import torch
import torch.nn as nn
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from sentence_transformers import SentenceTransformer

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
ANALYZED_INDEX = "analyzed_anomalies"

MAX_LEN = 128
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
ANOMALY_THRESHOLD = None  # set to a float to flag anomalies, otherwise scores only
STRICT_THRESHOLD_DEFAULT = 3.5
CONTEXT_K = 3
NORMAL_BUFFER_MAX = 50000

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


class SimpleDrain:
    """Simplified Drain algorithm for log template extraction."""

    def __init__(self, similarity_threshold=0.5, max_depth=4):
        self.similarity_threshold = similarity_threshold
        self.max_depth = max_depth
        self.templates = {}  # template_id -> template_tokens
        self.template_counter = 0

    def tokenize(self, log_text: str):
        """Split log into tokens."""
        return log_text.split()

    def get_similarity(self, tokens1, tokens2):
        """Calculate similarity between two token sequences."""
        if len(tokens1) != len(tokens2):
            return 0.0
        matches = sum(1 for t1, t2 in zip(tokens1, tokens2) if t1 == t2)
        return matches / len(tokens1) if len(tokens1) > 0 else 0.0

    def find_best_template(self, tokens):
        """Find the best matching template for given tokens."""
        best_template_id = None
        best_similarity = 0.0

        token_len = len(tokens)
        for template_id, template_tokens in self.templates.items():
            if len(template_tokens) == token_len:
                similarity = self.get_similarity(tokens, template_tokens)
                if similarity >= self.similarity_threshold and similarity > best_similarity:
                    best_similarity = similarity
                    best_template_id = template_id

        return best_template_id, best_similarity

    def create_template(self, tokens1, tokens2):
        """Merge two token sequences into a template with wildcards."""
        template = []
        for t1, t2 in zip(tokens1, tokens2):
            if t1 == t2:
                template.append(t1)
            else:
                template.append("<*>")  # Wildcard for variable parts
        return template

    def extract_parameters(self, tokens, template_tokens):
        """Extract variable parameters from tokens using template."""
        parameters = []
        for token, template_token in zip(tokens, template_tokens):
            if template_token == "<*>":
                parameters.append(token)
        return parameters

    def parse(self, log_text: str):
        """Parse a log line and return (template_id, template_str, parameters)."""
        tokens = self.tokenize(log_text)

        if not tokens:
            return None, "", []

        template_id, similarity = self.find_best_template(tokens)

        if template_id is not None:
            template_tokens = self.templates[template_id]
            parameters = self.extract_parameters(tokens, template_tokens)
            template_str = " ".join(template_tokens)
            return template_id, template_str, parameters
        else:
            self.template_counter += 1
            template_id = self.template_counter
            self.templates[template_id] = tokens.copy()
            template_str = " ".join(tokens)
            parameters = []
            return template_id, template_str, parameters

    def update_template(self, template_id, new_tokens):
        """Update an existing template by merging with new tokens."""
        if template_id in self.templates:
            old_template = self.templates[template_id]
            merged_template = self.create_template(old_template, new_tokens)
            self.templates[template_id] = merged_template


def tokenize_simple(text: str):
    return re.findall(r"[a-zA-Z0-9_]+", text.lower())


def bow_counter(text: str) -> Counter:
    return Counter(tokenize_simple(text))


def cosine_counter(a: Counter, b: Counter) -> float:
    if not a or not b:
        return 0.0
    # Dot product
    keys = a.keys() & b.keys()
    dot = sum(a[k] * b[k] for k in keys)
    na = sqrt(sum(v * v for v in a.values()))
    nb = sqrt(sum(v * v for v in b.values()))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _ollama_url() -> str:
    host = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434")
    if not host.startswith("http"):
        host = "http://" + host
    return host.rstrip("/") + "/api/generate"


def check_ollama_available(model: str = "gpt-oss:20b", timeout: int = 15) -> bool:
    url = _ollama_url()
    try:
        resp = requests.post(
            url,
            json={"model": model, "prompt": "ping", "stream": False},
            timeout=timeout,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error("LLM not available at %s: %s", url, e)
        return False


def ollama_llm_analysis(
    anomaly_log: str,
    contexts: list,
    model: str = "gpt-oss:20b",
    timeout: int = 120,
) -> Dict:
    url = _ollama_url()

    prompt_lines = [
        "You are a log analysis assistant.",
        "Given one anomalous log line and up to three normal context lines, return JSON only.",
        "JSON schema: {\"llm_class\": string, \"llm_root_cause\": string, \"llm_suggested_fix\": string, \"llm_confidence\": number between 0 and 1}.",
        "If you are unsure, set llm_class to 'needs_review', llm_confidence to 0, and provide a brief root_cause explaining uncertainty.",
        "",
        "Anomaly:",
        anomaly_log,
        "",
        "Contexts:",
    ]
    for i, ctx in enumerate(contexts[:3], 1):
        prompt_lines.append(f"{i}. {ctx.get('cleaned_log', '')}")
    prompt_lines.append("")
    prompt_lines.append("Return JSON only:")
    prompt_str = "\n".join(prompt_lines)

    default_resp = {
        "llm_class": "needs_review",
        "llm_root_cause": "llm_error",
        "llm_suggested_fix": "Review manually.",
        "llm_confidence": 0.0,
    }

    try:
        resp = requests.post(
            url,
            json={
                "model": model,
                "prompt": prompt_str,
                "stream": False,
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data.get("response", "").strip()
        parsed = None
        if text:
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = None
        if isinstance(parsed, dict):
            return {
                "llm_class": parsed.get("llm_class", default_resp["llm_class"]),
                "llm_root_cause": parsed.get("llm_root_cause", default_resp["llm_root_cause"]),
                "llm_suggested_fix": parsed.get("llm_suggested_fix", default_resp["llm_suggested_fix"]),
                "llm_confidence": float(parsed.get("llm_confidence", default_resp["llm_confidence"])),
            }
        fallback = default_resp.copy()
        fallback["llm_root_cause"] = "llm_unparsed"
        fallback["raw"] = text[:500]
        return fallback
    except Exception as e:
        logger.warning("LLM call failed: %s", e)
        fallback = default_resp.copy()
        fallback["llm_root_cause"] = "llm_exception"
        fallback["error"] = str(e)
        return fallback

def topk_similar(normals: deque, query_cleaned: str, k: int = CONTEXT_K):
    query_vec = bow_counter(query_cleaned)
    scored = []
    for item in normals:
        score = cosine_counter(query_vec, item["bow"])
        scored.append((score, item))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [itm for _, itm in scored[:k]]

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


def clear_analysis_index(es_hosts=ES_HOSTS, analyzed_index: str = ANALYZED_INDEX):
    es = Elasticsearch(es_hosts)
    try:
        es.indices.delete(index=analyzed_index, ignore=[404])
        logger.info("Elasticsearch index %s deleted", analyzed_index)
    except Exception as e:
        logger.warning("Could not delete ES index %s: %s", analyzed_index, e)

# ==========================
# Elasticsearch index helpers
# ==========================
def ensure_es_index_with_timestamp(es: Elasticsearch, index_name: str):
    """Ensure the given index exists with '@timestamp' mapped as a date using epoch_millis.

    If the index exists, attempt to put the mapping for '@timestamp' if missing.
    """
    try:
        exists = es.indices.exists(index=index_name)
    except Exception as e:
        logger.error("ES exists check failed for %s: %s", index_name, e)
        exists = False

    base_mappings = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date", "format": "epoch_millis"},
                "timestamp_received": {"type": "date", "format": "epoch_millis"},
                "raw_log": {"type": "text"},
                "cleaned_log": {"type": "text"},
                "template_id": {"type": "integer"},
                "template": {"type": "text"},
                "parameters": {"type": "keyword"},
                "num_parameters": {"type": "integer"},
                "source_file": {"type": "keyword"},
                "source_directory": {"type": "keyword"},
                "line_number": {"type": "integer"},
                "anomaly_score": {"type": "float"},
                "is_anomaly": {"type": "boolean"},
                "threshold_used": {"type": "float"},
                "mean_score": {"type": "float"},
                "std_score": {"type": "float"},
                "strict_threshold": {"type": "float"},
                "context_logs": {"type": "text"},
                "context_templates": {"type": "text"},
                "analysis": {"type": "object", "enabled": True},
            }
        }
    }

    if not exists:
        try:
            es.indices.create(index=index_name, **base_mappings)
            logger.info("Created ES index %s with timestamp mapping", index_name)
            return
        except Exception as e:
            logger.warning("Create index %s failed (may already exist): %s", index_name, e)

    # Try to update mapping (idempotent if already present)
    try:
        es.indices.put_mapping(index=index_name, body=base_mappings["mappings"]) 
        logger.info("Updated mapping for index %s", index_name)
    except Exception as e:
        logger.warning("Put mapping failed for %s: %s", index_name, e)

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
                            # Epoch millis for ES '@timestamp' compatibility
                            msg["timestamp"] = int(round(time.time() * 1000))
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
    # Ensure ES index exists with proper timestamp mapping
    ensure_es_index_with_timestamp(es, es_index)
    # Ensure vector index exists
    VECTOR_INDEX = "mozillalogs_vectorized"
    VECTOR_DIM = 384
    if not es.indices.exists(index=VECTOR_INDEX):
        es.indices.create(
            index=VECTOR_INDEX,
            body={
                "mappings": {
                    "properties": {
                        "raw_log": {"type": "text"},
                        "cleaned_log": {"type": "text"},
                        "embedding": {"type": "dense_vector", "dims": VECTOR_DIM},
                        # ...other fields as needed
                    }
                }
            }
        )
    # Load embedding model
    embedder = SentenceTransformer('all-MiniLM-L6-v2')
    drain_parser = SimpleDrain(similarity_threshold=0.5)

    if not quiet:
        logger.info("Consumer started; scoring and indexing with Drain")
    processed = 0
    anomalies = 0
    stats = RunningStats()
    try:
        for message in consumer:
            payload = message.value
            raw_log = payload.get("raw_log", "")
            cleaned = clean_text(raw_log)
            
            # DRAIN: Extract template and parameters
            template_id, template_str, parameters = drain_parser.parse(cleaned)
            
            # Use template for anomaly detection
            ids = text_to_ids(template_str, vocab, unk_id, pad_id, MAX_LEN)
            loss, _ = calculate_reconstruction_loss(model, ids, vocab_size, criterion, DEVICE)

            mean, std = stats.update(loss)
            dynamic_threshold = None
            if anomaly_threshold is not None:
                dynamic_threshold = anomaly_threshold
            elif stats.n > 1:
                dynamic_threshold = mean + factor * std

            # Normalize timestamp to epoch millis
            ts_payload = payload.get("timestamp")
            if isinstance(ts_payload, (int, float)):
                # if seconds float, convert to millis
                if ts_payload < 10_000_000_000:  # less than year ~2286 seconds threshold
                    ts_millis = int(round(ts_payload * 1000))
                else:
                    ts_millis = int(ts_payload)
            else:
                ts_millis = int(round(time.time() * 1000))

            doc = {
                "raw_log": raw_log,
                "cleaned_log": cleaned,
                "template_id": template_id,
                "template": template_str,
                "parameters": parameters,
                "num_parameters": len(parameters),
                "source_file": payload.get("source_file", "unknown"),
                "source_directory": payload.get("source_directory", "unknown"),
                "line_number": payload.get("line_number", -1),
                "timestamp_received": ts_millis,
                "@timestamp": ts_millis,
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



            # Index in classic index
            try:
                es.index(index=es_index, body=doc)
            except Exception as e:
                logger.error("Elasticsearch index error: %s", e)

            # Vectorize and index in vector index
            try:
                emb = embedder.encode([raw_log])[0]
                vector_doc = doc.copy()
                vector_doc["embedding"] = emb.tolist()
                es.index(index=VECTOR_INDEX, body=vector_doc)
            except Exception as e:
                logger.error("Elasticsearch vector index error: %s", e)

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


def consume_analyze(topic_name: str = TOPIC_NAME, bootstrap_servers=BOOTSTRAP_SERVERS, es_hosts=ES_HOSTS, analyzed_index: str = ANALYZED_INDEX, strict_threshold: float = STRICT_THRESHOLD_DEFAULT, context_k: int = CONTEXT_K, max_normals: int = NORMAL_BUFFER_MAX):
    if not check_ollama_available():
        print("[analyze] LLM unavailable; stopping.")
        return

    model, vocab, vocab_size, unk_id, pad_id, criterion = load_model_and_vocab()
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="log_analysis_analyze_group",
    )
    es = Elasticsearch(es_hosts)
    # Ensure analyzed index exists with proper timestamp mapping
    ensure_es_index_with_timestamp(es, analyzed_index)
    drain_parser = SimpleDrain(similarity_threshold=0.5)

    normals = deque(maxlen=max_normals)
    processed = 0
    anomalies = 0
    print(f"[analyze] Running with Drain + strict threshold={strict_threshold}, context_k={context_k}, normals buffer={max_normals}")
    try:
        for message in consumer:
            payload = message.value
            raw_log = payload.get("raw_log", "")
            cleaned = clean_text(raw_log)
            
            # DRAIN: Extract template and parameters
            template_id, template_str, parameters = drain_parser.parse(cleaned)
            
            # Use template for anomaly detection
            ids = text_to_ids(template_str, vocab, unk_id, pad_id, MAX_LEN)
            loss, _ = calculate_reconstruction_loss(model, ids, vocab_size, criterion, DEVICE)

            processed += 1

            if loss <= strict_threshold:
                normals.append({
                    "cleaned_log": cleaned,
                    "raw_log": raw_log,
                    "template_id": template_id,
                    "template": template_str,
                    "bow": bow_counter(cleaned),
                })
                continue

            anomalies += 1
            contexts = topk_similar(normals, cleaned, k=context_k) if normals else []
            analysis = ollama_llm_analysis(cleaned, contexts)

            # Normalize timestamp to epoch millis
            ts_payload = payload.get("timestamp")
            if isinstance(ts_payload, (int, float)):
                if ts_payload < 10_000_000_000:
                    ts_millis = int(round(ts_payload * 1000))
                else:
                    ts_millis = int(ts_payload)
            else:
                ts_millis = int(round(time.time() * 1000))

            doc = {
                "raw_log": raw_log,
                "cleaned_log": cleaned,
                "template_id": template_id,
                "template": template_str,
                "parameters": parameters,
                "num_parameters": len(parameters),
                "anomaly_score": loss,
                "strict_threshold": strict_threshold,
                "is_anomaly": True,
                "context_logs": [c.get("raw_log", "") for c in contexts],
                "context_templates": [c.get("template", "") for c in contexts],
                "analysis": analysis,
                "source_file": payload.get("source_file", "unknown"),
                "source_directory": payload.get("source_directory", "unknown"),
                "line_number": payload.get("line_number", -1),
                "timestamp_received": ts_millis,
                "@timestamp": ts_millis,
            }
            try:
                es.index(index=analyzed_index, body=doc)
            except Exception as e:
                logger.error("Elasticsearch index error (analyze): %s", e)

            if processed % 200 == 0:
                print(f"[analyze] processed={processed}, anomalies={anomalies}, normals_in_buffer={len(normals)}")

    except KeyboardInterrupt:
        print("[analyze] Interrupted by user")
    finally:
        consumer.close()
        print("[analyze] Kafka consumer closed")

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

    p_analyze = sub.add_parser("analyze", help="Strict triage + context search + LLM stub to enriched index")
    p_analyze.add_argument("--threshold", type=float, default=STRICT_THRESHOLD_DEFAULT, help="Strict anomaly threshold (score > threshold triggers analysis)")
    p_analyze.add_argument("--context-k", type=int, default=CONTEXT_K, help="Number of normal contexts to retrieve")
    p_analyze.add_argument("--max-normals", type=int, default=NORMAL_BUFFER_MAX, help="Max normal logs kept in buffer for context search")

    p_clear_anal = sub.add_parser("clear-anal", help="Delete analyzed anomalies index")

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
    elif args.cmd == "analyze":
        consume_analyze(
            topic_name=TOPIC_NAME,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            es_hosts=ES_HOSTS,
            analyzed_index=ANALYZED_INDEX,
            strict_threshold=args.threshold,
            context_k=args.context_k,
            max_normals=args.max_normals,
        )
    elif args.cmd == "clear-anal":
        clear_analysis_index(es_hosts=ES_HOSTS, analyzed_index=ANALYZED_INDEX)
