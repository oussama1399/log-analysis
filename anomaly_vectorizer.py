import json
import logging
import os
import time
import requests
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
from typing import Dict, List
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
ES_HOSTS = os.getenv("ELASTICSEARCH_HOSTS", "http://localhost:9200").split(",")
ANOMALY_INDEX = "mozilla-clogs-index"  # Index contenant tous les logs (avec flag is_anomaly)
ANOMALY_VECTOR_INDEX = "anomalies_vectorized"
VECTOR_DIM = 384
BATCH_SIZE = 50
POLL_INTERVAL = 5  # seconds

# Initialize Elasticsearch and embedding model
es = Elasticsearch(ES_HOSTS)
embedder = SentenceTransformer('all-MiniLM-L6-v2')

def create_vector_index():
    """Create the vector index if it doesn't exist."""
    if not es.indices.exists(index=ANOMALY_VECTOR_INDEX):
        es.indices.create(
            index=ANOMALY_VECTOR_INDEX,
            body={
                "mappings": {
                    "properties": {
                        "raw_log": {"type": "text"},
                        "cleaned_log": {"type": "text"},
                        "embedding": {"type": "dense_vector", "dims": VECTOR_DIM},
                        "anomaly_score": {"type": "float"},
                        "is_anomaly": {"type": "boolean"},
                        "timestamp_received": {"type": "long"},
                        "@timestamp": {"type": "date", "format": "epoch_millis"},
                        "source_file": {"type": "keyword"},
                        "template": {"type": "text"},
                    }
                }
            }
        )
        logger.info(f"Created index {ANOMALY_VECTOR_INDEX}")

def get_last_processed_timestamp():
    """Get the timestamp of the last processed anomaly."""
    try:
        resp = es.search(
            index=ANOMALY_VECTOR_INDEX,
            body={
                "size": 1,
                "sort": [{"@timestamp": {"order": "desc"}}]
            }
        )
        if resp["hits"]["hits"]:
            return resp["hits"]["hits"][0]["_source"].get("@timestamp", 0)
    except Exception as e:
        logger.warning(f"Could not retrieve last timestamp: {e}")
    return 0

def fetch_anomalies(since_timestamp: int) -> List[Dict]:
    """Fetch anomalies from Elasticsearch since a given timestamp."""
    try:
        resp = es.search(
            index=ANOMALY_INDEX,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"is_anomaly": True}},
                            {"range": {"@timestamp": {"gte": since_timestamp}}}
                        ]
                    }
                },
                "size": BATCH_SIZE,
                "sort": [{"@timestamp": {"order": "asc"}}]
            }
        )
        return [hit["_source"] for hit in resp["hits"]["hits"]]
    except Exception as e:
        logger.error(f"Error fetching anomalies: {e}")
        return []

def vectorize_and_store_anomalies(anomalies: List[Dict]):
    """Vectorize anomalies and store them in the vector index."""
    if not anomalies:
        return
    
    # Extract raw logs for vectorization
    raw_logs = [a.get("raw_log", "") or a.get("message", "") for a in anomalies]
    
    try:
        embeddings = embedder.encode(raw_logs, show_progress_bar=False)
    except Exception as e:
        logger.error(f"Error vectorizing anomalies: {e}")
        return
    
    # Prepare bulk operations
    actions = []
    for anomaly, emb in zip(anomalies, embeddings):
        doc = anomaly.copy()
        doc["embedding"] = emb.tolist()
        actions.append({
            "_index": ANOMALY_VECTOR_INDEX,
            "_source": doc
        })
    
    # Bulk index to Elasticsearch
    try:
        from elasticsearch import helpers
        helpers.bulk(es, actions)
        logger.info(f"Stored {len(actions)} vectorized anomalies")
    except Exception as e:
        logger.error(f"Error bulk indexing anomalies: {e}")

def monitor_anomalies():
    """Continuously monitor and vectorize anomalies."""
    logger.info("Starting anomaly vectorizer...")
    create_vector_index()
    
    last_timestamp = get_last_processed_timestamp()
    logger.info(f"Starting from timestamp: {last_timestamp}")
    
    while True:
        try:
            anomalies = fetch_anomalies(last_timestamp)
            if anomalies:
                logger.info(f"Found {len(anomalies)} new anomalies")
                vectorize_and_store_anomalies(anomalies)
                last_timestamp = anomalies[-1].get("@timestamp", last_timestamp)
            else:
                logger.debug("No new anomalies found")
        except Exception as e:
            logger.error(f"Error in monitoring loop: {e}")
        
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    monitor_anomalies()
