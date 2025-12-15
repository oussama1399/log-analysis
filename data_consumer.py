# data_consumer.py
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import logging

# Configurez le logging# data_consumer.py
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import logging

# Configurez le logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def consume_and_index():
    # Initialiser le consommateur Kafka
    # Utilisez localhost:9092 car le script est exécuté depuis l'hôte, pas un conteneur Docker
    consumer = KafkaConsumer(
        'mozilla-logs', # Topic à consommer
        bootstrap_servers=['localhost:9092'], # Adresse de Kafka
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest', # Commencer du début si le groupe n'a pas encore consommé
        group_id='log_analysis_group' # Identifiant du groupe de consommateurs
    )

    # Initialiser le client Elasticsearch
    # Utilisez localhost:9200 car le script est exécuté depuis l'hôte
    # Spécifiez les en-têtes pour forcer une compatibilité de version (ici, 8.x)
    es = Elasticsearch(
        [{'host': 'localhost', 'port': 9200, 'scheme': 'http'}],
        headers={
            "Content-Type": "application/json",
            # Assurez-vous que cette version correspond à celle de votre image Docker
            # et à celle supportée par votre installation Python.
            # Ex: Si vous utilisez 8.15.0 dans docker-compose.yml
            "Accept": "application/vnd.elasticsearch+json; compatible-with=8"
        }
    )

    index_name = "mozilla-clogs-index" # Nommez votre index comme vous le souhaitez

    logger.info("Démarrage du consommateur Kafka...")
    try:
        for message in consumer:
            log_data = message.value
            # print(f"Message reçu : {log_data}") # Pour déboguer, à commenter si trop verbeux

            # --- Votre logique de traitement ici (à développer plus tard) ---
            # Exemples :
            # - Nettoyage du raw_log
            # - Extraction d'entités (dates, erreurs, composants, etc.)
            # - Application de modèles ML/NLP (détecteurs d'anomalie, classification, etc.)
            # processed_data = your_nlp_or_ml_function(log_data)
            # Pour l'instant, on indexe tel quel, mais avec un peu de transformation
            doc_to_index = {
                "raw_log": log_data.get("raw_log", ""),
                "source_file": log_data.get("source_file", "unknown"),
                "source_directory": log_data.get("source_directory", "unknown"),
                "line_number": log_data.get("line_number", -1),
                "timestamp_received": log_data.get("timestamp", None) # Timestamp d'envoi à Kafka
                # Vous pouvez ajouter ici les résultats de votre traitement ML/NLP
            }
            # --- Fin de votre logique de traitement ---

            # Indexer dans Elasticsearch
            try:
                res = es.index(index=index_name, body=doc_to_index)
                # print(f"Document indexé avec ID: {res['_id']}") # Pour déboguer
            except Exception as e:
                logger.error(f"Erreur lors de l'indexation dans ES: {e}")

    except KeyboardInterrupt:
        logger.info("Arrêt du consommateur demandé par l'utilisateur.")
    except Exception as e:
        logger.error(f"Erreur fatale dans le consommateur: {e}")
        raise
    finally:
        consumer.close()
        logger.info("Consommateur Kafka fermé.")

if __name__ == "__main__":
    consume_and_index()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def consume_and_index():
    # Initialiser le consommateur Kafka
    # Utilisez localhost:9092 car le script est exécuté depuis l'hôte, pas un conteneur Docker
    consumer = KafkaConsumer(
        'mozilla-logs', # Topic à consommer
        bootstrap_servers=['localhost:9092'], # Adresse de Kafka
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest', # Commencer du début si le groupe n'a pas encore consommé
        group_id='log_analysis_group' # Identifiant du groupe de consommateurs
    )

    # Initialiser le client Elasticsearch
    # Utilisez localhost:9200 car le script est exécuté depuis l'hôte
    es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

    index_name = "mozilla-clogs-index" # Nommez votre index comme vous le souhaitez

    logger.info("Démarrage du consommateur Kafka...")
    try:
        for message in consumer:
            log_data = message.value
            # print(f"Message reçu : {log_data}") # Pour déboguer, à commenter si trop verbeux

            # --- Votre logique de traitement ici (à développer plus tard) ---
            # Exemples :
            # - Nettoyage du raw_log
            # - Extraction d'entités (dates, erreurs, composants, etc.)
            # - Application de modèles ML/NLP (détecteurs d'anomalie, classification, etc.)
            # processed_data = your_nlp_or_ml_function(log_data)
            # Pour l'instant, on indexe tel quel, mais avec un peu de transformation
            doc_to_index = {
                "raw_log": log_data.get("raw_log", ""),
                "source_file": log_data.get("source_file", "unknown"),
                "source_directory": log_data.get("source_directory", "unknown"),
                "line_number": log_data.get("line_number", -1),
                "timestamp_received": log_data.get("timestamp", None) # Timestamp d'envoi à Kafka
                # Vous pouvez ajouter ici les résultats de votre traitement ML/NLP
            }
            # --- Fin de votre logique de traitement ---

            # Indexer dans Elasticsearch
            try:
                res = es.index(index=index_name, body=doc_to_index)
                # print(f"Document indexé avec ID: {res['_id']}") # Pour déboguer
            except Exception as e:
                logger.error(f"Erreur lors de l'indexation dans ES: {e}")

    except KeyboardInterrupt:
        logger.info("Arrêt du consommateur demandé par l'utilisateur.")
    except Exception as e:
        logger.error(f"Erreur fatale dans le consommateur: {e}")
        raise
    finally:
        consumer.close()
        logger.info("Consommateur Kafka fermé.")

if __name__ == "__main__":
    consume_and_index()