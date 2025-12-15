# Fix for unresolved import
# Ensure the kafka-python library is installed

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import logging
import os
import time
from kafka import KafkaProducer
import json

# Configurez le logging pour voir les messages d'information et d'erreur
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_topic_exists(topic_name, num_partitions=1, replication_factor=1, 
                       bootstrap_servers='localhost:9092'):
    """
    Crée un topic Kafka. S'il existe déjà, le supprime et le recrée.

    Args:
        topic_name (str): Le nom du topic à gérer.
        num_partitions (int): Le nombre de partitions pour le topic. Défaut: 1.
        replication_factor (int): Le facteur de réplication. Défaut: 1.
        bootstrap_servers (str or list): Adresse(s) du serveur Kafka. Défaut: 'localhost:9092'.
    """
    # Fix for type mismatch in bootstrap_servers
    # Convert the list of bootstrap servers to a comma-separated string
    if isinstance(bootstrap_servers, list):
        bootstrap_servers = ",".join(bootstrap_servers)

    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers
    )

    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        # Vous pouvez ajouter des configurations spécifiques ici si nécessaire
        # topic_configs={'retention.ms': '604800000'} # Ex: 7 jours
    )

    try:
        logger.info(f"Tentative de création du topic '{topic_name}'...")
        admin_client.create_topics([topic], validate_only=False)
        logger.info(f"Topic '{topic_name}' créé avec succès.")

    except TopicAlreadyExistsError:
        logger.info(f"Le topic '{topic_name}' existe déjà. Tentative de suppression...")
        
        try:
            admin_client.delete_topics([topic_name])
            logger.info(f"Ancien topic '{topic_name}' supprimé.")
        except UnknownTopicOrPartitionError:
            # Ce cas est rare mais possible si le topic disparaît entre le check et la suppression
            logger.warning(f"Le topic '{topic_name}' n'existait plus lors de la tentative de suppression.")
        except Exception as e:
            logger.error(f"Erreur lors de la suppression du topic '{topic_name}': {e}")
            raise # Propage l'erreur si la suppression échoue

        # Recréer le topic après la suppression
        logger.info(f"Recréation du topic '{topic_name}'...")
        admin_client.create_topics([topic], validate_only=False)
        logger.info(f"Topic '{topic_name}' recréé avec succès.")

    except Exception as e:
        logger.error(f"Erreur lors de la gestion du topic '{topic_name}': {e}")
        raise # Propage toute autre erreur

    finally:
        admin_client.close()


def simulate_log_ingestion(
    base_dir="data/kafka",
    topic_name="mozilla-logs",
    bootstrap_servers=["localhost:9092"],
    delay_seconds=0.01,
    batch_size=1000,
    add_timestamp=True
):
    """
    Simule l'ingestion des logs Mozilla en respectant l'ordre chronologique.
    (Modifiée pour lire les fichiers .txt)
    """
    print(f"Démarrage de la simulation d'ingestion des logs depuis '{base_dir}'...")
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=500
    )

    try:
        # Lister et trier les dossiers par date (ordre croissant)
        log_dirs = [d for d in os.listdir(base_dir) if d.startswith("log-")]
        log_dirs.sort()
        
        total_lines = 0
        total_files = 0

        for log_dir in log_dirs:
            dir_path = os.path.join(base_dir, log_dir)
            if not os.path.isdir(dir_path):
                continue

            print(f"\n--- Traitement du dossier : {log_dir} ---")

            # MODIFICATION ICI : Chercher les fichiers .txt au lieu de .log
            files = [f for f in os.listdir(dir_path) if f.endswith(".txt")]
            files.sort() # Tri alphabétique des fichiers

            for filename in files:
                file_path = os.path.join(dir_path, filename)
                total_files += 1
                
                print(f"  Lecture du fichier : {filename}")

                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    line_count = 0
                    batch = []

                    for line in f:
                        line = line.rstrip('\n')
                        if not line.strip(): # Ignorer les lignes vides
                            continue

                        message = {
                            "raw_log": line,
                            "source_file": filename,
                            "source_directory": log_dir,
                            "line_number": line_count + 1
                        }

                        if add_timestamp:
                            message["timestamp"] = time.time()

                        batch.append(message)

                        if len(batch) >= batch_size or (line_count == 0 and len(batch) > 0):
                            for msg in batch:
                                producer.send(topic_name, value=msg)
                            producer.flush()
                            batch.clear()

                        time.sleep(delay_seconds)

                        line_count += 1
                        total_lines += 1

                if batch:
                    for msg in batch:
                        producer.send(topic_name, value=msg)
                    producer.flush()

                print(f"  Fichier traité : {filename} ({line_count} lignes)")

        print(f"\nSimulation terminée !")
        print(f"Total des fichiers traités : {total_files}")
        print(f"Total des lignes ingérées : {total_lines}")

    except Exception as e:
        print(f"Erreur lors de la simulation : {e}")
        raise

    finally:
        producer.close()
        print("Producteur Kafka fermé.")

# --- Exemple d'utilisation ---
if __name__ == "__main__":
    # Assurez-vous que Kafka est démarré
    TOPIC_NAME = "mozilla-logs" # Remplacez par le nom souhaité
    BOOTSTRAP_SERVERS = ['localhost:9092'] # Utilisez la liste ou une chaîne

    # Supprimer et recréer le topic
    ensure_topic_exists(
        topic_name=TOPIC_NAME,
        num_partitions=1, # Ajustez selon vos besoins
        replication_factor=1, # Toujours 1 en mode KRaft single-node
        bootstrap_servers=BOOTSTRAP_SERVERS
    )
    print("Le topic est prêt a etre utilise.")

    # Commencer le streaming
    simulate_log_ingestion(
        base_dir="data/kafka", # Chemin relatif a partir de la ou vous executez le script
        topic_name="mozilla-logs",
        bootstrap_servers=["localhost:9092"], # Ou ["kafka:9092"] si dans le meme reseau Docker
        delay_seconds=0.005, # Envoyer 200 lignes par seconde (ajustez selon vos besoins)
        batch_size=1000,
        add_timestamp=True
    )
