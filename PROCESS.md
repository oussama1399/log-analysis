# Pipeline global

## 1. Préparation & preprocessing
- Nettoyage des logs avec regex (horodatages, PID, IDs, IP → tokens normalisés).
- Tokenisation sur espaces, mapping vocabulaire, padding/troncature à longueur fixe (`MAX_LEN`).
- Jeu de données structuré en séquences d’IDs + vocabulaire sérialisé (`checkpoints/vocab_full.json`).

## 2. Modèle (autoencoder LSTM)
- Embedding → Encoder LSTM → latent → Decoder LSTM → projection vocab.
- Perte = CrossEntropy sur la reconstruction (ignore padding).
- Checkpoints entraînés sauvegardés dans `checkpoints/` (ex: `model.pth`, `autoencoder_epoch_*.pth`).

## 3. Entraînement (résumé)
- Entrée : séquences nettoyées/paddées.
- Optimiseur (non détaillé ici), CrossEntropyLoss `ignore_index=pad_id`.
- Sauvegarde : poids modèle + vocab.

## 4. Évaluation offline (notebook `test.ipynb`)
- Chargement vocab + modèle.
- Calcul des scores d’anomalie (perte de reconstruction) ligne par ligne sur un fichier.
- Statistiques : moyenne, écart-type, seuil = moyenne + 3*std ; top anomalies.
- Export CSV : `checkpoints/test_anomaly_results.csv`, `checkpoints/anomalies_only.csv`.

## 5. Streaming temps réel (`streaming_process.py`)
- Kafka topic helper : création/recréation du topic (`topic`).
- Producteur : lecture des fichiers `data/kafka/log-*/**.txt`, envoi ligne par ligne dans Kafka avec métadonnées.
- Consommateur (scoring) :
  - Nettoie + encode + passe dans le modèle.
  - Score = perte de reconstruction.
  - Seuil :
    - fixe si `--threshold` fourni
    - sinon dynamique en ligne : `threshold = mean + factor * std` (Welford, `factor` par défaut 3.0)
  - Ajoute les champs : `anomaly_score`, `is_anomaly`, `threshold_used`, `mean_score`, `std_score`.
  - Indexe dans Elasticsearch (`mozilla-clogs-index`).
- Consommateur (analyse) :
  - Seuil strict (défaut 3.5) : si `loss > threshold` → anomalie à enrichir.
  - Buffer de normales (max 50k) + recherche contexte top-k (cosine BoW) sur les normales.
  - Appel LLM via Ollama (`gpt-oss:20b`) avec les contextes ; réponse attendue en JSON structuré : `llm_class`, `llm_root_cause`, `llm_suggested_fix`, `llm_confidence`.
  - Indexe les anomalies enrichies dans Elasticsearch (`analyzed_anomalies`).

### Commandes clés
- Assurer/rafraîchir le topic :
  - `python streaming_process.py topic`
- Ingestion seule :
  - `python streaming_process.py ingest --base-dir data/kafka --delay 0.01 --batch 1000`
- Consommation + scoring + ES :
  - Seuil dynamique : `python streaming_process.py consume --factor 3.0`
  - Seuil fixe : `python streaming_process.py consume --threshold 3.5`
- Analyse (seuil strict + contexte + LLM) :
  - `python streaming_process.py analyze --threshold 3.5 --context-k 3 --max-normals 50000`
- Flux tout-en-un (topic + ingest + consume) :
  - `python streaming_process.py stream --base-dir data/kafka --delay 0.01 --batch 1000 --factor 3.0`
- Nettoyage :
  - `python streaming_process.py clear` (topic + index principal)
  - `python streaming_process.py clear-anal` (index des anomalies enrichies)

## 6. Déploiement local (Docker Compose)
- Services : Kafka, Elasticsearch, Kibana.
- Attention : supprimer/renommer tout conteneur `elasticsearch` existant avant `docker-compose up`.

## 7. Points d’attention
- Placer le checkpoint attendu (ex: `checkpoints/model.pth`) avant `consume/stream`.
- LLM : définir `OLLAMA_HOST` si différent (défaut `http://127.0.0.1:11434`), s’assurer que le modèle `gpt-oss:20b` est pullé et que l’API répond ; sinon `analyze` s’arrête.
- Le seuil dynamique a besoin de données “normales” pour se calibrer ; ajuster `factor` selon le bruit.
- Sur gros fichiers, l’ingestion est déjà en streaming (line-by-line, batch Kafka).