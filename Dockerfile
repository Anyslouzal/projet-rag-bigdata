# Image unique utilisée par tous les services Python du projet
# (Ray head/workers, Prefect server, pipeline, application Gradio).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src \
    FASTEMBED_CACHE_PATH=/opt/models \
    AIRBYTE_INSTALL_DIR=/opt/airbyte

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl build-essential \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.lock requirements.txt ./
RUN pip install -r requirements.lock

# Pré-installe le connecteur Airbyte "source-file" dans son propre venv Python 3.11
# (il ne s'installe pas sous 3.12) pour éviter une installation à chaque run.
RUN python -c "import airbyte as ab; ab.get_source('source-file', install_if_missing=True, use_python='3.11')"

# Pré-télécharge le modèle d'embedding multilingue (~220 Mo) dans l'image
RUN python -c "from fastembed import TextEmbedding; TextEmbedding('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')"

COPY src ./src
COPY tests ./tests
