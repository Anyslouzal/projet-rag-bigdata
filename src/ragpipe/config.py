"""Configuration centralisée du projet, lue depuis les variables d'environnement.

En Docker, les valeurs viennent du fichier .env (copie de .env.example).
Les valeurs par défaut permettent de lancer les scripts depuis la machine hôte.
"""

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Settings:
    # PostgreSQL
    pg_host: str = _env("POSTGRES_HOST", "localhost")
    pg_port: int = int(_env("POSTGRES_PORT", "5432"))
    pg_user: str = _env("POSTGRES_USER", "rag")
    pg_password: str = _env("POSTGRES_PASSWORD", "rag")
    pg_db: str = _env("POSTGRES_DB", "rag")

    # Qdrant
    qdrant_url: str = _env("QDRANT_URL", "http://localhost:6333")
    qdrant_collection: str = _env("QDRANT_COLLECTION", "wikipedia_fr")

    # Corpus : Wikipédia FR (17 fichiers parquet, ~2,6 M articles)
    corpus_name: str = _env("CORPUS_NAME", "wikipedia_fr")
    corpus_url_template: str = _env(
        "CORPUS_URL_TEMPLATE",
        "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/"
        "20231101.fr/train-{shard:05d}-of-00017.parquet",
    )
    min_doc_chars: int = int(_env("MIN_DOC_CHARS", "500"))

    # Traitement
    embedding_model: str = _env(
        "EMBEDDING_MODEL", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    )
    chunk_max_chars: int = int(_env("CHUNK_MAX_CHARS", "1000"))
    chunk_overlap: int = int(_env("CHUNK_OVERLAP", "150"))

    # LLM (toute API compatible OpenAI : Ollama, Mistral, Groq, OpenAI...)
    llm_base_url: str = _env("LLM_BASE_URL", "http://localhost:11434/v1")
    llm_api_key: str = _env("LLM_API_KEY", "ollama")
    llm_model: str = _env("LLM_MODEL", "qwen2.5:7b")

    # Ray
    ray_dashboard_url: str = _env("RAY_DASHBOARD_URL", "http://localhost:8265")

    @property
    def pg_dsn(self) -> str:
        return (
            f"postgresql://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )


settings = Settings()
