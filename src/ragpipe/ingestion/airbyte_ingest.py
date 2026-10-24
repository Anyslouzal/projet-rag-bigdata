"""Collecte Airbyte : fichiers parquet Wikipédia FR (HTTPS) -> PostgreSQL, schéma `raw`.

Utilise PyAirbyte, qui exécute les connecteurs Airbyte officiels directement en Python
(ici `source-file`) et écrit dans un cache PostgreSQL, sans déployer la plateforme.

    python -m ragpipe.ingestion.airbyte_ingest --shards 16
"""

import argparse

import airbyte as ab
from airbyte.caches import PostgresCache

from ragpipe.config import settings

RAW_SCHEMA = "raw"


def get_cache() -> PostgresCache:
    return PostgresCache(
        host=settings.pg_host,
        port=settings.pg_port,
        username=settings.pg_user,
        password=settings.pg_password,
        database=settings.pg_db,
        schema_name=RAW_SCHEMA,
    )


def get_source(url: str) -> ab.Source:
    source = ab.get_source(
        "source-file",
        config={
            "dataset_name": settings.corpus_name,  # devient le nom du flux et de la table raw
            "format": "parquet",
            "url": url,
            "provider": {"storage": "HTTPS", "user_agent": False},
        },
        install_if_missing=True,
        use_python="3.11",  # le connecteur ne s'installe pas sous Python 3.12
    )
    source.select_all_streams()
    return source


def ingest_shards(shards: list[int]) -> int:
    """Collecte les fichiers demandés et retourne le nombre d'enregistrements lus."""
    cache = get_cache()
    total = 0
    for shard in shards:
        url = settings.corpus_url_template.format(shard=shard)
        print(f"[airbyte] fichier {shard} : {url}")
        source = get_source(url)
        source.check()
        # "append" : chaque fichier s'ajoute à la table raw au lieu de la remplacer
        result = source.read(cache=cache, write_strategy="append")
        total += result.processed_records
        print(f"[airbyte] fichier {shard} : {result.processed_records} enregistrements")
    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shards", type=int, nargs="+", default=[16],
                        help="numéros des fichiers parquet à collecter (0 à 16)")
    args = parser.parse_args()
    print(f"{ingest_shards(args.shards)} enregistrements collectés")
