"""Accès Qdrant : collection vectorielle des chunks (vecteur + texte + métadonnées)."""

from qdrant_client import QdrantClient, models

from ragpipe.config import settings

COLLECTION = settings.qdrant_collection


def get_client() -> QdrantClient:
    return QdrantClient(url=settings.qdrant_url, timeout=60)


def ensure_collection(client: QdrantClient, dim: int) -> bool:
    """Crée la collection si elle n'existe pas. Retourne True si elle vient d'être créée."""
    if client.collection_exists(COLLECTION):
        return False
    client.create_collection(
        collection_name=COLLECTION,
        vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE, on_disk=True),
        # Quantification int8 : vecteurs 4x plus petits en RAM, recherche plus rapide ;
        # les vecteurs originaux restent sur disque pour le re-scoring.
        quantization_config=models.ScalarQuantization(
            scalar=models.ScalarQuantizationConfig(type=models.ScalarType.INT8, always_ram=True)
        ),
        on_disk_payload=True,
    )
    # Index de payload : filtres rapides par document et recherche plein texte sur le titre
    client.create_payload_index(COLLECTION, "doc_id", models.PayloadSchemaType.INTEGER)
    client.create_payload_index(
        COLLECTION,
        "title",
        models.TextIndexParams(type=models.TextIndexType.TEXT,
                               tokenizer=models.TokenizerType.WORD, lowercase=True),
    )
    return True


def upsert_points(client: QdrantClient, ids: list[str], vectors: list[list[float]],
                  payloads: list[dict]) -> None:
    client.upsert(
        collection_name=COLLECTION,
        points=models.Batch(ids=ids, vectors=vectors, payloads=payloads),
        wait=True,
    )


def delete_docs(client: QdrantClient, doc_ids: list[int]) -> None:
    if not doc_ids or not client.collection_exists(COLLECTION):
        return
    client.delete(
        collection_name=COLLECTION,
        points_selector=models.FilterSelector(filter=models.Filter(must=[
            models.FieldCondition(key="doc_id", match=models.MatchAny(any=doc_ids))
        ])),
    )


def search(client: QdrantClient, vector: list[float], k: int,
           title_contains: str | None = None) -> list[models.ScoredPoint]:
    query_filter = None
    if title_contains:
        query_filter = models.Filter(must=[
            models.FieldCondition(key="title", match=models.MatchText(text=title_contains))
        ])
    return client.query_points(
        collection_name=COLLECTION,
        query=vector,
        limit=k,
        query_filter=query_filter,
        with_payload=True,
        search_params=models.SearchParams(
            quantization=models.QuantizationSearchParams(rescore=True)
        ),
    ).points


def count_points(client: QdrantClient) -> int:
    if not client.collection_exists(COLLECTION):
        return 0
    return client.count(COLLECTION, exact=True).count
