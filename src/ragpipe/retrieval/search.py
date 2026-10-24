"""Recherche sémantique : question -> vecteur -> passages les plus proches dans Qdrant."""

from dataclasses import asdict, dataclass

from ragpipe.processing.embedding import Embedder
from ragpipe.storage import qdrant_store


@dataclass
class Source:
    doc_id: int
    title: str
    url: str
    text: str
    score: float

    def to_dict(self) -> dict:
        return asdict(self)


class Retriever:
    def __init__(self):
        self.embedder = Embedder()
        self.client = qdrant_store.get_client()

    def search(self, question: str, k: int = 5, title_contains: str | None = None) -> list[Source]:
        vector = self.embedder.embed_query(question)
        points = qdrant_store.search(self.client, vector, k, title_contains or None)
        return [
            Source(
                doc_id=p.payload["doc_id"],
                title=p.payload["title"],
                url=p.payload.get("url", ""),
                text=p.payload["text"],
                score=p.score,
            )
            for p in points
        ]
