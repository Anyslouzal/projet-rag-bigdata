"""Modèle d'embedding, partagé par le traitement Ray et la recherche.

Le même modèle doit produire les vecteurs des documents et ceux des questions,
sinon la recherche par similarité n'a pas de sens.
"""

import numpy as np
from fastembed import TextEmbedding

from ragpipe.config import settings


class Embedder:
    def __init__(self, model_name: str | None = None, threads: int | None = None):
        self.model_name = model_name or settings.embedding_model
        self.model = TextEmbedding(self.model_name, threads=threads)

    @property
    def dim(self) -> int:
        return len(self.embed_query("dimension"))

    def embed(self, texts: list[str], batch_size: int = 64) -> np.ndarray:
        """Retourne une matrice (n_textes, dim) de vecteurs float32."""
        return np.stack(list(self.model.embed(list(texts), batch_size=batch_size)))

    def embed_query(self, query: str) -> list[float]:
        return next(iter(self.model.query_embed(query))).tolist()
