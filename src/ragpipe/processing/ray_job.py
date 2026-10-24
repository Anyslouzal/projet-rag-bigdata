"""Job Ray Data : documents PostgreSQL -> chunks -> embeddings -> Qdrant (+ métadonnées PostgreSQL).

Soumis au cluster Ray par Prefect (API Ray Jobs). Peut aussi se lancer à la main
depuis un conteneur du cluster :

    python -m ragpipe.processing.ray_job --max-docs 2000

Étapes (exécutées en flux, en parallèle sur les workers Ray) :
    1. lecture des documents par plages de doc_id + découpage en chunks   (tâches Ray)
    2. calcul des embeddings          (pool d'acteurs, 1 modèle chargé par acteur)
    3. écriture Qdrant + PostgreSQL   (acteur dédié)
"""

import argparse
import json
import time
import uuid

import numpy as np
import ray

from ragpipe.config import settings
from ragpipe.processing.chunking import split_text
from ragpipe.storage import postgres, qdrant_store

# fastembed / onnxruntime ne sont importés que dans les acteurs d'embedding :
# les autres processus Ray restent légers en mémoire.

CHUNK_NAMESPACE = uuid.UUID("6f1c2a52-6b0e-4d55-9c1e-2b7d3c9e8a10")


def chunk_uuid(doc_id: int, chunk_index: int) -> str:
    """Identifiant déterministe : relancer le job réécrit les mêmes points (idempotent)."""
    return str(uuid.uuid5(CHUNK_NAMESPACE, f"{doc_id}:{chunk_index}"))


def load_and_chunk(task: dict) -> list[dict]:
    """Lit une plage de documents dans PostgreSQL et les découpe en chunks."""
    docs = postgres.fetch_pending_documents(task["lo"], task["hi"])
    return [chunk for doc in docs for chunk in to_chunks(doc)]


def to_chunks(doc: dict) -> list[dict]:
    pieces = split_text(doc["content"], settings.chunk_max_chars, settings.chunk_overlap)
    return [
        {
            "chunk_id": chunk_uuid(doc["doc_id"], i),
            "doc_id": doc["doc_id"],
            "chunk_index": i,
            "title": doc["title"],
            "url": doc["url"],
            "text": piece,
        }
        for i, piece in enumerate(pieces)
    ]


class EmbedChunks:
    """Acteur Ray : charge le modèle une seule fois, puis vectorise des lots de chunks."""

    def __init__(self, threads: int):
        from ragpipe.processing.embedding import Embedder

        self.embedder = Embedder(threads=threads)

    def __call__(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        # Le titre est ajouté au texte : un passage seul perd souvent son sujet
        texts = [f"{title}\n{text}" for title, text in zip(batch["title"], batch["text"])]
        batch["vector"] = self.embedder.embed(texts)
        return batch


class WriteChunks:
    """Acteur Ray : écrit les vecteurs dans Qdrant et les métadonnées dans PostgreSQL."""

    def __init__(self):
        self.client = qdrant_store.get_client()
        self.collection_ready = False

    def __call__(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        if not self.collection_ready:
            # La dimension des vecteurs est lue sur le premier lot
            qdrant_store.ensure_collection(self.client, batch["vector"].shape[1])
            self.collection_ready = True
        ids = [str(x) for x in batch["chunk_id"]]
        payloads = [
            {"doc_id": int(d), "chunk_index": int(i), "title": str(t), "url": str(u), "text": str(x)}
            for d, i, t, u, x in zip(batch["doc_id"], batch["chunk_index"], batch["title"],
                                     batch["url"], batch["text"])
        ]
        qdrant_store.upsert_points(self.client, ids, batch["vector"].tolist(), payloads)
        postgres.insert_chunks([
            (cid, p["doc_id"], p["chunk_index"], len(p["text"]), settings.embedding_model)
            for cid, p in zip(ids, payloads)
        ])
        return {"written": np.array([len(ids)])}


def run(max_docs: int, docs_per_task: int, embed_actors: int, threads: int) -> dict:
    start = time.time()
    lo, hi, n_docs = postgres.pending_range(max_docs)
    if not n_docs:
        return {"docs": 0, "chunks": 0, "seconds": 0.0}

    tasks = [{"lo": a, "hi": min(a + docs_per_task - 1, hi)}
             for a in range(lo, hi + 1, docs_per_task)]
    print(f"{n_docs} documents (doc_id {lo} à {hi}) répartis en {len(tasks)} tâches")

    ds = (
        ray.data.from_items(tasks, override_num_blocks=len(tasks))
        .flat_map(load_and_chunk, num_cpus=0.5)
        .map_batches(EmbedChunks, fn_constructor_kwargs={"threads": threads},
                     batch_size=128, num_cpus=threads, concurrency=embed_actors)
        .map_batches(WriteChunks, batch_size=256, num_cpus=0.5, concurrency=1)
    )
    n_chunks = int(ds.sum("written") or 0)
    n_marked = postgres.mark_indexed(lo, hi)
    return {"docs": n_marked, "chunks": n_chunks, "seconds": round(time.time() - start, 1)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vectorisation distribuée avec Ray Data")
    parser.add_argument("--max-docs", type=int, default=2000, help="documents à traiter dans ce run")
    parser.add_argument("--docs-per-task", type=int, default=200, help="documents lus par tâche Ray")
    parser.add_argument("--embed-actors", type=int, default=2, help="acteurs d'embedding en parallèle")
    parser.add_argument("--threads", type=int, default=2, help="threads CPU par acteur d'embedding")
    args = parser.parse_args()

    ray.init()  # se connecte au cluster existant (ou en démarre un local)
    result = run(args.max_docs, args.docs_per_task, args.embed_actors, args.threads)
    # Ligne lue par Prefect pour récupérer le résultat du job
    print("RESULT " + json.dumps(result))
