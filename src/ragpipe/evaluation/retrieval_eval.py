"""Évaluation de la recherche sémantique et de la génération sur des questions synthétiques.

Pour chaque passage tiré au hasard dans l'index, le LLM écrit une question à laquelle ce
passage répond. On vérifie ensuite si la recherche retrouve l'article d'origine
(Hit@k, MRR), puis on mesure la génération complète sur quelques questions.

    docker compose exec -T app python -m ragpipe.evaluation.retrieval_eval > docs/evaluation/resultats.json
"""

import argparse
import json
import re
import statistics
import sys
import time

import httpx

from ragpipe.config import settings
from ragpipe.retrieval.llm import stream_answer
from ragpipe.retrieval.search import Retriever
from ragpipe.storage import postgres, qdrant_store

QUESTION_PROMPT = """Voici un extrait d'un article Wikipédia intitulé « {title} ».
Écris UNE question en français, précise, à laquelle cet extrait permet de répondre.
Reformule avec tes propres mots : ne recopie aucune phrase de l'extrait.
La question doit se comprendre seule : ne parle pas de « l'extrait » ni du « texte ».
Réponds uniquement par la question.

Extrait :
{text}"""


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def sample_passages(n: int, seed: str) -> list[dict]:
    """Un passage par article, tiré de façon reproductible (même graine = même échantillon)."""
    with postgres.connect() as conn:
        rows = conn.execute(
            """
            SELECT chunk_id::text, doc_id, chunk_index FROM (
                SELECT DISTINCT ON (doc_id) chunk_id, doc_id, chunk_index
                FROM chunks ORDER BY doc_id, md5(chunk_id::text || %s)
            ) AS one_per_doc
            ORDER BY md5(chunk_id::text || %s) LIMIT %s
            """,
            (seed, seed, n),
        ).fetchall()
    client = qdrant_store.get_client()
    points = client.retrieve(qdrant_store.COLLECTION, [r["chunk_id"] for r in rows],
                             with_payload=True)
    payloads = {str(p.id): p.payload for p in points}
    return [{**r, **payloads[r["chunk_id"]]} for r in rows if r["chunk_id"] in payloads]


def generate_question(passage: dict) -> str:
    response = httpx.post(
        f"{settings.llm_base_url}/chat/completions",
        headers={"Authorization": f"Bearer {settings.llm_api_key}"},
        json={
            "model": settings.llm_model,
            "messages": [{"role": "user", "content": QUESTION_PROMPT.format(**passage)}],
            "temperature": 0,
        },
        timeout=180,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"].strip().strip('"«» ')


def evaluate_retrieval(retriever: Retriever, passages: list[dict], k: int) -> list[dict]:
    rows = []
    for i, passage in enumerate(passages, 1):
        question = generate_question(passage)
        t0 = time.perf_counter()
        sources = retriever.search(question, k)
        latency_ms = (time.perf_counter() - t0) * 1000
        doc_ids = [s.doc_id for s in sources]
        rank = doc_ids.index(passage["doc_id"]) + 1 if passage["doc_id"] in doc_ids else None
        rows.append({
            "question": question,
            "title": passage["title"],
            "doc_id": passage["doc_id"],
            "rank": rank,
            "passage_found": any(s.text == passage["text"] for s in sources[:5]),
            "top1_title": sources[0].title if sources else None,
            "top1_score": round(sources[0].score, 3) if sources else None,
            "retrieval_ms": round(latency_ms, 1),
        })
        log(f"[{i}/{len(passages)}] rang={rank} · {question}")
    return rows


def evaluate_generation(retriever: Retriever, questions: list[str], k: int) -> list[dict]:
    """Temps jusqu'au premier mot, temps total et présence de citations [n]."""
    rows = []
    for question in questions:
        sources = retriever.search(question, k)
        t0 = time.perf_counter()
        first_token_s, chunks = None, []
        for chunk in stream_answer(question, sources):
            if first_token_s is None:
                first_token_s = time.perf_counter() - t0
            chunks.append(chunk)
        answer = "".join(chunks)
        rows.append({
            "question": question,
            "first_token_s": round(first_token_s or 0, 2),
            "total_s": round(time.perf_counter() - t0, 2),
            "n_words": len(answer.split()),
            "cites_sources": bool(re.search(r"\[\d+\]", answer)),
            "answer": answer,
        })
        log(f"génération : {rows[-1]['total_s']} s · {question}")
    return rows


def summarize(retrieval: list[dict], generation: list[dict], k: int) -> dict:
    ranks = [r["rank"] for r in retrieval]
    n = len(ranks)
    latencies = sorted(r["retrieval_ms"] for r in retrieval)
    summary = {
        "questions": n,
        "hit@1": round(sum(1 for r in ranks if r == 1) / n, 3),
        "hit@5": round(sum(1 for r in ranks if r and r <= 5) / n, 3),
        f"hit@{k}": round(sum(1 for r in ranks if r) / n, 3),
        f"mrr@{k}": round(sum(1 / r for r in ranks if r) / n, 3),
        "passage_exact@5": round(sum(r["passage_found"] for r in retrieval) / n, 3),
        "retrieval_ms_median": round(statistics.median(latencies), 1),
        "retrieval_ms_p95": latencies[max(0, int(0.95 * n) - 1)],
        "llm_model": settings.llm_model,
        "embedding_model": settings.embedding_model,
    }
    if generation:
        summary.update({
            "generation_runs": len(generation),
            "first_token_s_median": statistics.median(g["first_token_s"] for g in generation),
            "total_s_median": statistics.median(g["total_s"] for g in generation),
            "answers_with_citations": sum(g["cites_sources"] for g in generation),
        })
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Évalue la recherche et la génération")
    parser.add_argument("--n", type=int, default=60, help="questions synthétiques")
    parser.add_argument("--k", type=int, default=10, help="passages retrouvés par question")
    parser.add_argument("--answers", type=int, default=5, help="questions avec génération complète")
    parser.add_argument("--seed", default="groupe6")
    args = parser.parse_args()

    retriever = Retriever()
    passages = sample_passages(args.n, args.seed)
    log(f"{len(passages)} passages tirés au hasard dans l'index")
    retrieval = evaluate_retrieval(retriever, passages, args.k)
    generation = evaluate_generation(retriever, [r["question"] for r in retrieval[:args.answers]], 5)
    result = {"summary": summarize(retrieval, generation, args.k),
              "retrieval": retrieval, "generation": generation}
    print(json.dumps(result, ensure_ascii=False, indent=2))
