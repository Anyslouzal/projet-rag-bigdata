"""Orchestration Prefect : collecte Airbyte -> staging PostgreSQL -> vectorisation Ray -> contrôles.

Lancer un run immédiatement (depuis le conteneur `pipeline`) :

    python -m ragpipe.orchestration.flows --shards 16 --max-docs 2000
"""

import argparse
import json
import time

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run
from ray.job_submission import JobStatus, JobSubmissionClient

from ragpipe.config import settings
from ragpipe.ingestion import airbyte_ingest
from ragpipe.storage import postgres, qdrant_store, staging

RAY_DONE = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.STOPPED}


@task(name="collecte-airbyte", retries=2, retry_delay_seconds=60)
def collect(shards: list[int]) -> int:
    return airbyte_ingest.ingest_shards(shards)


@task(name="staging-postgres")
def stage() -> int:
    logger = get_run_logger()
    result = staging.stage_documents(settings.corpus_name, settings.corpus_name,
                                     settings.min_doc_chars)
    updated = result["updated_doc_ids"]
    if updated:
        # Documents modifiés : leurs anciens vecteurs sont supprimés avant re-vectorisation
        qdrant_store.delete_docs(qdrant_store.get_client(), updated)
    logger.info("%s nouveaux documents, %s modifiés", result["inserted"], len(updated))
    return result["inserted"] + len(updated)


@task(name="vectorisation-ray")
def vectorize(max_docs: int, embed_actors: int) -> dict:
    logger = get_run_logger()
    client = JobSubmissionClient(settings.ray_dashboard_url)
    job_id = client.submit_job(
        entrypoint=(f"python -m ragpipe.processing.ray_job "
                    f"--max-docs {max_docs} --embed-actors {embed_actors}"),
    )
    logger.info("Job Ray %s soumis, suivi sur le dashboard Ray (port 8265)", job_id)
    while (status := client.get_job_status(job_id)) not in RAY_DONE:
        time.sleep(5)
    logs = client.get_job_logs(job_id)
    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(f"Job Ray {job_id} terminé en {status}\n{logs[-3000:]}")
    result_line = next(line for line in reversed(logs.splitlines()) if line.startswith("RESULT "))
    result = json.loads(result_line.removeprefix("RESULT "))
    logger.info("Ray : %(docs)s documents, %(chunks)s chunks en %(seconds)s s", result)
    return result


@task(name="controles-qualite")
def quality_checks() -> dict:
    logger = get_run_logger()
    stats = postgres.corpus_stats()
    points = qdrant_store.count_points(qdrant_store.get_client())
    if points != stats["chunks"]:
        logger.warning("Incohérence : %s chunks dans PostgreSQL, %s points dans Qdrant",
                       stats["chunks"], points)
    logger.info("Corpus : %s documents dont %s vectorisés, %s chunks",
                stats["documents"], stats["documents_indexes"], stats["chunks"])
    return {**stats, "qdrant_points": points}


@flow(name="pipeline-rag", log_prints=True)
def rag_pipeline(shards: list[int] = [16], max_docs: int = 2000, embed_actors: int = 2,
                 skip_collect: bool = False) -> dict:
    """Pipeline complet. `skip_collect=True` vectorise seulement les documents déjà en base."""
    run_id = postgres.start_run(flow_run.id, [] if skip_collect else shards)
    counters: dict[str, int] = {}
    try:
        if not skip_collect:
            counters["docs_collected"] = collect(shards)
        counters["docs_staged"] = stage()
        result = vectorize(max_docs, embed_actors)
        counters["docs_indexed"] = result["docs"]
        counters["chunks_indexed"] = result["chunks"]
        report = quality_checks()
    except Exception as exc:
        postgres.finish_run(run_id, "failed", error=str(exc)[:2000], **counters)
        raise
    postgres.finish_run(run_id, "success", **counters)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lance un run du pipeline RAG")
    parser.add_argument("--shards", type=int, nargs="+", default=[16])
    parser.add_argument("--max-docs", type=int, default=2000)
    parser.add_argument("--embed-actors", type=int, default=2)
    parser.add_argument("--skip-collect", action="store_true")
    args = parser.parse_args()
    rag_pipeline(args.shards, args.max_docs, args.embed_actors, args.skip_collect)
