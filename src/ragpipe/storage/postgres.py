"""Accès PostgreSQL : documents, chunks, suivi des runs et des requêtes."""

import json

import psycopg
from psycopg.rows import dict_row

from ragpipe.config import settings


def connect() -> psycopg.Connection:
    """Connexion à utiliser avec `with` : commit automatique en sortie de bloc."""
    return psycopg.connect(settings.pg_dsn, row_factory=dict_row)


# --- Documents et chunks (utilisés par le job Ray) ---------------------------

def pending_range(max_docs: int) -> tuple[int | None, int | None, int]:
    """Plage d'identifiants des `max_docs` prochains documents à vectoriser."""
    with connect() as conn:
        row = conn.execute(
            """
            SELECT min(doc_id) AS lo, max(doc_id) AS hi, count(*) AS n
            FROM (SELECT doc_id FROM documents
                  WHERE indexed_at IS NULL ORDER BY doc_id LIMIT %s) AS pending
            """,
            (max_docs,),
        ).fetchone()
    return row["lo"], row["hi"], row["n"]


def fetch_pending_documents(lo: int, hi: int) -> list[dict]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT doc_id, title, coalesce(url, '') AS url, content
            FROM documents
            WHERE doc_id BETWEEN %s AND %s AND indexed_at IS NULL
            """,
            (lo, hi),
        ).fetchall()


def insert_chunks(rows: list[tuple]) -> None:
    """rows : (chunk_id, doc_id, chunk_index, n_chars, embedding_model)."""
    with connect() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO chunks (chunk_id, doc_id, chunk_index, n_chars, embedding_model)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (chunk_id) DO NOTHING
            """,
            rows,
        )


def mark_indexed(lo: int, hi: int) -> int:
    with connect() as conn:
        cur = conn.execute(
            """
            UPDATE documents SET indexed_at = now()
            WHERE doc_id BETWEEN %s AND %s AND indexed_at IS NULL
            """,
            (lo, hi),
        )
        return cur.rowcount


# --- Suivi des runs (utilisé par Prefect) ------------------------------------

def start_run(flow_run_id: str | None, shards: list[int]) -> int:
    with connect() as conn:
        return conn.execute(
            "INSERT INTO pipeline_runs (flow_run_id, shards) VALUES (%s, %s) RETURNING run_id",
            (flow_run_id, shards),
        ).fetchone()["run_id"]


def finish_run(run_id: int, status: str, error: str | None = None, **counters: int) -> None:
    """counters : docs_collected, docs_staged, docs_indexed, chunks_indexed."""
    with connect() as conn:
        conn.execute(
            """
            UPDATE pipeline_runs SET
                finished_at    = now(),
                status         = %(status)s,
                error          = %(error)s,
                docs_collected = %(docs_collected)s,
                docs_staged    = %(docs_staged)s,
                docs_indexed   = %(docs_indexed)s,
                chunks_indexed = %(chunks_indexed)s,
                duration_s     = extract(epoch FROM now() - started_at)
            WHERE run_id = %(run_id)s
            """,
            {
                "run_id": run_id,
                "status": status,
                "error": error,
                "docs_collected": counters.get("docs_collected"),
                "docs_staged": counters.get("docs_staged"),
                "docs_indexed": counters.get("docs_indexed"),
                "chunks_indexed": counters.get("chunks_indexed"),
            },
        )


# --- Requêtes de l'application (utilisé par Gradio) ---------------------------

def log_query(question: str, top_k: int, best_score: float | None, retrieval_ms: float,
              generation_ms: float | None, llm_model: str | None, sources: list[dict]) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO query_logs
                (question, top_k, best_score, retrieval_ms, generation_ms, llm_model, sources)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (question, top_k, best_score, retrieval_ms, generation_ms, llm_model,
             json.dumps(sources, ensure_ascii=False)),
        )


def corpus_stats() -> dict:
    with connect() as conn:
        return conn.execute(
            """
            SELECT count(*)                         AS documents,
                   count(indexed_at)                AS documents_indexes,
                   (SELECT count(*) FROM chunks)    AS chunks,
                   coalesce(sum(n_chars), 0)        AS caracteres,
                   (SELECT count(*) FROM query_logs) AS questions
            FROM documents
            """
        ).fetchone()


def recent_runs(limit: int = 10) -> list[dict]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT run_id, to_char(started_at, 'YYYY-MM-DD HH24:MI') AS debut, status, shards,
                   docs_collected, docs_staged, docs_indexed, chunks_indexed,
                   round(duration_s::numeric, 1) AS duree_s
            FROM pipeline_runs ORDER BY run_id DESC LIMIT %s
            """,
            (limit,),
        ).fetchall()


def recent_queries(limit: int = 10) -> list[dict]:
    with connect() as conn:
        return conn.execute(
            """
            SELECT to_char(asked_at, 'YYYY-MM-DD HH24:MI') AS date, question,
                   round(best_score::numeric, 3) AS meilleur_score,
                   round(retrieval_ms::numeric) AS recherche_ms,
                   round(generation_ms::numeric) AS generation_ms
            FROM query_logs ORDER BY query_id DESC LIMIT %s
            """,
            (limit,),
        ).fetchall()
