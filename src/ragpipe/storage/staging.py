"""Staging : zone brute Airbyte (schéma raw) -> table `documents` nettoyée et dédoublonnée."""

from psycopg import sql

from ragpipe.storage.postgres import connect

# - DISTINCT ON garde la version la plus récente de chaque article (ré-ingestions)
# - les articles trop courts (ébauches, homonymies) sont écartés
# - ON CONFLICT ... WHERE : un article déjà présent n'est mis à jour que si son
#   contenu a changé, et il repasse alors en "à vectoriser" (indexed_at = NULL)
# - xmax = 0 distingue une insertion d'une mise à jour
STAGE_SQL = """
WITH src AS (
    SELECT DISTINCT ON (id) id, title, url, text
    FROM {raw}
    WHERE text IS NOT NULL AND length(text) >= %(min_chars)s
    ORDER BY id, _airbyte_extracted_at DESC
), upserted AS (
    INSERT INTO documents (source, source_id, title, url, content, n_chars, checksum)
    SELECT %(source)s, id, title, url, text, length(text), md5(text) FROM src
    ON CONFLICT (source, source_id) DO UPDATE SET
        title = EXCLUDED.title, url = EXCLUDED.url, content = EXCLUDED.content,
        n_chars = EXCLUDED.n_chars, checksum = EXCLUDED.checksum,
        ingested_at = now(), indexed_at = NULL
    WHERE documents.checksum <> EXCLUDED.checksum
    RETURNING doc_id, (xmax = 0) AS inserted
)
SELECT count(*) FILTER (WHERE inserted)                          AS inserted,
       coalesce(array_agg(doc_id) FILTER (WHERE NOT inserted), '{{}}') AS updated_doc_ids
FROM upserted
"""


def stage_documents(raw_table: str, source: str, min_chars: int, purge_raw: bool = True) -> dict:
    """Retourne le nombre de nouveaux documents et les doc_id des documents modifiés.

    Les chunks des documents modifiés sont supprimés ici (PostgreSQL) ; leurs points
    Qdrant doivent être supprimés par l'appelant avant la re-vectorisation.
    """
    raw = sql.Identifier("raw", raw_table)
    with connect() as conn:
        if conn.execute("SELECT to_regclass(%s) AS t", (f"raw.{raw_table}",)).fetchone()["t"] is None:
            return {"inserted": 0, "updated_doc_ids": []}
        row = conn.execute(
            sql.SQL(STAGE_SQL).format(raw=raw), {"source": source, "min_chars": min_chars}
        ).fetchone()
        if row["updated_doc_ids"]:
            conn.execute("DELETE FROM chunks WHERE doc_id = ANY(%s)", (row["updated_doc_ids"],))
        if purge_raw:
            # La zone brute est un sas : une fois les données promues, on la vide
            conn.execute(sql.SQL("TRUNCATE {}").format(raw))
    return {"inserted": row["inserted"], "updated_doc_ids": row["updated_doc_ids"]}
