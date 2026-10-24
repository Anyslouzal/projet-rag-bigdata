-- Schéma relationnel du projet (exécuté au premier démarrage de PostgreSQL).
--
--   raw.*          zone d'atterrissage : tables écrites telles quelles par Airbyte
--   documents      documents nettoyés et dédoublonnés (1 ligne = 1 article)
--   chunks         métadonnées des passages vectorisés (le vecteur est dans Qdrant)
--   pipeline_runs  historique des exécutions du pipeline (rempli par Prefect)
--   query_logs     questions posées dans l'application (rempli par Gradio)

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS documents (
    doc_id       BIGSERIAL PRIMARY KEY,
    source       TEXT        NOT NULL,          -- ex. 'wikipedia_fr'
    source_id    TEXT        NOT NULL,          -- identifiant dans la source
    title        TEXT        NOT NULL,
    url          TEXT,
    content      TEXT        NOT NULL,
    n_chars      INTEGER     NOT NULL,
    checksum     TEXT        NOT NULL,          -- md5 du contenu : détecte les modifications
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    indexed_at   TIMESTAMPTZ,                   -- NULL = pas encore vectorisé
    UNIQUE (source, source_id)
);
-- Index partiel : retrouve vite les documents restant à vectoriser
CREATE INDEX IF NOT EXISTS documents_pending_idx ON documents (doc_id) WHERE indexed_at IS NULL;

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id        UUID        PRIMARY KEY,    -- même identifiant que le point Qdrant
    doc_id          BIGINT      NOT NULL REFERENCES documents (doc_id) ON DELETE CASCADE,
    chunk_index     INTEGER     NOT NULL,
    n_chars         INTEGER     NOT NULL,
    embedding_model TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (doc_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    flow_run_id     TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT        NOT NULL DEFAULT 'running',  -- running | success | failed
    shards          INTEGER[],
    docs_collected  INTEGER,
    docs_staged     INTEGER,
    docs_indexed    INTEGER,
    chunks_indexed  INTEGER,
    duration_s      REAL,
    error           TEXT
);

CREATE TABLE IF NOT EXISTS query_logs (
    query_id       BIGSERIAL PRIMARY KEY,
    asked_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    question       TEXT        NOT NULL,
    top_k          INTEGER,
    best_score     REAL,
    retrieval_ms   REAL,
    generation_ms  REAL,
    llm_model      TEXT,
    sources        JSONB
);
