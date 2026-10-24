"""Application Gradio : assistant RAG, recherche sémantique et suivi du pipeline.

    python -m ragpipe.app.main      ->  http://localhost:7860
"""

import time

import gradio as gr
import httpx
import pandas as pd

from ragpipe.config import settings
from ragpipe.retrieval.llm import stream_answer
from ragpipe.retrieval.search import Retriever, Source
from ragpipe.storage import postgres, qdrant_store

retriever = Retriever()

# Sujets présents dans les 2 000 premiers articles du fichier 16 (premier run de démo)
EXAMPLES = [
    "Qu'est-ce que l'effet de faux consensus ?",
    "Comment fonctionne une horloge mécanique ?",
    "Que prévoit le droit à la déconnexion ?",
]


def format_sources(sources: list[Source]) -> str:
    if not sources:
        return "_Aucun passage trouvé. Le pipeline a-t-il déjà vectorisé des documents ?_"
    blocks = []
    for i, s in enumerate(sources, 1):
        excerpt = s.text[:350].replace("\n", " ")
        blocks.append(f"**[{i}] [{s.title}]({s.url})** · similarité {s.score:.3f}\n\n> {excerpt}…")
    return "\n\n".join(blocks)


# --- Onglet Assistant ---------------------------------------------------------

def ask(question: str, history: list[dict], k: int):
    """Recherche les passages, puis diffuse la réponse du LLM au fil de l'eau."""
    question = question.strip()
    if not question:
        yield history, history, gr.skip(), ""
        return
    history = history + [{"role": "user", "content": question},
                         {"role": "assistant", "content": ""}]

    t0 = time.perf_counter()
    sources = retriever.search(question, k)
    retrieval_ms = (time.perf_counter() - t0) * 1000
    sources_md = format_sources(sources)
    yield history, history, sources_md, ""

    if not sources:
        history[-1]["content"] = "Je n'ai trouvé aucun passage pertinent dans le corpus indexé."
        yield history, history, sources_md, ""
        return

    t1 = time.perf_counter()
    generation_ms = None
    try:
        for token in stream_answer(question, sources):
            history[-1]["content"] += token
            yield history, history, sources_md, ""
        generation_ms = (time.perf_counter() - t1) * 1000
    except httpx.HTTPError as exc:
        history[-1]["content"] += (
            f"\n\n⚠️ LLM injoignable ({exc.__class__.__name__}). Vérifiez qu'Ollama tourne "
            f"et que le modèle `{settings.llm_model}` est installé (`ollama pull {settings.llm_model}`)."
        )
    yield history, history, sources_md, ""

    postgres.log_query(question, k, sources[0].score, retrieval_ms, generation_ms,
                       settings.llm_model, [s.to_dict() | {"text": s.text[:200]} for s in sources])


# --- Onglet Recherche ---------------------------------------------------------

def semantic_search(query: str, k: int, title_filter: str) -> pd.DataFrame:
    if not query.strip():
        return pd.DataFrame()
    sources = retriever.search(query, k, title_filter.strip() or None)
    return pd.DataFrame([
        {"score": round(s.score, 3), "titre": s.title, "extrait": s.text[:300], "lien": s.url}
        for s in sources
    ])


# --- Onglet Pipeline ----------------------------------------------------------

def pipeline_status():
    try:
        stats = postgres.corpus_stats()
        runs = pd.DataFrame(postgres.recent_runs())
        queries = pd.DataFrame(postgres.recent_queries())
        if queries.empty:
            queries = pd.DataFrame(columns=["date", "question", "meilleur_score",
                                            "recherche_ms", "generation_ms"])
    except Exception as exc:  # base indisponible : on l'affiche au lieu de planter
        return f"⚠️ PostgreSQL indisponible : {exc}", pd.DataFrame(), pd.DataFrame()
    try:
        points = qdrant_store.count_points(qdrant_store.get_client())
    except Exception:
        points = "?"
    summary = (
        f"| Documents | Vectorisés | Chunks (PostgreSQL) | Points (Qdrant) | Caractères | Questions posées |\n"
        f"|---:|---:|---:|---:|---:|---:|\n"
        f"| {stats['documents']:,} | {stats['documents_indexes']:,} | {stats['chunks']:,} "
        f"| {points if isinstance(points, str) else f'{points:,}'} | {stats['caracteres']:,} "
        f"| {stats['questions']:,} |"
    ).replace(",", " ")
    return summary, runs, queries


# --- Interface ----------------------------------------------------------------

with gr.Blocks(title="RAG Wikipédia FR") as demo:
    gr.Markdown(
        "# Recherche sémantique sur Wikipédia FR\n"
        "Pipeline Big Data : **Airbyte** → **PostgreSQL** → **Ray** → **Qdrant** → **Gradio**, "
        f"orchestré par **Prefect**. LLM : `{settings.llm_model}`."
    )

    with gr.Tab("Assistant"):
        history_state = gr.State([])
        with gr.Row():
            with gr.Column(scale=3):
                chatbot = gr.Chatbot(height=480, label="Conversation")
                question_box = gr.Textbox(placeholder="Posez une question sur le corpus…",
                                          show_label=False, submit_btn=True)
                gr.Examples(EXAMPLES, inputs=question_box)
            with gr.Column(scale=2):
                k_slider = gr.Slider(1, 10, value=5, step=1, label="Passages utilisés (top-k)")
                sources_md = gr.Markdown("_Les sources apparaîtront ici._", label="Sources")
        clear_btn = gr.Button("Nouvelle conversation", variant="secondary")
        question_box.submit(ask, [question_box, history_state, k_slider],
                            [chatbot, history_state, sources_md, question_box])
        clear_btn.click(lambda: ([], [], "_Les sources apparaîtront ici._"), None,
                        [chatbot, history_state, sources_md])

    with gr.Tab("Recherche sémantique"):
        with gr.Row():
            query_box = gr.Textbox(label="Requête", scale=3)
            title_box = gr.Textbox(label="Filtre : le titre contient", scale=2)
            k_search = gr.Slider(1, 50, value=10, step=1, label="Résultats", scale=1)
        results = gr.Dataframe(wrap=True, label="Passages les plus proches")
        gr.Button("Rechercher", variant="primary").click(
            semantic_search, [query_box, k_search, title_box], results)
        query_box.submit(semantic_search, [query_box, k_search, title_box], results)

    with gr.Tab("Pipeline"):
        summary_md = gr.Markdown()
        gr.Markdown("### Derniers runs du pipeline")
        runs_df = gr.Dataframe(wrap=True)
        gr.Markdown("### Dernières questions")
        queries_df = gr.Dataframe(wrap=True)
        refresh_btn = gr.Button("Actualiser")
        refresh_btn.click(pipeline_status, None, [summary_md, runs_df, queries_df])
        demo.load(pipeline_status, None, [summary_md, runs_df, queries_df])


if __name__ == "__main__":
    demo.queue().launch(server_name="0.0.0.0", server_port=7860, theme=gr.themes.Soft())
