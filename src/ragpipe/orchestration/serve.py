"""Publie le pipeline comme déploiement Prefect et attend les runs.

Les runs se lancent depuis l'interface Prefect (http://localhost:4200, bouton « Run »)
ou automatiquement selon la planification ci-dessous (en pause par défaut).
"""

from ragpipe.orchestration.flows import rag_pipeline

if __name__ == "__main__":
    rag_pipeline.serve(
        name="wikipedia-fr",
        cron="0 2 * * *",  # chaque nuit à 2 h, une fois activé dans l'UI
        paused=True,
        parameters={"shards": [16], "max_docs": 2000},
        tags=["rag", "big-data"],
        description="Collecte Wikipédia FR, vectorise les nouveaux documents et contrôle la cohérence.",
    )
