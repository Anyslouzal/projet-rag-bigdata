# Raccourcis : make up | make run | make test | make eval | make logs | make down
.PHONY: up run run-quick test eval logs ps down reset

up:          ## Construit l'image et démarre tous les services
	docker compose up -d --build

run:         ## Lance un run complet : collecte du fichier 16 + vectorisation de 2000 documents
	docker compose exec pipeline python -m ragpipe.orchestration.flows --shards 16 --max-docs 2000

run-quick:   ## Vectorise 500 documents déjà collectés (sans nouvelle collecte)
	docker compose exec pipeline python -m ragpipe.orchestration.flows --skip-collect --max-docs 500

test:        ## Tests unitaires
	docker compose run --rm --no-deps app pytest -q tests

eval:        ## Évalue la recherche et la génération sur 60 questions de test (~5 min)
	docker compose exec -T app python -m ragpipe.evaluation.retrieval_eval > docs/evaluation/resultats.json

logs:
	docker compose logs -f --tail=50

ps:
	docker compose ps

down:        ## Arrête les services (les données sont conservées)
	docker compose down

reset:       ## Arrête tout ET supprime les données (PostgreSQL + Qdrant)
	docker compose down -v
