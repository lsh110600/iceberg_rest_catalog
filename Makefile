.PHONY: up down logs build test reset playground-up playground-down playground-status livy-up livy-down airgap-bundle airgap-validate

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f backend frontend

build:
	docker compose build

test:
	docker build --target test -t iceberg-ops-backend-test ./backend
	docker run --rm --entrypoint sh iceberg-ops-backend-test -c "ruff check app tests && pytest -q"

playground-up:
	docker compose --profile playground up --build -d

playground-down:
	docker compose --profile playground stop operation-worker playground-runner livy spark-worker spark-master datanode namenode

playground-status:
	docker compose --profile playground ps

livy-up:
	docker compose --profile livy up --build -d livy-worker

livy-down:
	docker compose --profile livy stop livy-worker

reset:
	docker compose down -v

airgap-bundle:
	./scripts/build-airgap-bundle.sh 0.1.0

airgap-validate:
	docker compose --env-file deploy/.env.airgap.example -f deploy/compose.airgap.yaml config --quiet
