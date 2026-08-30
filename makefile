COMPOSE_FILE = docker-compose.yml
COMPOSE_LOCAL_FILE = docker-compose-local.yaml

.PHONY: init up up_build up_build_local down down_local restart status logs shell heartbeat clean clean_all \
        pipeline_ingestion_airbnb pipeline_vector_airbnb agent_test \
        pipeline_docker_ingestion_airbnb pipeline_docker_vector_airbnb \
        fastapi_dev fastapi_docker_build fastapi_docker_run_container duckdb_docker

# Ensure external networks exist before booting
init:
	@docker network inspect movie-network >/dev/null 2>&1 || docker network create movie-network
	@docker network inspect langfuse-network >/dev/null 2>&1 || docker network create langfuse-network
	@docker network inspect mlflow-network >/dev/null 2>&1 || docker network create mlflow-network

# ---------------------------------------------------------
# DOCKER COMPOSE COMMANDS
# ---------------------------------------------------------
up: init
	docker compose -f $(COMPOSE_FILE) up -d

up_build: init
	docker compose -f $(COMPOSE_FILE) up --build -d

up_build_local: init
	docker compose -f $(COMPOSE_LOCAL_FILE) up --build -d

down:
	docker compose -f $(COMPOSE_FILE) down

down_local:
	docker compose -f $(COMPOSE_LOCAL_FILE) down

restart:
	docker compose -f $(COMPOSE_FILE) restart

status:
	docker compose -f $(COMPOSE_FILE) ps

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

shell:
	docker exec -it chromadb /bin/sh

heartbeat:
	curl -f http://localhost:8201/api/v2/heartbeat

clean:
	docker compose -f $(COMPOSE_FILE) down -v
	docker system prune -f

clean_all:
	docker compose -f $(COMPOSE_FILE) down -v
	docker system prune -a -f
	rm -rf ./_chroma-data ./_clickhouse-data

# ---------------------------------------------------------
# LOCAL PIPELINE SCRIPTS (Run directly via uv on host)
# ---------------------------------------------------------
pipeline_ingestion_airbnb:
	uv run python pipeline/ingestion_airbnb.py

pipeline_vector_airbnb:
	uv run python pipeline/vector_airbnb.py

agent_test:
	uv run backend/agent.py

example_chroma:
	uv run example/chroma.py

# ---------------------------------------------------------
# DOCKER PIPELINE SCRIPTS (Run inside backend container)
# ---------------------------------------------------------
pipeline_docker_ingestion_airbnb:
	docker compose exec backend uv run python pipeline/scrape_airbnb.py
	docker compose exec backend uv run python pipeline/ingestion_airbnb.py

pipeline_docker_vector_airbnb:
	docker compose exec backend uv run python pipeline/vector_airbnb.py

# ---------------------------------------------------------
# FASTAPI
# ---------------------------------------------------------
fastapi_dev:
	export PYTHONPATH=$$PYTHONPATH:. && uv run fastapi dev backend/main.py --port 8000

fastapi_docker_build:
	docker build -t movie-planner-backend -f Dockerfile .

fastapi_docker_run_container:
	docker run -p 8000:8000 --env-file .env movie-planner-backend

# ---------------------------------------------------------
# DUCKDB
# ---------------------------------------------------------

duckdb_docker:
	docker compose exec backend duckdb /app/_duckdb-data/hotel_movie.db