# Variables
COMPOSE_FILE=docker-compose.yml

.PHONY: up down restart status logs shell clean heartbeat

# Start the ChromaDB service in the background
up:
	docker compose up -d

# Stop the service
down:
	docker compose down

# Restart the service (useful if you change the volumes or config)
restart:
	docker compose restart

# Check the status of the containers
status:
	docker compose ps

# Follow the logs to see what's happening inside Chroma
logs:
	docker compose logs -f

# Enter the container's shell (useful for debugging)
shell:
	docker exec -it $(shell docker ps -qf "name=chromadb") /bin/sh

# Check the heartbeat (Healthcheck) via curl locally
# Note: uses the mapped port 8201
heartbeat:
	curl -f http://localhost:8201/api/v2/heartbeat

# Clean up: stops containers and removes the chroma-data volume (WARNING: deletes data)
clean:
	docker compose down -v
	rm -rf ./_chroma-data
	rm -rf ./_clickhouse-data

example_chroma:
	uv run example/chroma.py

agent_test:
	uv run backend/agent.py

pipeline_docker_ingestion_airbnb:
	docker compose exec backend uv run python pipeline/ingestion_airbnb.py

pipeline_docker_ingestion_airbnb_duckdb:
	docker compose exec backend uv run python pipeline/ingestion_airbnb_duckdb.py

pipeline_docker_vector_airbnb:
	docker compose exec backend uv run python pipeline/vector_airbnb.py

# ----------
# fastapi
# ----------

fastapi_dev:
	export PYTHONPATH=$PYTHONPATH:.
	uv run fastapi dev backend/main.py --port 8000

# This builds the image from the root context so 'shared' is included
fastapi_docker_build:
	docker build -t movie-planner-backend -f backend/Dockerfile .

# Runs the container locally to test the production build
fastapi_docker_run_container:
	docker run -p 8000:80 --env-file backend/.env movie-planner-backend