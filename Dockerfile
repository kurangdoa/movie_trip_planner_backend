FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# 1. Copy dependency files
COPY pyproject.toml uv.lock ./

# 2. Install dependencies
# --no-install-project tells uv to install the libs but wait to install 'your' code
# --frozen ensures it matches your lockfile exactly
RUN uv sync --frozen --no-install-project --no-dev

# 3. Copy the rest of the project
COPY . .

# 4. Set the path so your code is discoverable
ENV PYTHONPATH=/app

# 5. Run using 'uv run' to ensure the environment is activated correctly
CMD ["uv", "run", "fastapi", "dev", "backend/main.py", "--port", "80", "--host", "0.0.0.0"]