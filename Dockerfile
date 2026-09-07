FROM python:3.12-slim AS runtime

ENV HOST=0.0.0.0 PORT=8111 PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.11.8 /uv /uvx /bin/
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY static ./static
RUN uv sync --frozen --no-dev

EXPOSE 8111
CMD ["durable-workflows"]
