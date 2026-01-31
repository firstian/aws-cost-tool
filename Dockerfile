FROM python:3.14-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV UV_SYSTEM_PYTHON=1 \
    PATH="/root/.local/bin:$PATH"

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /app

ARG BUILD_CONFIG_DIR=none

ENV CONFIG_DIR=/app/.config \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

RUN uv sync --extra app --frozen

RUN mkdir -p /app/.config

RUN if [ "$BUILD_CONFIG_DIR" != "none" ] && [ -d "$BUILD_CONFIG_DIR" ]; then \
    echo "Copying config from $BUILD_CONFIG_DIR"; \
    cp -r "$BUILD_CONFIG_DIR"/. /app/.config/ || true; \
    fi

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["uv", "run", "aws-cost-tool", "--config", "/app/.config"]
