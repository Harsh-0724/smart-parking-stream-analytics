FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY common common
COPY simulator simulator
COPY processor processor
COPY sink sink
COPY alerter alerter
ENV PATH="/app/.venv/bin:$PATH" PYTHONPATH=/app
CMD ["python", "-m", "processor.main"]
