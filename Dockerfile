# One image, two modes:
#   docker run durableflow server --host 0.0.0.0 --port 8000
#   docker run durableflow worker
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim
RUN useradd --create-home --uid 10001 app && mkdir /data && chown app /data
COPY --from=builder --chown=app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    DURABLEFLOW_DB=/data/durableflow.db \
    DURABLEFLOW_HOST=0.0.0.0 \
    DURABLEFLOW_PORT=8000
USER app
WORKDIR /home/app
EXPOSE 8000
# Server mode answers /healthz. Worker mode has no port, so the check falls back to
# opening the database when the HTTP check fails.
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD durableflow healthcheck --url http://127.0.0.1:8000/healthz || durableflow healthcheck
ENTRYPOINT ["durableflow"]
CMD ["server"]
