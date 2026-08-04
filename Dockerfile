FROM python:3.12.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY clean_mcp/requirements-api.lock /tmp/requirements-api.lock
RUN python -m pip install --no-cache-dir -r /tmp/requirements-api.lock \
    && groupadd --system schemabridge \
    && useradd --system --gid schemabridge --home-dir /app --shell /usr/sbin/nologin schemabridge

COPY --chown=schemabridge:schemabridge clean_mcp /app/clean_mcp

USER schemabridge
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=2).read()"]

CMD ["python", "-m", "uvicorn", "clean_mcp.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
