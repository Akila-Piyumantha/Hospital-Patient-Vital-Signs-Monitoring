# FastAPI serving layer (Member C). Build context = repository root (imports `common`).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=UTC

WORKDIR /app
COPY serving/requirements.txt serving/requirements.txt
# optional PyPI mirror for slow links: docker compose build --build-arg PIP_INDEX_URL=<mirror>/simple
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN pip install --default-timeout=300 --retries 10 -r serving/requirements.txt

COPY common ./common
COPY serving ./serving

EXPOSE 8000
# uvicorn's own access log is off: the app logs every request as structured JSON (contract 4.8)
CMD ["uvicorn", "serving.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
