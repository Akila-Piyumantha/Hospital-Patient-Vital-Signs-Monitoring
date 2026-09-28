# Image for the simulators + seeding job. Build context is the repository root
# (see docker-compose.yml) because the code imports the shared `common` package.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
# optional PyPI mirror for slow links: docker compose build --build-arg PIP_INDEX_URL=<mirror>/simple
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN pip install -r requirements.txt

COPY common ./common
COPY simulators ./simulators
COPY sql ./sql

# The default command is overridden per service in docker-compose.yml.
CMD ["python", "-m", "simulators.vitals_producer"]
