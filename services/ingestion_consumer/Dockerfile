# Build context is the repo root (see docker-compose.yml) so this image can
# share common/db.py and db/models.py with the other services instead of
# duplicating the schema/connection code three times.
FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg2 apt-transport-https ca-certificates \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc-dev \
    && apt-get purge -y curl gnupg2 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY services/ingestion_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/ingestion_consumer/app .

ENV PYTHONUNBUFFERED=1
CMD ["python", "consumer.py"]
