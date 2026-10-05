FROM python:3.12-slim

WORKDIR /app

# Build context is the repo root (see docker-compose.yml) so common/telemetry.py is shared.
COPY services/webhook_receiver/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/webhook_receiver/app ./app

ENV PYTHONPATH=/app

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
