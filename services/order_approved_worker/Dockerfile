FROM python:3.12-slim

WORKDIR /app

# Build context is the repo root (see docker-compose.yml) so common/telemetry.py is shared.
COPY services/order_approved_worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/order_approved_worker/app ./app

ENV PYTHONPATH=/app

ENV PYTHONUNBUFFERED=1
CMD ["python", "app/worker.py"]
