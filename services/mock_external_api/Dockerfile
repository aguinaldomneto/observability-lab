FROM python:3.12-slim

WORKDIR /app

# Build context is the repo root (see docker-compose.yml) so common/telemetry.py is shared.
COPY services/mock_external_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services/mock_external_api/app ./app

ENV PYTHONPATH=/app

EXPOSE 9000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000"]
