FROM python:3.12-slim

WORKDIR /app

COPY backend/ ./backend/

RUN pip install --no-cache-dir -r backend/requirements.txt python-dateutil

CMD ["sh", "-c", "echo 'PORT=$PORT' && echo 'DATABASE_URL set:' && python -c 'import os; print(bool(os.getenv(\"DATABASE_URL\")))' && uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
