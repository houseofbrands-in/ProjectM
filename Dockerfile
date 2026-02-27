FROM python:3.12-slim

WORKDIR /app

COPY backend/ ./backend/

RUN pip install --no-cache-dir -r backend/requirements.txt python-dateutil

ENV PORT=8000

CMD ["sh", "-c", "python -c 'print(\"Starting app on port $PORT\")' && exec uvicorn backend.main:app --host 0.0.0.0 --port $PORT"]
