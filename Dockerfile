FROM python:3.12-slim

# Don't buffer stdout/stderr — important for kubectl logs
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY watcher.py .
COPY cs_helpers.py .
COPY config.py .
COPY pack_parquet_to_csv_zips.py .

USER nobody

CMD ["python", "watcher.py"]
