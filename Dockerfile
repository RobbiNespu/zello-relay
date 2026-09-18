FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ZELLO_RELAY_CONFIG=/config/config.json

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY zello_relay.py .

RUN useradd --create-home --uid 1000 relay
USER relay

CMD ["python", "zello_relay.py"]
