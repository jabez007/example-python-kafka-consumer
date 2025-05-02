# syntax=docker/dockerfile:1.4
FROM python:3.11-slim as base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Optional: install system deps
RUN apt-get update && \
    apt-get install -y \
        gcc \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY . /app/


# ---- Pip Setup ----
RUN pip install --upgrade pip && pip install -r requirements.txt


# Default CMD to run consumer
CMD [ "python", "./src/main.py" ]

