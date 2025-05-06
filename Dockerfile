# syntax=docker/dockerfile:1.4
FROM python:3.11-slim as base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Create a non-root user
RUN adduser --disabled-password --gecos "" appuser

# Optional: install system deps
RUN apt-get update -y && \
    apt-get install -y \
        curl \
    && rm -rf /var/lib/apt/lists/*


# ---- Pip Setup ----
# Copy requirements first for better caching
COPY requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.txt


# Copy project files
COPY . /app/

USER appuser

# Default CMD to run consumer
CMD [ "python", "./src/main.py" ]

