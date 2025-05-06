# syntax=docker/dockerfile:1.4
FROM python:3.11-slim as base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Create a non-root user
RUN adduser --disabled-password --gecos "" appuser


# ---- Pip Setup ----
# Copy requirements first for better caching
COPY requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# Copy project files
COPY . /app/

USER appuser

# Default CMD to run consumer
ENTRYPOINT [ "python" ]
CMD [ "/app/src/main.py" ]

