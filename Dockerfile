# syntax=docker/dockerfile:1.4
FROM python:3.11-slim as base

LABEL maintainer="Your Name <your.email@example.com>"
LABEL description="Kafka consumer with topic-specific handlers"
LABEL version="0.1.0"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Create a non-root user
RUN adduser --disabled-password --gecos "" appuser


# ---- Pip Setup ----
# Copy requirements first for better caching
COPY requirements.txt /app/

# Install dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# Clean up to reduce image size
RUN apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy project files with appropriate ownership
COPY --chown=appuser:appuser . /app/

USER appuser

ENV PYTHONPATH "${PYTHONPATH}:/app/src"

# Default CMD to run consumer
ENTRYPOINT [ "python" ]
CMD [ "/app/src/main.py" ]

# Health check - assumes your application responds to SIGTERM for graceful shutdown
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f "python /app/src/main.py" || exit 1
