FROM python:3.10-slim

# Install uv directly from the official astral container
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HEADLESS_MODE=1 \
    UV_SYSTEM_PYTHON=1

WORKDIR /app

# Install system dependencies if required (e.g. for curl_cffi)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies strictly bounded by lock file
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-cache

# Copy application source code
COPY . /app

# Ensure correct path resolution
ENV PYTHONPATH=/app

# Run as non-root for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Default command to run the pipeline
CMD ["python", "prefect_flows/finpulse_pipeline.py"]
