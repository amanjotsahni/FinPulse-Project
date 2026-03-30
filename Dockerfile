FROM python:3.10-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HEADLESS_MODE=1

WORKDIR /app

# Install system dependencies if required (e.g. for curl_cffi)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . /app

# Ensure correct path resolution
ENV PYTHONPATH=/app

# Default command to run the pipeline
CMD ["python", "prefect_flows/finpulse_pipeline.py"]
