FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml README.md /app/
COPY core/ /app/core/
COPY consumers/ /app/consumers/
COPY producers/ /app/producers/
COPY database/ /app/database/

# Install dependencies
RUN pip install --no-cache-dir -e .

# Default to consumer; can override at runtime
CMD ["python", "-m", "consumers.app"]


