FROM python:3.11-slim

# Initial working directory
WORKDIR /app

# Copy requirements first for better caching
COPY src/workers/requirements.txt ./requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy middleware
COPY middleware /app/middleware

# Copy workers source code preserving the package layout
COPY src/workers /app/src/workers

# copy healthcheck source code
COPY src/healthcheck /app/src/healthcheck

# Ensure all modules are importable
ENV PYTHONPATH=/app:/app/src:/app/src/workers:/app/src/workers/utils

# Run commands from workers directory by default
WORKDIR /app/src/workers

# Default command (can be overridden)
CMD ["python", "filter/year.py"]
