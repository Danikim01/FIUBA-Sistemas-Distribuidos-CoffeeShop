# Gateway
FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY src/gateway/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy middleware
COPY middleware ./middleware

# Copy healthcheck source code preserving package structure
COPY src/healthcheck /app/src/healthcheck

# Copy common source code preserving package structure
COPY src/common /app/src/common

# Copy gateway source code
COPY src/gateway/ .

# Copy workers source code preserving the package layout
COPY src/workers /app/src/workers

# Set Python path to include src directory
ENV PYTHONPATH=/app:/app/src


EXPOSE 12345

CMD ["python", "main.py"]