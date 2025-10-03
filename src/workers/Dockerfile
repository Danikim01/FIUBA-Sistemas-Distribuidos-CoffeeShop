FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY src/workers/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy middleware
COPY middleware ./middleware

# Copy workers source code
COPY src/workers/ .

# Set Python path
ENV PYTHONPATH=/app

# Default command (can be overridden)
CMD ["python", "filter/year.py"]
