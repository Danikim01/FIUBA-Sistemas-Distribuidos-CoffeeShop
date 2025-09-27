# Gateway
FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY src/gateway/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy middleware
COPY middleware ./middleware

# Copy gateway source code
COPY src/gateway/ .

# Set Python path
ENV PYTHONPATH=/app


EXPOSE 12345

CMD ["python", "main.py"]