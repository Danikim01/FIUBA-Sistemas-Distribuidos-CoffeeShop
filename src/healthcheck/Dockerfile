FROM python:3.11-slim

# Install docker CLI for docker restart commands
# We'll mount the docker socket from the host, so we only need the CLI
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy healthcheck source code
COPY src/healthcheck /app/src/healthcheck

# Set Python path to include src directory
ENV PYTHONPATH=/app:/app/src

# Run healthchecker
WORKDIR /app/src/healthcheck

CMD ["python", "main.py"]

