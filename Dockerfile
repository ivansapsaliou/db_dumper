FROM python:3.12-slim

# System deps for psycopg2, paramiko, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev libssl-dev openssh-client \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Create default dump directory
RUN mkdir -p /app/dumps

# Expose Flask + Socket.IO port
EXPOSE 5000

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/').read()" || exit 1

CMD ["python3", "app.py"]
