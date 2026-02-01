FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY router.py .

# Create non-root user
RUN useradd -m -u 1000 router && \
    chown -R router:router /app

USER router

EXPOSE 8000

CMD ["python", "router.py"]
