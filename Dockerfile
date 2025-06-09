# PestRoutes to Snowflake Pipeline Docker Image
FROM python:3.11-slim

# Set working directory
WORKDIR /opt/pestroutes-pipeline

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set Python path
ENV PYTHONPATH=/opt/pestroutes-pipeline

# Default command (can be overridden by Prefect)
CMD ["python", "-m", "flows.main_pipeline"]