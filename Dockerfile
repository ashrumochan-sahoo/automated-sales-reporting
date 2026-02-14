# ============================================
# Automated Sales Reporting Pipeline
# Docker Container Definition
# Author: Ashrumochan Sahoo
# ============================================

# Base image - Python 3.9 slim (small size)
FROM python:3.9-slim

# Set working directory inside container
WORKDIR /app

# Copy requirements first (Docker caching trick)
# If requirements don't change, this layer is cached
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy entire project into container
COPY . .

# Create necessary directories
RUN mkdir -p data/raw data/processed data/tableau_export logs

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Default command - run the pipeline
CMD ["python3", "-m", "pipeline.main"]
