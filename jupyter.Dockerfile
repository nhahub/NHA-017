# Source: jupyter/pyspark-notebook:spark-3.5.0
FROM jupyter/pyspark-notebook:spark-3.5.0

# Switch to root to install system packages
USER root

# 1. Update apt-get first
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Python Libraries (Added lxml and playwright)
RUN pip install --no-cache-dir \
    pandas \
    scikit-learn \
    psycopg2-binary \
    flask \
    numpy \
    lxml \
    playwright \
    selenium \
    webdriver-manager

# 3. Install Playwright Browsers & System Dependencies
# 'playwright install-deps' installs the Linux libs required to run Chromium
RUN playwright install chromium && \
    playwright install-deps chromium

# Switch back to default user
USER jovyan
