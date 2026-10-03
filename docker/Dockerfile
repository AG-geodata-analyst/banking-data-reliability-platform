FROM apache/airflow:3.1.3-python3.12

# Switch to root to install system packages if needed later
USER root

# Add any system-level dependencies here (e.g., gcc, libpq-dev)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     && rm -rf /var/lib/apt/lists/*

USER airflow

# Project Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
