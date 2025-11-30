# Start from the official Airflow image
FROM apache/airflow:2.9.2

# Switch to root to install software
USER root

# Install the Docker CLI (Client)
RUN apt-get update && \
    apt-get install -y docker.io

# Switch back to airflow user? 
# NO. For this specific 'Docker-in-Docker' setup to work easily, 
# we will stay as root.
