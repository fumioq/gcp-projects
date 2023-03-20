# Use the official Python image.
# https://hub.docker.com/_/python
FROM python:3.9-slim

# Install manually all the missing libraries
# RUN apt-get update

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Setting Sao Paulo timezone
RUN apt update && apt install tzdata -y
ENV TZ="America/Sao_Paulo"

# Install Python dependencies.
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . .

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 main:app