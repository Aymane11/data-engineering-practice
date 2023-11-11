FROM python:3.9.8-slim-buster

WORKDIR /app
COPY . /app

ENV PYTHONUNBUFFERED=1

RUN python3 -m pip install -r requirements.txt