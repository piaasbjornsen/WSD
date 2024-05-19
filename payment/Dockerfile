FROM python:3.12-slim

WORKDIR /home/flask-app

COPY ./requirements.txt .
COPY common /home/flask-app/common

RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000