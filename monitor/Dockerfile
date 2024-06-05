FROM python:3.12-slim

WORKDIR /home/flask-app

# Install curl
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE 8003

CMD ["gunicorn", "-b", "0.0.0.0:8003", "app:app"]
