import os
import threading
import time
import requests
import logging
from flask import Flask, jsonify, abort

app = Flask("monitoring-service")

SERVICES = {
    "order-service-1": "http://order-service-1:5000/health",
    "order-service-2": "http://order-service-2:5000/health",
    "stock-service-1": "http://stock-service-1:5000/health",
    "stock-service-2": "http://stock-service-2:5000/health",
    "payment-service-1": "http://payment-service-1:5000/health",
    "payment-service-2": "http://payment-service-2:5000/health",
}

RESTART_COMMANDS = {
    "order-service-1": ["docker-compose", "restart", "order-service-1"],
    "order-service-2": ["docker-compose", "restart", "order-service-2"],
    "stock-service-1": ["docker-compose", "restart", "stock-service-1"],
    "stock-service-2": ["docker-compose", "restart", "stock-service-2"],
    "payment-service-1": ["docker-compose", "restart", "payment-service-1"],
    "payment-service-2": ["docker-compose", "restart", "payment-service-2"],
}

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify(status="healthy"), 200

def check_service(service_name, url):
    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Service {service_name} returned status code {response.status_code}")
    except Exception as e:
        app.logger.error(f"Service {service_name} health check failed: {str(e)}")
        restart_service(service_name)

def restart_service(service_name):
    command = RESTART_COMMANDS[service_name]
    app.logger.info(f"Restarting service: {service_name}")
    os.system(" ".join(command))

def monitor_services():
    while True:
        for service_name, url in SERVICES.items():
            check_service(service_name, url)
        time.sleep(60)

if __name__ == "__main__":
    monitor_thread = threading.Thread(target=monitor_services)
    monitor_thread.start()
    app.run(host="0.0.0.0", port=8003, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
