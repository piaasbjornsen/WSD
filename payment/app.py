import logging
import os
import atexit
import uuid
import json
from kafka import KafkaProducer, KafkaConsumer
import redis
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
import threading

DB_ERROR_STR = "DB error"

app = Flask("payment-service")

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        abort(400, f"User: {user_id} not found!")
    return entry


def emit_event(event_data: dict):
    producer.send('payment_events', value=event_data)
    producer.flush()  # Ensure message is sent immediately


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    emit_event({
        "event_type": "UserCreated",
        "user_id": key
    })
    
    return jsonify({'user_id': key})


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    emit_event({
        "event_type": "CreditAdded",
        "user_id": user_id,
        "amount": amount
    })
    
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    emit_event({
        "event_type": "CreditRemoved",
        "user_id": user_id,
        "amount": amount
    })
    
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

def handle_order_initiated(event_data):
    user_id = event_data['user_id']
    total_cost = event_data['total_cost']
    order_id = event_data['order_id']
    items = event_data['items']

    user_entry = get_user_from_db(user_id)
    if user_entry.credit >= total_cost:
        user_entry.credit -= total_cost
        try:
            db.set(user_id, msgpack.encode(user_entry))
        except redis.exceptions.RedisError:
            emit_event({
                "event_type": "CreditReservationFailed",
                "reason": "Database error during credit reservation",
                "order_id": order_id,
                "user_id": user_id,
                "total_cost": total_cost,
                "items": items
            })
            return Response("Database error during credit reservation", status=500)

        emit_event({
            "event_type": "CreditReserved",
            "order_id": order_id,
            "user_id": user_id,
            "total_cost": total_cost,
            "items": items
        })
        return Response("Credit reserved successfully", status=200)
    else:
        emit_event({
            "event_type": "CreditReservationFailed",
            "order_id": order_id,
            "user_id": user_id,
            "total_cost": total_cost,
            "items": items,
            "reason": "Insufficient credit"
        })
        return Response("Insufficient credit", status=400)
    
#If checkout fails, release credit to the user again
def handle_release_credit(event_data):
    user_id = event_data['user_id']
    total_cost = event_data.get('total_cost', 0)
    user_entry = get_user_from_db(user_id)
    user_entry.credit += total_cost
    try:
        db.set(user_id, msgpack.encode(user_entry))
        return Response("Credit reserved successfully", status=200)
    except redis.exceptions.RedisError:
        app.logger.error("Failed to release credit for user: %s", user_id)
        return Response("Failed to reserve credit", status=500)


def consume_events():
    consumer = KafkaConsumer('order_events',
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest',
                             group_id='payment_group',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for event in consumer:
        event_type = event.value.get('event_type')
        if event_type == 'OrderInitiated':
            handle_order_initiated(event.value)
        elif event_type == 'ReleaseCredit':
            handle_release_credit(event.value)


if __name__ == '__main__':
    consumer_thread = threading.Thread(target=consume_events)
    consumer_thread.start()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
