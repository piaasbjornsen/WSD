# payment/app.py (partial implementation)
import json
import logging
import os
import atexit
import uuid

import redis
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

# Importing event_bus from common
from common.event_bus import EventBus

GATEWAY_URL = os.environ['GATEWAY_URL']

DB_ERROR_STR = "DB error"

app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

event_bus = EventBus(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    db=int(os.environ['REDIS_DB']),
    password=os.environ['REDIS_PASSWORD']
)


# The rest of your code...


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/reserve_credit/<user_id>/<amount>')
def reserve_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    if user_entry.credit >= amount:
        user_entry.credit -= int(amount)
        try:
            db.set(user_id, msgpack.encode(user_entry))
            event = {"type": "CreditReserved", "data": {"user_id": user_id, "amount": amount}}
            event_bus.publish("CreditReserved", event)
        except redis.exceptions.RedisError:
            return abort(400, DB_ERROR_STR)
        return Response(f"User: {user_id} credit reserved: {amount}", status=200)
    else:
        event = {"type": "CreditReservationFailed", "data": {"user_id": user_id, "amount": amount}}
        event_bus.publish("CreditReservationFailed", event)
        return abort(400, f"User: {user_id} not enough credit")


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


# Event handler
def handle_credit_reserved(event_data):
    order_id = event_data['order_id']
    # Update order to paid and finalize the transaction
    # Handle stock deduction
    event = {"type": "CheckoutCompleted", "data": {"order_id": order_id}}
    event_bus.publish("CheckoutCompleted", event)


# Subscribing to events
pubsub = event_bus.subscribe("CreditReserved")
for message in pubsub.listen():
    if message['type'] == 'message':
        event = json.loads(message['data'])
        if event['type'] == "CreditReserved":
            handle_credit_reserved(event['data'])


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
