import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"

app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

# pub and sub to events
pubsub = db.pubsub()
pubsub.subscribe('payment_events')

def publish_event(channel, event):
    for _ in range(3):  # Retry 3 times
        try:
            db.publish(channel, msgpack.encode(event))
            return True
        except redis.exceptions.RedisError:
            continue
    app.logger.error(f"Failed to publish event: {event}")
    return False

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

@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
        publish_event('payment_events', {'event': 'user_created', 'user_id': key})
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})

@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money)) for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})

@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify({
        "user_id": user_id,
        "credit": user_entry.credit
    })

@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
        publish_event('payment_events', {'event': 'funds_added', 'user_id': user_id, 'amount': amount})
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    try:
        with db.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(user_id)
                    user_entry: UserValue = get_user_from_db(user_id)
                    if user_entry.credit < int(amount):
                        pipe.unwatch()
                        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
                    user_entry.credit -= int(amount)
                    pipe.multi()
                    pipe.set(user_id, msgpack.encode(user_entry))
                    pipe.execute()
                    if not publish_event('payment_events', {'event': 'payment_completed', 'user_id': user_id, 'amount': amount}):
                        # If event publishing fails, rollback the credit deduction
                        pipe.watch(user_id)
                        user_entry.credit += int(amount)
                        pipe.multi()
                        pipe.set(user_id, msgpack.encode(user_entry))
                        pipe.execute()
                        raise redis.exceptions.RedisError("Failed to publish payment event")
                    break
                except redis.WatchError:
                    continue
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
