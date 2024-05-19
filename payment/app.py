import logging
import os
import atexit
import uuid
import threading

import redis
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"

app = Flask("payment-service")

db = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

pubsub = db.pubsub()
pubsub.subscribe("payment_events")


def publish_event(channel, event):
    for attempt in range(3):
        try:
            db.publish(channel, msgpack.encode(event))
            app.logger.debug(f"Event published: {event}")
            return True
        except redis.exceptions.RedisError as e:
            app.logger.warning(
                f"Failed to publish event: {event}, attempt {attempt+1}, error: {str(e)}"
            )
    app.logger.error(f"Failed to publish event after retries: {event}")
    return False


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry = db.get(user_id)
        app.logger.debug(f"Retrieved user entry from DB: {entry}")
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when retrieving user {user_id}: {str(e)}")
        abort(400, DB_ERROR_STR)
    entry = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        app.logger.error(f"User: {user_id} not found!")
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post("/create_user")
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
        publish_event("payment_events", {"event": "user_created", "user_id": key})
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when creating user: {str(e)}")
        abort(400, DB_ERROR_STR)
    return jsonify({"user_id": key})


@app.post("/batch_init/<n>/<starting_money>")
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs = {
        str(uuid.uuid4()): msgpack.encode(UserValue(credit=starting_money))
        for _ in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error during batch init: {str(e)}")
        abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get("/find_user/<user_id>")
def find_user(user_id: str):
    user_entry = get_user_from_db(user_id)
    return jsonify({"user_id": user_id, "credit": user_entry.credit})


@app.post("/add_funds/<user_id>/<amount>")
def add_credit(user_id: str, amount: int):
    app.logger.debug(f"Adding {amount} credit to user: {user_id}")
    user_entry = get_user_from_db(user_id)
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
        publish_event(
            "payment_events",
            {"event": "funds_added", "user_id": user_id, "amount": amount},
        )
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when adding funds: {str(e)}")
        abort(400, DB_ERROR_STR)
    return Response(
        f"User: {user_id} credit updated to: {user_entry.credit}", status=200
    )


@app.post("/pay/<user_id>/<amount>")
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    try:
        with db.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(user_id)
                    user_entry = get_user_from_db(user_id)
                    if user_entry.credit < int(amount):
                        pipe.unwatch()
                        app.logger.error(f"User: {user_id} has insufficient credit")
                        abort(
                            400,
                            f"User: {user_id} credit cannot get reduced below zero!",
                        )
                    user_entry.credit -= int(amount)
                    pipe.multi()
                    pipe.set(user_id, msgpack.encode(user_entry))
                    pipe.execute()
                    app.logger.info(
                        f"User: {user_id} credit updated to: {user_entry.credit}"
                    )

                    if not publish_event(
                        "payment_events",
                        {
                            "event": "payment_completed",
                            "user_id": user_id,
                            "amount": amount,
                        },
                    ):
                        app.logger.error(
                            f"Failed to publish payment event for user: {user_id}, rolling back"
                        )
                        with db.pipeline() as rollback_pipe:
                            while True:
                                try:
                                    rollback_pipe.watch(user_id)
                                    user_entry = get_user_from_db(user_id)
                                    user_entry.credit += int(amount)
                                    rollback_pipe.multi()
                                    rollback_pipe.set(
                                        user_id, msgpack.encode(user_entry)
                                    )
                                    rollback_pipe.execute()
                                    break
                                except redis.WatchError:
                                    continue
                        raise redis.exceptions.RedisError(
                            "Failed to publish payment event"
                        )
                    break
                except redis.WatchError as e:
                    app.logger.warning(
                        f"WatchError occurred, retrying... error: {str(e)}"
                    )
                    continue
    except redis.exceptions.RedisError as e:
        app.logger.error(
            f"DB error when removing credit for user: {user_id}, error: {str(e)}"
        )
        abort(400, DB_ERROR_STR)
    return Response(
        f"User: {user_id} credit updated to: {user_entry.credit}", status=200
    )

event_listener()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
