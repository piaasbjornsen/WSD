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
pubsub.subscribe("stock_events", "payment_events", "order_events")


def publish_event(channel, event):
    """
    Publish an event to a Redis channel, with retries on failure.
    """
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
    """
    Close the Redis connection on application exit.
    """
    db.close()

# Register the database connection close function to be called on exit
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
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


def handle_payment_reserve(event):
    """
    Handle payment reservation event by deducting the reserved amount from user's credit.
    """
    order_id = event["order_id"]
    user_id = event["user_id"]
    total_cost = event["total_cost"]
    items = event["items"]

    try:
        # Retrieve user information from the database
        user_entry = get_user_from_db(user_id)

        # Check if user has enough credit for the payment
        if user_entry.credit < total_cost:
            # Publish event indicating payment failure due to insufficient credit
            publish_event(
                "payment_events",
                {
                    "event": "payment_failed",
                    "order_id": order_id,
                    "user_id": user_id,
                    "total_cost": total_cost,
                    "items": items
                }
            )
            app.logger.error(f"Payment failed for order: {order_id}, user: {user_id}. Insufficient credit.")
            return Response("Insufficient credit. Payment failed.", status=400)

        # Deduct the total cost of the reserved items from the user's credit
        user_entry.credit -= total_cost

        # Update user's credit in the database
        db.set(user_id, msgpack.encode(user_entry))

        app.logger.info(f"Payment reserved for order: {order_id}, user: {user_id}")

        # Publish event indicating successful payment reservation
        publish_event(
            "payment_events",
            {
                "event": "payment_completed",
                "order_id": order_id,
                "user_id": user_id,
                "total_cost": total_cost,
                "items": items
            }
        )
        return Response("Payment reserved successfully", status=200)
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error during payment reservation: {str(e)}")
        # Publish an event indicating that the credit reservation failed
        publish_event(
            "payment_events",
            {
                "event": "payment_failed",
                "order_id": order_id,
                "user_id": user_id,
                "total_cost": total_cost,
                "items": items
            }
        )
        return abort(400, DB_ERROR_STR)


def handle_payment_compensation(event):
    """
    Handle payment compensation event by adding back the reserved amount to the user's credit.
    """
    order_id = event["order_id"]
    user_id = event["user_id"]
    total_cost = event["total_cost"]
    items = event["items"]

    try:
        # Retrieve user information from the database
        user_entry = get_user_from_db(user_id)

        # Add the total cost of the reserved items back to the user's credit
        user_entry.credit += total_cost

        # Update user's credit in the database
        db.set(user_id, msgpack.encode(user_entry))

        app.logger.info(f"Payment compensation for order: {order_id}, user: {user_id}")

        # Publish event indicating successful payment compensation
        publish_event(
            "payment_events",
            {
                "event": "payment_compensated",
                "order_id": order_id,
                "user_id": user_id,
                "total_cost": total_cost,
                "items": items
            }
        )

        return Response("Payment compensation successful", status=200)
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error during payment compensation: {str(e)}")
        # Publish an event indicating that the payment compensation failed
        publish_event(
            "payment_events",
            {
                "event": "payment_compensation_failed",
                "order_id": order_id,
                "user_id": user_id,
                "total_cost": total_cost,
                "items": items
            }
        )
        return Response("Payment compensation failed", status=400)


def consume_events():
    """
    Consume and handle events from the Redis Pub/Sub channels.
    """
    for message in pubsub.listen():
        event = msgpack.decode(message["data"])
        event_type = event.get("event")
        if event_type == "inventory_reserved":
            handle_payment_reserve(event)
        elif event_type == "order_compensation":
            handle_payment_compensation(event)


if __name__ == '__main__':
    # Start the event consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_events)
    consumer_thread.start()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
