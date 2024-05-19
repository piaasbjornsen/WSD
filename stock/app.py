import logging
import os
import atexit
import uuid
import threading

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

pubsub = db.pubsub()


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post("/item/create/<price>")
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
        db.publish("item_created", key)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"item_id": key})


@app.post("/batch_init/<n>/<starting_stock>/<item_price>")
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
        for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get("/find/<item_id>")
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify({"stock": item_entry.stock, "price": item_entry.price})


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        atomic_set_and_publish(
            item_id, msgpack.encode(item_entry), "stock_events", "stock_added", item_id
        )
        app.logger.info(f"Stock rollback successful for item {item_id}")
    except redis.exceptions.RedisError:
        publish_event("stock_events", "stock_add_failed", item_id)
        app.logger.error(f"DB error during stock rollback for item {item_id}")
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        publish_event("stock_events", "stock_subtract_failed", item_id)
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        atomic_set_and_publish(
            item_id,
            msgpack.encode(item_entry),
            "stock_events",
            "stock_subtracted",
            item_id,
        )
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


def handle_event(message):
    data = message["data"].decode("utf-8")
    event_type, payload = data.split("|", 1)

    if event_type == "order_created":
        order_id = payload
        app.logger.info(f"Order created with ID {order_id}")

    elif event_type == "item_added":
        order_id = payload
        app.logger.info(f"Item added to order {order_id}")

    elif event_type == "order_paid":
        item_id, quantity = payload.split(",")
        app.logger.info(
            f"Order paid, stock subtracted for item {item_id}, quantity {quantity}"
        )

    elif event_type == "order_paid_failed":
        item_id, quantity = payload.split(",")
        app.logger.info(
            f"Payment failed, rollback stock for item {item_id}, quantity {quantity}"
        )
        add_stock(item_id, quantity)


def publish_event(channel: str, event_type: str, payload: str):
    message = f"{event_type}|{payload}"
    db.publish(channel, message)


def atomic_set_and_publish(key, value, channel, event_type, payload):
    pipeline = db.pipeline()
    pipeline.set(key, value)
    pipeline.publish(channel, f"{event_type}|{payload}")
    pipeline.execute()


def event_listener():
    pubsub.subscribe(
        **{
            "order_created": handle_event,
            "item_added": handle_event,
            "order_paid": handle_event,
            "order_paid_failed": handle_event,
        }
    )
    pubsub.run_in_thread(sleep_time=1.0)


event_listener()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8002, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
