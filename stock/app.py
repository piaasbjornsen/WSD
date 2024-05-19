import logging
import os
import atexit
import uuid
import time
import threading

import redis
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

pubsub = db.pubsub()
pubsub.subscribe("stock_events")


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


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry = db.get(item_id)
        app.logger.debug(f"Retrieved item entry from DB: {entry}")
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when retrieving item {item_id}: {str(e)}")
        abort(400, DB_ERROR_STR)
    entry = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        app.logger.error(f"Item: {item_id} not found!")
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post("/item/create/<price>")
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Creating item with ID: {key} and price: {price}")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
        publish_event(
            "stock_events", {"event": "item_created", "item_id": key, "price": price}
        )
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when creating item: {str(e)}")
        abort(400, DB_ERROR_STR)
    return jsonify({"item_id": key})


@app.post("/batch_init/<n>/<starting_stock>/<item_price>")
def batch_init_items(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs = {
        str(uuid.uuid4()): msgpack.encode(
            StockValue(stock=starting_stock, price=item_price)
        )
        for _ in range(n)
    }
    try:
        db.mset(kv_pairs)
        app.logger.debug(
            f"Batch initialized {n} items with stock: {starting_stock} and price: {item_price}"
        )
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error during batch init: {str(e)}")
        abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get("/find/<item_id>")
def find_item(item_id: str):
    item_entry = get_item_from_db(item_id)
    return jsonify(
        {"item_id": item_id, "stock": item_entry.stock, "price": item_entry.price}
    )


@app.post("/add/<item_id>/<amount>")
def add_stock(item_id: str, amount: int):
    app.logger.debug(f"Adding {amount} stock to item: {item_id}")
    item_entry = get_item_from_db(item_id)
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
        publish_event(
            "stock_events",
            {"event": "stock_added", "item_id": item_id, "amount": amount},
        )
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when adding stock: {str(e)}")
        abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    app.logger.debug(f"Removing {amount} stock from item: {item_id}")
    retries = 0
    max_retries = 5
    backoff_factor = 0.5

    try:
        with db.pipeline() as pipe:
            while retries < max_retries:
                try:
                    pipe.watch(item_id)
                    item_entry = get_item_from_db(item_id)
                    if item_entry.stock < int(amount):
                        pipe.unwatch()
                        app.logger.error(
                            f"Item: {item_id} stock cannot get reduced below zero!"
                        )
                        abort(
                            400, f"Item: {item_id} stock cannot get reduced below zero!"
                        )
                    item_entry.stock -= int(amount)
                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item_entry))
                    pipe.publish(
                        "stock_events",
                        msgpack.encode(
                            {
                                "event": "stock_subtracted",
                                "item_id": item_id,
                                "amount": amount,
                            }
                        ),
                    )
                    pipe.execute()
                    app.logger.info(
                        f"Item: {item_id} stock updated to: {item_entry.stock}"
                    )
                    break
                except redis.WatchError as e:
                    retries += 1
                    app.logger.warning(
                        f"WatchError occurred, retrying... attempt {retries}, error: {str(e)}"
                    )
                    time.sleep(backoff_factor * (2**retries))  # Exponential backoff
                    continue
                except redis.exceptions.RedisError as e:
                    app.logger.error(f"DB error when removing stock: {str(e)}")
                    abort(400, DB_ERROR_STR)
            else:
                app.logger.error(
                    f"Failed to remove stock for item {item_id} after {max_retries} retries"
                )
                abort(500, "Failed to update stock after multiple retries")
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when removing stock: {str(e)}")
        abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8002, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
