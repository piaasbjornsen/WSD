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
pubsub.subscribe("stock_events", "payment_events")



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
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when adding stock: {str(e)}")
        abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post("/subtract/<item_id>/<amount>")
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


def handle_event(event):
    event_type = event.get("event")
    if event_type == "order_checkout":
        handle_order_checkout(event)
    elif event_type == "order_compensation":
        handle_order_compensation(event)

def handle_order_checkout(event):
    items = event["items"]  # Dict of id, quantity 
    stock_available = True

    for item_id, quantity in items:
        item_entry = get_item_from_db(item_id)
        if item_entry.stock < quantity:
            stock_available = False
            app.logger.debug(f"Item: {item_id} stock is {item_entry.stock}, request is for {quantity}")
    if stock_available: 
        for item_id, quantity in items:
            remove_stock(item_id, quantity)
            publish_event(
                "stock_events",
                {
                    "event": "inventory_reserved",
                    "order_id": event["order_id"],
                    "user_id": event["user_id"],
                    "total_cost": event["total_cost"],
                    "items": items
                }
            )
    else:
        publish_event(
            "stock_events" ,
            {
                "event": "inventory_not_available",
                "order_id": event["order_id"],
                "user_id": event["user_id"],
                "total_cost": event["total_cost"],
                "items": items
            }
        )

    
def handle_order_compensation(event):
    add_stock

def consume_events():
    for message in pubsub.listen():
        if message["type"] == "message":
            event = msgpack.decode(message["data"])
            handle_event(event)


if __name__ == '__main__':
    # Start the event consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_events)
    consumer_thread.start()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
