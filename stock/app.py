import logging
import os
import atexit
import uuid
import time
import threading

import redis
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

# Constant for database error messages
DB_ERROR_STR = "DB error"

# Initialize the Flask app
app = Flask("stock-service")

# Initialize Redis connection using environment variables
db = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)

# Initialize Redis Pub/Sub
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

def handle_order_checkout(event):
    """
    Handle order checkout event by reserving stock if available.
    """
    items = event["items"]  # Dict of id, quantity
    stock_available = True

    # Check if there is enough stock for all items
    for item_id, quantity in items.items():
        item_entry = get_item_from_db(item_id)
        if item_entry.stock < quantity:
            stock_available = False
            app.logger.debug(f"Item: {item_id} stock is {item_entry.stock}, request is for {quantity}")
            break

    if stock_available:
        try:
            # Start a Redis pipeline to perform batch operations atomically
            with db.pipeline() as pipe:
                for item_id, quantity in items.items():
                    item_entry = get_item_from_db(item_id)
                    item_entry.stock -= quantity
                    # Queue the update to the Redis pipeline
                    # The pipe.set() command queues up a set operation to be executed as part of the pipeline.
                    pipe.set(item_id, msgpack.encode(item_entry))

                # Execute all the queued commands in the pipeline atomically
                # The pipe.execute() command then executes all the queued commands in the pipeline in sequence, as a single atomic operation. 
                # It sends all the commands to the Redis server, which processes them in the order they were queued. 
                #If any command fails during execution, none of the commands in the pipeline will be applied to the data in Redis. 
                #This ensures data integrity and consistency.
                pipe.execute()

            # Publish an event indicating that inventory has been reserved
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
            return Response("Inventory reserved successfully", status=200)
        except redis.exceptions.RedisError as e:
            app.logger.error(f"DB error during batch stock update: {str(e)}")
            # Publish an event indicating that the inventory update failed
            publish_event(
                "stock_events",
                {
                    "event": "inventory_not_available",
                    "order_id": event["order_id"],
                    "user_id": event["user_id"],
                    "total_cost": event["total_cost"],
                    "items": items
                }
            )
            abort(400, DB_ERROR_STR)
    else:
        # Publish an event indicating that inventory is not available
        publish_event(
            "stock_events",
            {
                "event": "inventory_not_available",
                "order_id": event["order_id"],
                "user_id": event["user_id"],
                "total_cost": event["total_cost"],
                "items": items
            }
        )
        return Response("Inventory not available", status=400)


def handle_order_compensation(event):
    """
    Handle order compensation event by adding stock back to the inventory.
    """
    items = event["items"]  # Dict of item_id, quantity
    order_id = event["order_id"]
    user_id = event["user_id"]
    app.logger.debug(f"Handling order compensation for order_id: {order_id}, user_id: {user_id}")

    compensation_successful = True

    try:
        with db.pipeline() as pipe:
            for item_id, quantity in items.items():
                # Retrieve the current stock for the item
                item_entry = get_item_from_db(item_id)
                # Calculate the compensated stock (add back the quantity)
                compensated_stock = item_entry.stock + quantity
                # Update the stock in the database
                item_entry.stock = compensated_stock
                pipe.set(item_id, msgpack.encode(item_entry))
                app.logger.debug(f"Compensated {quantity} stock for item: {item_id}")

        # Execute all the queued commands in the pipeline atomically
        pipe.execute()
    except Exception as e:
        app.logger.error(f"Failed to compensate stock for order_id: {order_id}, user_id: {user_id}, error: {str(e)}")
        compensation_successful = False

    if compensation_successful:
        response_message = "Order compensation successful"
        event_data = {
            "event": "order_compensated",
            "order_id": order_id,
            "user_id": user_id,
            "items": items
        }
    else:
        response_message = "Failed to compensate order. Please check logs for details."
        event_data = {
            "event": "order_compensation_failed",
            "order_id": order_id,
            "user_id": user_id,
            "items": items
        }

    publish_event("stock_events", event_data)
    return Response(response_message, status=200 if compensation_successful else 500)


def consume_events():
    """
    Consume and handle events from the Redis Pub/Sub channels.
    """
    for message in pubsub.listen():
        event = msgpack.decode(message["data"])
        event_type = event.get("event")
        if event_type == "order_checkout":
            handle_order_checkout(event)

        #For both checkout failed as well as payment failed, we need to rollback
        elif event_type == "payment_failed":
            handle_order_compensation(event)
        elif event_type == "order_compensation":
            handle_order_compensation(event)

if __name__ == '__main__':
    # Start the event consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_events)
    consumer_thread.start()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    # Set up logging for production use with Gunicorn
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)