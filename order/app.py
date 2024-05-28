import logging
import os
import atexit
import random
import uuid
import threading
from collections import defaultdict
import time

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ["GATEWAY_URL"]

app = Flask("order-service")

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)
# Create a pub/sub object from the Redis db, used to subscribe and publish to channels 
pubsub = db.pubsub()

# Subscribe to order, stock and payment service channels
pubsub.subscribe("order_events", "stock_events", "payment_events")

# Function to publish events to channel
def publish_event(channel, event):
    retry = 3       
    for _ in range(retry):  
        try:
            db.publish(channel, msgpack.encode(event))
            app.logger.info(f"Published event: {event}")
            return True
        except redis.exceptions.RedisError as e:
            app.logger.warning(f"Failed to publish event {event} on attempt: {e}")
    app.logger.error(f"Failed to publish event after {retry} retries: {event}")
    return False

def close_db_connection():
    db.close()
    app.logger.info("Closed database connection.")

atexit.register(close_db_connection)

# Creates an OrderValue class, with key equal order_id
class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
        app.logger.debug(f"Retrieved order {order_id} from DB.")
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when retrieving order {order_id}: {str(e)}")
        abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        app.logger.error(f"Order: {order_id} not found!")
        abort(400, f"Order: {order_id} not found!")
    return entry


# Create an empty order, with user_id and order_id
@app.post("/create/<user_id>")
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(
        OrderValue(paid=False, items=[], user_id=user_id, total_cost=0)
    )
    try:
        db.set(key, value)
        app.logger.info(f"Created order {key} for user {user_id}.")
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when creating order: {str(e)}")
        abort(400, DB_ERROR_STR)
    return jsonify({"order_id": key})

@app.post("/batch_init/<n>/<n_items>/<n_users>/<item_price>")
def batch_init_orders(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price,
        )
        return value

    kv_pairs: dict[str, bytes] = {
        str(uuid.uuid4()): msgpack.encode(generate_entry()) for _ in range(n)
    }
    try:
        db.mset(kv_pairs)
        app.logger.info(f"Batch initialized {n} orders.")
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error during batch init: {str(e)}")
        abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})

@app.get("/find/<order_id>")
def find_order(order_id: str):
    app.logger.debug(f"Finding order {order_id}.")
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
        }
    )

def send_post_request(url: str):
    app.logger.debug(f"Sending POST request to {url}.")
    try:
        response = requests.post(url)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request error: {str(e)}")
        abort(400, REQ_ERROR_STR)

def send_get_request(url: str):
    app.logger.debug(f"Sending GET request to {url}.")
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request error: {str(e)}")
        abort(400, REQ_ERROR_STR)

@app.post("/addItem/<order_id>/<item_id>/<quantity>")
def add_item(order_id: str, item_id: str, quantity: int):
    app.logger.debug(
        f"Adding item {item_id} to order {order_id} with quantity {quantity}."
    )
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}") 
    if item_reply.status_code != 200:
        app.logger.error(f"Item {item_id} does not exist!")
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
        app.logger.info(f"Item {item_id} added to order {order_id}.")
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when adding item: {str(e)}")
        abort(400, DB_ERROR_STR)
    return Response(
        f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        status=200,
    )

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    publish_event( 
        "order_events",
            {
                "event": "order_checkout",
                "user_id": order_entry.user_id,         
                "order_id": order_id,
                "total_cost": order_entry.total_cost,
                "items": list(items_quantities.items())         # Keep this as dictionary!
            },
    )
    return Response("Checkout initiated", status=200)

def handle_event(event):
    event_type = event.get("event")
    if event_type == "payment_completed":
        handle_payment_completed(event)
    elif event_type == "payment_failed":
        handle_payment_failed(event)
    elif event_type == "inventory_not_available":
        handle_inventory_na(event)
    elif event_type == "inventory_reserved":
        handle_inventory_reserved()

    # Add more event types and handlers as needed



def handle_payment_completed(event):
    # Handle payment completion logic
    order_id = event["order_id"]
    order = get_order_from_db(order_id)
    order.paid = True
    try:
        db.set(order_id, msgpack.encode(order))
        app.logger.info(f"Order {order_id} marked as paid.")

    except redis.exceptions.RedisError as e:
        app.logger.error(f"Failed to update order {order_id}: {str(e)}")        # Should this return 400?

def handle_payment_failed(event):
    # Handle payment failure logic and compensation
    item_id = event["item_id"]
    amount = event["amount"]
    publish_event(
        "order_events",
        {
            "event": "order_compensation",
            "item_id": item_id,
            "amount" : amount, 
            
          }
    
    )


def handle_inventory_na(event):

    publish_event(
            "order_events",
            {
                "event": "order_cancelled",
            }
        )
    
def handle_inventory_reserved():

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
