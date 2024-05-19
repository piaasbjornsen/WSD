import logging
import os
import atexit
import random
import uuid
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

pubsub = db.pubsub()
pubsub.subscribe("order_events")


def publish_event(channel, event):
    for _ in range(3):  # Retry 3 times
        try:
            db.publish(channel, msgpack.encode(event))
            app.logger.info(f"Published event: {event}")
            return True
        except redis.exceptions.RedisError as e:
            app.logger.warning(f"Failed to publish event {event} on attempt: {e}")
    app.logger.error(f"Failed to publish event after retries: {event}")
    return False


def close_db_connection():
    db.close()
    app.logger.info("Closed database connection.")


atexit.register(close_db_connection)


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


@app.post("/create/<user_id>")
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(
        OrderValue(paid=False, items=[], user_id=user_id, total_cost=0)
    )
    try:
        db.set(key, value)
        app.logger.info(f"Created order {key} for user {user_id}.")
        publish_event(
            "order_events",
            {"event": "order_created", "order_id": key, "user_id": user_id},
        )
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
        publish_event(
            "order_events",
            {
                "event": "item_added",
                "order_id": order_id,
                "item_id": item_id,
                "quantity": quantity,
            },
        )
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error when adding item: {str(e)}")
        abort(400, DB_ERROR_STR)
    return Response(
        f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        status=200,
    )


def handle_order_events():
    for message in pubsub.listen():
        if message["type"] == "message":
            event = msgpack.decode(message["data"])
            if event["event"] == "payment_completed":
                order_id = event["order_id"]
                order = get_order_from_db(order_id)
                order.paid = True
                try:
                    db.set(order_id, msgpack.encode(order))
                    app.logger.info(f"Order {order_id} marked as paid.")
                except redis.exceptions.RedisError as e:
                    app.logger.error(f"Failed to update order {order_id}: {str(e)}")


def retry_post_request(url: str, max_retries: int = 3, backoff_factor: float = 0.5):
    app.logger.debug(f"Retrying POST request to {url} with max retries {max_retries}.")
    for attempt in range(max_retries):
        try:
            response = requests.post(url)
            if response.status_code == 200:
                return response
        except requests.exceptions.RequestException as e:
            app.logger.warning(
                f"Request error on attempt {attempt+1} for URL {url}: {str(e)}"
            )
            if attempt == max_retries - 1:
                abort(400, "Failed to send POST request after several attempts")
            time.sleep(backoff_factor * (2**attempt))
    return None


def rollback_stock(removed_items: list[tuple[str, int]]):
    app.logger.debug(f"Rolling back stock for items: {removed_items}")
    for item_id, quantity in removed_items:
        retry_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


def rollback_payment(user_id: str, amount: int):
    app.logger.debug(f"Rolling back payment for user {user_id} amount {amount}.")
    retry_post_request(f"{GATEWAY_URL}/payment/add_funds/{user_id}/{amount}")


@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    app.logger.debug(f"Checking out order {order_id}.")
    order_entry: OrderValue = get_order_from_db(order_id)
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = retry_post_request(
            f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}"
        )
        if not stock_reply or stock_reply.status_code != 200:
            app.logger.error(f"Out of stock for item {item_id}. Rolling back.")
            rollback_stock(removed_items)
            abort(400, f"Out of stock on item_id: {item_id}")
        removed_items.append((item_id, quantity))
    user_reply = retry_post_request(
        f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}"
    )
    if not user_reply or user_reply.status_code != 200:
        app.logger.error(f"User {order_entry.user_id} out of credit. Rolling back.")
        rollback_stock(removed_items)
        rollback_payment(order_entry.user_id, order_entry.total_cost)
        abort(400, "User out of credit")
    order_entry.paid = True
    try:
        db.set(order_id, msgpack.encode(order_entry))
        app.logger.info(f"Checkout successful for order {order_id}.")
        publish_event(
            "order_events", {"event": "payment_completed", "order_id": order_id}
        )
    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error during checkout for order {order_id}: {str(e)}")
        abort(400, DB_ERROR_STR)
    return Response("Checkout successful", status=200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
