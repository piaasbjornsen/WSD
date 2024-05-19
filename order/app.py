import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import redis
import requests

from kafka import KafkaProducer, KafkaConsumer
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
import threading
import json


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry


# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def emit_event(event_data):
    producer.send('order_events', value=event_data)
    producer.flush()


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    emit_event({
        "event_type": "OrderCreated",
        "order_id": key,
        "user_id": user_id
    })

    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2 * item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    emit_event({
        "event_type": "OrderInitiated",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
        "items": list(items_quantities.items())
    })

    return Response("Checkout initiated", status=200)

#If enough credit is reserved, check if we can reserve enough stock
def handle_credit_reserved(event_data):
    user_id = event_data['user_id']
    total_cost = event_data['total_cost']
    order_id = event_data['order_id']
    items = event_data['items']

    emit_event({
        "event_type": "StockCheck",
        "order_id": order_id,
        "user_id": user_id,
        "total_cost": total_cost,
        "items": items
    })

#If there is not enough credit, tell them that checkout cant proceed and fail checkout
def handle_credit_reservation_failed(event_data):
    user_id = event_data['user_id']
    total_cost = event_data['total_cost']
    order_id = event_data['order_id']
    items = event_data['items']
    reason = event_data.get('reason', 'Unknown reason')

    emit_event({
        "event_type": "CheckoutFailed",
        "order_id": order_id,
        "user_id": user_id,
        "total_cost": total_cost,
        "items": items,
        "reason": f"Credit reservation failed: {reason}"
    })

#If stock is reserved, this means both payment and stock is reserved so checkout can be completed
def handle_stock_checked(event_data):
    order_id = event_data['order_id']
    order_entry: OrderValue = get_order_from_db(order_id)
    order_entry.paid = True

    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        emit_event({
            "event_type": "CheckoutFailed",
            "order_id": order_id,
            "reason": "Database error while finalizing checkout"
        })
        return

    emit_event({
        "event_type": "CheckoutCompleted",
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost
    })

#If stock cant be reserve, lead to checkoutfailed
def handle_stock_reservation_failed(event_data):
    user_id = event_data['user_id']
    total_cost = event_data['total_cost']
    order_id = event_data['order_id']
    items = event_data['items']
    reason = event_data.get('reason', 'Unknown reason')

    emit_event({
        "event_type": "CheckoutFailed",
        "order_id": order_id,
        "user_id": user_id,
        "total_cost": total_cost,
        "items": items,
        "reason": f"Stock reservation failed: {reason}"
    })


# def handle_checkout_completed(event_data):
#     pass


#If checkout fails, release credit
def handle_checkout_failed(event_data):
    user_id = event_data['user_id']
    total_cost = event_data['total_cost']
    order_id = event_data['order_id']
    items = event_data['items']
    reason = event_data['reason']

    emit_event({
        "event_type": "ReleaseCredit",
        "order_id": order_id,
        "user_id": user_id,
        "total_cost": total_cost,
        "items": items,
        "reason": reason
    })

    emit_event({
        "event_type": "ReleaseStock",
        "order_id": order_id,
        "user_id": user_id,
        "total_cost": total_cost,
        "items": items,
        "reason": reason
    })


def consume_events():
    consumer = KafkaConsumer('payment_events', 'stock_events', 'order_events',
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest',
                             group_id='order_group',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for event in consumer:
        event_type = event.value.get('event_type')
        if event_type == 'CreditReserved':
            handle_credit_reserved(event.value)
        elif event_type == 'CreditReservationFailed':
            handle_credit_reservation_failed(event.value)
        elif event_type == 'StockChecked':
            handle_stock_checked(event.value)
        elif event_type == 'StockReservationFailed':
            handle_stock_reservation_failed(event.value)
        # elif event_type == 'CheckoutCompleted':
        #     handle_checkout_completed(event.value)
        elif event_type == 'CheckoutFailed':
            handle_checkout_failed(event.value)


if __name__ == '__main__':
    consumer_thread = threading.Thread(target=consume_events)
    consumer_thread.start()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
   
