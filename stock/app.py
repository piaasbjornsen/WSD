# stock/app.py (partial implementation)
import logging
import os
import atexit
import uuid

import redis
import requests
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

GATEWAY_URL = os.environ['GATEWAY_URL']

# Importing event_bus from common
from common.event_bus import EventBus


DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

event_bus = EventBus(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    db=int(os.environ['REDIS_DB']),
    password=os.environ['REDIS_PASSWORD']
)

def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, "No response")
    else:
        return response

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


@app.post('/check/<item_id>/<quantity>')
def check_stock(item_id: str, quantity: int):
    item_entry: StockValue = get_item_from_db(item_id)
    if item_entry.stock >= quantity:
        event = {"type": "StockChecked", "data": {"item_id": item_id, "quantity": quantity}}
        event_bus.publish("StockChecked", event)
        return Response(f"Item: {item_id} has enough stock", status=200)
    else:
        event = {"type": "StockReservationFailed", "data": {"item_id": item_id}}
        event_bus.publish("StockReservationFailed", event)
        return abort(400, f"Item: {item_id} not enough stock")

@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    if item_entry.stock >= int(amount):
        item_entry.stock -= int(amount)
        try:
            db.set(item_id, msgpack.encode(item_entry))
            event = {"type": "StockReserved", "data": {"item_id": item_id, "amount": amount}}
            event_bus.publish("StockReserved", event)
        except redis.exceptions.RedisError:
            return abort(400, DB_ERROR_STR)
        return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)
    else:
        return abort(400, f"Item: {item_id} not enough stock")

# Event handler
def handle_stock_checked(event_data):
    order_id = event_data['order_id']
    user_id = event_data['user_id']
    total_cost = event_data['total_cost']
    user_reply = send_post_request(f"{GATEWAY_URL}/payment/reserve_credit/{user_id}/{total_cost}")
    if user_reply.status_code != 200:
        event = {"type": "CreditReservationFailed", "data": {"order_id": order_id}}
        event_bus.publish("CreditReservationFailed", event)
    else:
        event = {"type": "CreditReserved", "data": {"order_id": order_id}}
        event_bus.publish("CreditReserved", event)

# Subscribing to events
pubsub = event_bus.subscribe("StockChecked")
for message in pubsub.listen():
    if message['type'] == 'message':
        event = json.loads(message['data'])
        if event['type'] == "StockChecked":
            handle_stock_checked(event['data'])

@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
