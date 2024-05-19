import logging
import os
import atexit
import uuid
import json
from kafka import KafkaProducer, KafkaConsumer
import redis
from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
import threading

DB_ERROR_STR = "DB error"

app = Flask("stock-service")

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        abort(400, f"Item: {item_id} not found!")
    return entry


def emit_event(event_data: dict):
    producer.send('stock_events', value=event_data)
    producer.flush()  # Ensure message is sent immediately


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    emit_event({
        "event_type": "ItemCreated",
        "item_id": key,
        "price": price
    })
    
    return jsonify({'item_id': key})


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    emit_event({
        "event_type": "StockAdded",
        "item_id": item_id,
        "amount": amount
    })
    
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    emit_event({
        "event_type": "StockSubtracted",
        "item_id": item_id,
        "amount": amount
    })
    
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


# def handle_stock_check(event_data):
#     order_id = event_data['order_id']
#     items = event_data['items']
#     stock_available = True

#     for item_id, quantity in items:
#         item_entry = get_item_from_db(item_id)
#         if item_entry.stock < quantity:
#             stock_available = False
#             break

#     emit_event({
#         "event_type": "StockChecked",
#         "order_id": order_id,
#         "stock_available": stock_available
#     })


def handle_order_initiated(event_data):
    user_id = event_data['user_id']
    total_cost = event_data['total_cost']
    order_id = event_data['order_id']
    items = event_data['items']
    stock_available = True

    #Check if we have enough stock
    for item_id, quantity in items:
        item_entry = get_item_from_db(item_id)
        if item_entry.stock < quantity:
            stock_available = False
            break
    
    #If we have enough stock, reserve stock
    if stock_available:
        for item_id, quantity in items:
            item_entry = get_item_from_db(item_id)
            item_entry.stock -= quantity
            try:
                db.set(item_id, msgpack.encode(item_entry))
            except redis.exceptions.RedisError:
                emit_event({
                    "event_type": "StockReservationFailed",
                    "order_id": order_id,
                    "user_id": user_id,
                    "total_cost": total_cost,
                    "items": items,
                    "reason": "Database error during stock reservation"
                })
                return Response("Database error during stock reservation", status=500)

        emit_event({
            "event_type": "StockReserved",
            "order_id": order_id,
            "user_id": user_id,
            "total_cost": total_cost,
            "items": items
        })
        return Response("Stock reserved successfully", status=200)
    #If we dont have enough stock, says stock reservation has failed
    else:
        emit_event({
            "event_type": "StockReservationFailed",
            "order_id": order_id,
            "user_id": user_id,
            "total_cost": total_cost,
            "items": items,
            "reason": "Stock not available"
        })
        return Response("Stock not available", status=400)


def handle_release_stock(event_data):
    items = event_data['items']
    for item_id, quantity in items:
        item_entry = get_item_from_db(item_id)
        item_entry.stock += quantity
        try:
            db.set(item_id, msgpack.encode(item_entry))
        except redis.exceptions.RedisError:
            app.logger.error("Failed to release stock for item: %s", item_id)
            return Response(f"Failed to release stock for item: {item_id}", status=500)

    return Response("Stock released successfully", status=200)


def consume_events():
    consumer = KafkaConsumer('order_events',
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest',
                             group_id='stock_group',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for event in consumer:
        event_type = event.value.get('event_type')
        # if event_type == 'StockCheck':
        #     handle_stock_check(event.value)
        if event_type == 'StockCheck':
            handle_order_initiated(event.value)
        elif event_type == 'ReleaseStock':
            handle_release_stock(event.value)


if __name__ == '__main__':
    consumer_thread = threading.Thread(target=consume_events)
    consumer_thread.start()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
