# event_bus.py
import redis
import json
import os


class EventBus:
    def __init__(self, host, port, db, password):
        self.client = redis.Redis(host=host, port=port, db=db, password=password)

    def publish(self, channel, event):
        self.client.publish(channel, json.dumps(event))

    def subscribe(self, channel):
        pubsub = self.client.pubsub()
        pubsub.subscribe(channel)
        return pubsub


# Initialize event bus
event_bus = EventBus(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    db=int(os.environ['REDIS_DB']),
    password=os.environ['REDIS_PASSWORD']
)
