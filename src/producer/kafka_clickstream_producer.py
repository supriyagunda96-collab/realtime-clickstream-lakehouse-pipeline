import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

TOPIC = "retail_clickstream_events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

event_types = ["page_view", "product_view", "add_to_cart", "purchase"]
devices = ["mobile", "desktop", "tablet"]
products = ["P1001", "P1002", "P1003", "P1004"]

while True:
    event = {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "user_id": f"user_{random.randint(1, 5000)}",
        "session_id": f"sess_{random.randint(1, 10000)}",
        "event_type": random.choice(event_types),
        "product_id": random.choice(products),
        "device_type": random.choice(devices),
        "event_time": datetime.utcnow().isoformat(),
        "price": round(random.uniform(10, 500), 2)
    }

    producer.send(TOPIC, event)
    print(f"Produced event: {event}")
    time.sleep(1)