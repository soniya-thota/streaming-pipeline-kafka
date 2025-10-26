from kafka import KafkaProducer
import json, time, random, datetime

def generate_event():
    products = ["Keyboard", "Mouse", "Laptop", "Monitor", "Headphones"]
    event = {
        "order_id": random.randint(1000, 9999),
        "product": random.choice(products),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10, 1000), 2),
        "timestamp": datetime.datetime.now().isoformat()
    }
    return event

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    event = generate_event()
    producer.send("transactions", event)
    print("Produced:", event)
    time.sleep(1)
