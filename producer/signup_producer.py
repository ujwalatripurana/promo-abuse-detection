import json, os, time, random, string
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# For host: use localhost:9094 (exposed listener). For in-container use: kafka:9092
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "signup-events")

ABUSIVE_DEVICES = ["dev_003", "dev_007"]
NORMAL_DEVICES  = [f"dev_{i:03d}" for i in range(1, 50) if f"dev_{i:03d}" not in ABUSIVE_DEVICES]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def random_user():
    return "u_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))

def build_event(device_id: str, abusive=False) -> dict:
    return {
        "event": "signup",
        "user_id": random_user(),
        "device_id": device_id,
        "timestamp": now_iso(),
        "label_abusive": bool(abusive),
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k,  # keys are bytes (device_id encoded)
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        acks="all",
        retries=10,
        request_timeout_ms=20000,
        max_in_flight_requests_per_connection=5,
    )
    print(f"Producing to {BOOTSTRAP} topic '{TOPIC}' ... Ctrl+C to stop")

    try:
        while True:
            if random.random() < 0.12:
                # Create bursts on abusive device to trigger alerts
                device = random.choice(ABUSIVE_DEVICES)
                for _ in range(random.randint(3, 5)):
                    producer.send(
                        TOPIC,
                        key=device.encode("utf-8"),  # 🔑 key by device_id for per-device partitioning
                        value=build_event(device, abusive=True)
                    )
                    time.sleep(random.uniform(0.05, 0.2))
            else:
                device = random.choice(NORMAL_DEVICES)
                producer.send(
                    TOPIC,
                    key=device.encode("utf-8"),  # 🔑 key by device_id for normal events too
                    value=build_event(device, abusive=False)
                )

            producer.flush()
            time.sleep(random.uniform(0.1, 0.5))

    except KeyboardInterrupt:
        print("Stopping producer.")
        producer.flush()

if __name__ == "__main__":
    main()