import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
	bootstrap_servers="localhost:9092", #where kafka is running
	key_serializer=lambda k : k.encode("utf-8"), #converts batch_id(string) -> bytes
	value_serializer=lambda v : json.dumps(v).encode("utf-8"), #comverts python dict -> json -> bytes
	acks="all" #at least once
	)

BATCH_IDS = ["BATCH_1", "BATCH_2", "BATCH_3"]
EQUIPMENT_IDS = ["MIXER_1", "MIXER_2"]

def generate_event():
    return {
        "event_time": datetime.utcnow().isoformat(),
        "batch_id": random.choice(BATCH_IDS),
        "equipment_id": random.choice(EQUIPMENT_IDS),
        "temperature": round(random.uniform(60, 90), 2),
        "pressure": round(random.uniform(20, 40), 2),
        "flow_rate": round(random.uniform(5, 15), 2)
    }


if __name__ == "__main__":
    while True:
        event = generate_event()

        producer.send(
            topic="iot_telemetry",
            key=event["batch_id"],
            value=event
        )

        print(f"Sent event: {event}")
        time.sleep(1)
