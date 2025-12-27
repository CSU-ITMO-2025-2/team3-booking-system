import asyncio
import os
import json
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
#KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9092")
TOPIC = "bookings"

async def consume():
    print(f"Starting Notification Service. Connecting to {KAFKA_BOOTSTRAP_SERVERS}...")
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="notification-group"
    )
    
    # Подключение с повторами
    while True:
        try:
            await consumer.start()
            print("Consumer connected! Waiting for messages...")
            break
        except Exception as e:
            print(f"Connection failed: {e}. Retrying in 5s...")
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            data = json.loads(msg.value.decode('utf-8'))
            print(f"[NOTIFICATION] Sending email to {data['user_name']} for booking #{data['booking_id']}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())