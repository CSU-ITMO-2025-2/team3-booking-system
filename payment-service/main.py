import asyncio
import json
import random
from aiokafka import AIOKafkaConsumer

# Настройки
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "bookings"
GROUP_ID = "payment-group"  # Другая группа, чтобы читать параллельно с уведомлениями

async def process_payment(booking_data):
    """Имитация логики оплаты"""
    user = booking_data.get("user_name")
    booking_id = booking_data.get("booking_id")
    price = random.randint(100, 500) # генерация случайной цены
    
    print(f"[PAYMENT] Обработка платежа для заказа #{booking_id}...")
    await asyncio.sleep(2) # Имитация задержки банка (2 сек)
    
    # лог
    print(f"[PAYMENT] успешно! Списано ${price} у пользователя {user}.")

async def consume():
    print(f"Payment Service запускается...")
    
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Цикл подключения (если Кафка еще грузится)
    while True:
        try:
            await consumer.start()
            print("Payment Service подключен к Kafka")
            break
        except Exception:
            print("Жду Kafka")
            await asyncio.sleep(5)

    try:
        # чтение сообщений бесконечно
        async for msg in consumer:
            await process_payment(msg.value)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())