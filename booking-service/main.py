import os
import json
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from aiokafka import AIOKafkaProducer

# Настройки Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap:9092")

# Настройки БД
DATABASE_URL = f"postgresql://{os.getenv('DB_USER','user')}:{os.getenv('DB_PASSWORD','password')}@{os.getenv('DB_HOST','localhost')}:5432/{os.getenv('DB_NAME','dbname')}"

app = FastAPI()

# Сама БД
Base = declarative_base()
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Booking(Base):
    __tablename__ = "bookings"
    id = Column(Integer, primary_key=True, index=True)
    user_name = Column(String)
    status = Column(String)

Base.metadata.create_all(bind=engine)

class BookingRequest(BaseModel):
    user_name: str

# Глобальная переменная для producer
producer = None

@app.on_event("startup")
async def startup_event():
    global producer
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        await producer.start()
        print("✅ Kafka Producer started!")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    if producer:
        await producer.stop()

@app.post("/book")
async def create_booking(booking: BookingRequest):
    session = SessionLocal()
    # 1. Сохранение в БД
    new_booking = Booking(user_name=booking.user_name, status="PENDING")
    session.add(new_booking)
    session.commit()
    session.refresh(new_booking)
    
    # 2. Создание сообщения
    message = {
        "booking_id": new_booking.id,
        "user_name": new_booking.user_name,
        "status": "created"
    }
    
    # 3. Отправка в Kafka ('bookings')
    if producer:
        try:
            await producer.send_and_wait("bookings", json.dumps(message).encode('utf-8'))
            print(f"Sent to Kafka: {message}")
        except Exception as e:
            print(f"Kafka send error: {e}")
    else:
        print("Kafka producer not available")

    session.close()
    return {"status": "created", "id": new_booking.id}

@app.get("/health")
def health():
    return {"status": "ok"}