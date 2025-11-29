import os
import httpx
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base, Session

# Настройки
# Безопасное чтение данных 
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# URL сервиса оплаты
PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL")

# Подключение к БД
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# база данных (SQLAlchemy)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Описание таблицы заявок
class Booking(Base):
    __tablename__ = "bookings"
    id = Column(Integer, primary_key=True, index=True)
    user_name = Column(String)
    status = Column(String) # Статусы: PENDING, CONFIRMED, FAILED

# Создаем таблицу в БД, если её нет
Base.metadata.create_all(bind=engine)

app = FastAPI()

# Модель данных для запроса (что нам присылает пользователь)
class BookingRequest(BaseModel):
    user_name: str

# Функция для получения сессии БД
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "booking-service"}

@app.post("/book")
async def create_booking(request: BookingRequest, db: Session = Depends(get_db)):
    # 1. Создание записи "в ожидании"
    new_booking = Booking(user_name=request.user_name, status="PENDING")
    db.add(new_booking)
    db.commit()
    db.refresh(new_booking)

    # 2. Звонок в сервис оплаты 
    print(f"Booking {new_booking.id}: contacting payment service...")
    async with httpx.AsyncClient() as client:
        try:
            # Отправка ID брони на оплату
            response = await client.post(
                f"{PAYMENT_SERVICE_URL}/pay", 
                json={"booking_id": new_booking.id}
            )
            
            if response.status_code == 200:
                new_booking.status = "CONFIRMED"
            else:
                new_booking.status = "FAILED"
                
        except Exception as e:
            print(f"Error contacting payment service: {e}")
            new_booking.status = "FAILED"
    
    # 3. Сохранение итогового статуса
    db.commit()
    
    return {
        "booking_id": new_booking.id, 
        "status": new_booking.status,
        "user": new_booking.user_name
    }