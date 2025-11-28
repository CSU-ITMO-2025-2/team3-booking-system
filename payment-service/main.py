from fastapi import FastAPI
from pydantic import BaseModel
import time

app = FastAPI()

# Модель того, что ожидается от сервиса бронирования
class PaymentRequest(BaseModel):
    booking_id: int

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "payment-service"}

@app.post("/pay")
def process_payment(request: PaymentRequest):
    # Имитация задержки работы банка (полсекунды)
    time.sleep(0.5)
    
    # Запись в лог (консоль), что деньги списаны
    print(f"Processing payment for booking ID: {request.booking_id}")
    
    # Возрат успешного ответа
    return {
        "status": "PAID",
        "booking_id": request.booking_id,
        "transaction_id": "tx-123456789"
    }