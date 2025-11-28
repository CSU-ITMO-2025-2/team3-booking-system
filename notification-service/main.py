from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class EmailRequest(BaseModel):
    email: str
    message: str

@app.get("/health")
def health_check():
    return {"status": "ok", "service": "notification-service"}

@app.post("/send")
def send_notification(request: EmailRequest):
    # Логика отправки email (имитация)
    # Запись в консоль
    print(f"------------")
    print(f"SENDING EMAIL TO: {request.email}")
    print(f"MESSAGE: {request.message}")
    print(f"------------")
    
    return {"status": "SENT", "recipient": request.email}