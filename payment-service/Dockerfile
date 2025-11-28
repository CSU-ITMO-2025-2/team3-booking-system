# Лёгкий образ Python
FROM python:3.9-slim

# Папка внутри контейнера
WORKDIR /app

# Сначала копия только списка библиотек (для кэширования)
COPY requirements.txt .

# Установка библиотеки
RUN pip install --no-cache-dir -r requirements.txt

# Копия всего остального кода
COPY . .

# Команда запуска (порт 8000)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]