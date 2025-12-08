# 1. Лёгкий образ Python
FROM python:3.9-slim

# 2. Папка внутри контейнера
WORKDIR /app

# Сначала копия только списка библиотек (для кэширования)
COPY requirements.txt .

# Установка библиотеки
RUN pip install --no-cache-dir -r requirements.txt

# Копия всего остального кода
COPY . .

# Команда запуска (порт 8000)
CMD ["python", "-u", "main.py"]