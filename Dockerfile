FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Настройки пользователей сохраняются в /app/data
VOLUME /app/data

CMD ["python", "bot.py"]
