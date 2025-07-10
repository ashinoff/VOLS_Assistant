# Используем официальный образ Python 3.11 (можешь выбрать любой tag!)
FROM python:3.11-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файлы проекта
COPY . /app

# Обновляем pip и устанавливаем зависимости
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Экспортируем порт (замени 10000 на свой, если надо)
EXPOSE 10000

# Команда запуска приложения
CMD ["python", "main.py"]
