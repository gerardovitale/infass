FROM python:3.9-slim

WORKDIR /app

COPY . .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["fastapi", "run"]
