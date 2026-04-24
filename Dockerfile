FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV OPTIONS_ENV=prod
ENV OPTIONS_HOST=0.0.0.0
ENV OPTIONS_PORT=8050

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8050

CMD ["gunicorn", "-b", "0.0.0.0:8050", "index:server"]
