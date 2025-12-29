FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean;

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

ENV SPARK_MASTER="local[*]"

ENV PYTHONPATH="/app"

CMD ["tail", "-f", "/dev/null"]