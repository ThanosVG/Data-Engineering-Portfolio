FROM eclipse-temurin:17-jre

WORKDIR /app

RUN apt-get update && apt-get install -y python3.10 python3-pip && rm -rf /var/lib/apt/lists/*

RUN pip install --break-system-packages pyspark==3.5.0 kafka-python

COPY spark_kafka_consumer.py .
COPY kafka_producer.py .
COPY salaries.csv .

CMD ["python", "spark_kafka_consumer.py"]