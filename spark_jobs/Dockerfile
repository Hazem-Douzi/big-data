FROM spark:3.5.6-python3

USER root

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY spark_kafka_to_mongo.py .

CMD ["/opt/spark/bin/spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6", \
    "/app/spark_kafka_to_mongo.py"]
