import os
import json
import logging
import psycopg2
from kafka import KafkaConsumer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'flight_data')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'postgres')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def main():
    logger.info("Starting Consumer...")

    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='flight-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Database connection
    conn = get_db_connection()
    cur = conn.cursor()

    logger.info(f"Listening on topic: {KAFKA_TOPIC}")

    try:
        for message in consumer:
            data = message.value
            logger.info(f"Received message: {data}")

            # Insert into Postgres
            try:
                cur.execute(
                    "INSERT INTO raw_data.flights (data) VALUES (%s)",
                    (json.dumps(data),)
                )
                conn.commit()
                logger.info("Data inserted into DB")
            except Exception as e:
                logger.error(f"Error inserting into DB: {e}")
                conn.rollback()

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()

