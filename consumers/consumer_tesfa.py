import json
import sqlite3
import redis
from kafka import KafkaConsumer
from utils import utils_config as config
from utils.utils_logger import logger

def main():
    logger.info("Starting consumer_tesfa.py")

    # Kafka Configuration
    kafka_topic = config.get_kafka_topic()
    kafka_broker = config.get_kafka_broker_address()

    # SQLite Configuration
    sqlite_path = config.get_sqlite_path()
    conn = sqlite3.connect(str(sqlite_path))
    cursor = conn.cursor()

    # Redis Configuration
    redis_host = config.get_redis_host()
    redis_port = config.get_redis_port()
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    # Create SQLite Table (if it doesn't exist)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS keyword_counts (
            keyword TEXT PRIMARY KEY,
            count INTEGER
        )
    """)
    conn.commit()

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_broker,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    try:
        for message in consumer:
            message_data = message.value
            if keyword := message_data.get('keyword_mentioned'):
                # Update SQLite
                cursor.execute("""
                    INSERT OR REPLACE INTO keyword_counts (keyword, count)
                    VALUES (?, COALESCE((SELECT count + 1 FROM keyword_counts WHERE keyword = ?), 1))
                """, (keyword, keyword))
                conn.commit()

                # Update Redis
                r.incr(f'keyword:{keyword}')

                logger.info(f"Keyword '{keyword}' count updated.")

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error processing message: {e}")
    finally:
        conn.close()
        consumer.close()
        logger.info("Consumer shutting down.")

if __name__ == "__main__":
    main()