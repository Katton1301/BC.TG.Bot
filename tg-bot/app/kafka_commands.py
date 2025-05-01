from aiokafka import AIOKafkaProducer
import os
import json

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

async def send_to_kafka(topic, data):
    kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await kafka_producer.start()
    try:
        await kafka_producer.send_and_wait(topic, json.dumps(data).encode('utf-8'))
    finally:
        await kafka_producer.stop()