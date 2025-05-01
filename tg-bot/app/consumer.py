from aiogram import Bot
from aiokafka import AIOKafkaConsumer
import asyncio
import json
import logging
import os
import secrets
from kafka_commands import send_to_kafka

waiting_messages = []
bot = Bot(token=os.environ["TG_API_BC_TOKEN"])
KAFKA_BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS']
TOPIC_POSTGRES_REQUEST = os.environ['TOPIC_POSTGRES_REQUEST']

def get_uniq_message_id():
    while True:
        id = secrets.randbits(63)
        if id not in waiting_messages:
            return id

async def handle_kafka_message(message: dict):
    try:
        message_id = message.get('MessageId')
        
        if message_id in waiting_messages:
            logging.info(f"Message id {message_id} responsing with table: {message.get('Table')} and id: {message.get('Id')}")
            waiting_messages.remove(message_id)
        
        else:
            logging.info(f"Message id {message_id} already responsed or not requested")
            
    except Exception as e:
        logging.error(f"Error handling Kafka message: {e}", exc_info=True)

async def run_consumer():
    retry_count = 0
    max_retries = 5
    retry_delay = 5  # seconds
    
    while retry_count < max_retries:
        consumer = AIOKafkaConsumer(
            TOPIC_POSTGRES_REQUEST,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=TOPIC_POSTGRES_REQUEST,
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )
        
        try:
            await consumer.start()
            logging.info("Kafka consumer started")
            
            async for msg in consumer:
                try:
                    message = json.loads(msg.value.decode('utf-8'))
                    logging.info(f"Received message: {message}")
                    
                    await handle_kafka_message(message)
                    
                    await consumer.commit()
                    
                except json.JSONDecodeError as e:
                    logging.error(f"Error decoding JSON: {e}")
                except Exception as e:
                    logging.error(f"Error processing message: {e}", exc_info=True)
        except asyncio.CancelledError:
            logging.info("Consumer stopped (CancelledError)")
        except Exception as e:
            retry_count += 1
            logging.error(f"Connection attempt {retry_count} failed: {e}")
            if retry_count < max_retries:
                await asyncio.sleep(retry_delay)
            else:
                logging.error("Max retries reached, giving up")
                raise
        finally:
            await consumer.stop()
            logging.info("Kafka consumer stopped")