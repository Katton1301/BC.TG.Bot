import asyncio
import json
import uuid
import logging
from typing import Dict, Any, Optional
import os
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

class KafkaHandler:
    def __init__(self, bootstrap_servers: str, topics: Dict[str, Any]):
        self.bootstrap_servers = bootstrap_servers
        self.postgres_request_topic = "postgres_request"
        if "postgres_request_topic" in topics:
            self.postgres_request_topic = topics["postgres_request_topic"]
        self.postgres_responce_topic = "postgres_response"
        if "postgres_responce_topic" in topics:
            self.postgres_responce_topic = topics["postgres_responce_topic"]
        self.game_request_topic = "game_request"
        if "game_request_topic" in topics:
            self.game_request_topic = topics["game_request_topic"]
        self.game_response_topic = "game_response"
        if "game_response_topic" in topics:
            self.game_response_topic = topics["game_response_topic"]

        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.consumer_task = None
        self.loop = asyncio.get_event_loop()

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            self.response_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=f'{self.response_topic}-group',
            auto_offset_reset='earliest'
        )
        await self.consumer.start()
        self.consumer_task = asyncio.create_task(self._consume_responses())

    async def stop(self):
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
        await self.consumer.stop()

    async def _consume_responses(self):
        try:
            async for msg in self.consumer:
                try:
                    message = json.loads(msg.value.decode('utf-8'))
                    correlation_id = message.get('correlation_id')
                    
                    if correlation_id and correlation_id in self.pending_requests:
                        future = self.pending_requests.pop(correlation_id)
                        future.set_result(message)
                        logging.debug(f"Received response for {correlation_id}")
                        
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logging.error(f"Error processing message: {e}")
        except Exception as e:
            logging.error(f"Consumer error: {e}")

    async def send(self, message: Dict[str, Any]) -> str:
        correlation_id = str(uuid.uuid4())
        message['correlation_id'] = correlation_id
        
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await producer.start()
        try:
            await producer.send(
                self.request_topic,
                json.dumps(message).encode('utf-8')
            )
            logging.debug(f"Sent message with correlation_id {correlation_id}")
            return correlation_id
        finally:
            await producer.stop()

    async def request(self, message: Dict[str, Any], timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        correlation_id = str(uuid.uuid4())
        message['correlation_id'] = correlation_id
        
        future = self.loop.create_future()
        self.pending_requests[correlation_id] = future
        
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await producer.start()
        try:
            await producer.send(
                self.request_topic,
                json.dumps(message).encode('utf-8')
            )
            logging.debug(f"Sent request with correlation_id {correlation_id}")
            
            try:
                return await asyncio.wait_for(future, timeout)
            except asyncio.TimeoutError:
                logging.warning(f"Timeout waiting for response to {correlation_id}")
                self.pending_requests.pop(correlation_id, None)
                return None
        finally:
            await producer.stop()