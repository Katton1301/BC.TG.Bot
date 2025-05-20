import asyncio
import json
import uuid
import logging
from typing import Dict, Any, Optional
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

logger = logging.getLogger(__name__)

class KafkaHandler:
    def __init__(self, bootstrap_servers: str, topics: Dict[str, Any]):
        self.bootstrap_servers = bootstrap_servers
        self.db_listen_topic = "db_bot"
        if "db_listen_topic" in topics:
            self.db_listen_topic = topics["db_listen_topic"]
        self.db_send_topic = "bot_db"
        if "db_send_topic" in topics:
            self.db_send_topic = topics["db_send_topic"]
        self.game_listen_topic = "game_bot"
        if "game_listen_topic" in topics:
            self.game_listen_topic = topics["game_listen_topic"]
        self.game_send_topic = "bot_game"
        if "game_send_topic" in topics:
            self.game_send_topic = topics["game_send_topic"]

        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.consumer_task = []
        self.loop = asyncio.get_event_loop()

    async def start(self):
        self.postgres_consumer = AIOKafkaConsumer(
            self.db_listen_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id='bot-group',
            auto_offset_reset='latest'
        )
        await self.postgres_consumer.start()
        self.consumer_task.append(asyncio.create_task(self._consume_responses(self.postgres_consumer)))
        self.game_consumer = AIOKafkaConsumer(
            self.game_listen_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id='bot-group',
            auto_offset_reset='latest'
        )
        await self.game_consumer.start()
        self.consumer_task.append(asyncio.create_task(self._consume_responses(self.game_consumer)))

    async def stop(self):
        for task in self.consumer_task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await self.postgres_consumer.stop()
        await self.game_consumer.stop()

    async def _consume_responses(self, consumer):
        try:
            async for msg in consumer:
                try:
                    message = json.loads(msg.value.decode('utf-8'))
                    correlation_id = message.get('correlation_id')
                    
                    if correlation_id and correlation_id in self.pending_requests:
                        future = self.pending_requests.pop(correlation_id)
                        future.set_result(message)
                        logger.info(f"Received response for {correlation_id}")
                    else:
                        logger.info(f"Received unknown response for {correlation_id}")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        except Exception as e:
            logger.error(f"Consumer error: {e}")

    async def send_to_bd(self, message: Dict[str, Any]) -> str:
        logger.info(f"send to bd : {message}")
        await self.send(message, self.db_send_topic)

    async def send_to_game(self, message: Dict[str, Any]) -> str:
        await self.send(message, self.game_send_topic)

    async def send(self, message: Dict[str, Any], topic : str) -> str:
        correlation_id = str(uuid.uuid4())
        message['correlation_id'] = correlation_id
        
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await producer.start()
        try:
            await producer.send(
                topic,
                json.dumps(message).encode('utf-8')
            )
            logger.info(f"Sent message with correlation_id {correlation_id}, topic {topic}")
            logger.debug(f"Sent message with correlation_id {correlation_id}")
            return correlation_id
        finally:
            await producer.stop()

    async def request_to_db(self, message: Dict[str, Any], timeout: float = 5.0) -> str:
        return await self.request(message, self.db_send_topic, timeout)

    async def request_to_game(self, message: Dict[str, Any], timeout: float = 5.0) -> str:
        return await self.request(message, self.game_send_topic, timeout)

    async def request(
        self, 
        message: Dict[str, Any], 
        topic: str, 
        timeout: float = 5.0
    ) -> Optional[Dict[str, Any]]:
        correlation_id = str(uuid.uuid4())
        message['correlation_id'] = correlation_id
        
        future = self.loop.create_future()
        self.pending_requests[correlation_id] = future
        
        producer = None
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                enable_idempotence=True,
                max_batch_size=32768,
                linger_ms=10
            )
            await producer.start()
            send_future = await producer.send(
                topic,
                value=json.dumps(message).encode('utf-8')
            )
            await send_future  # Wait for delivery confirmation
            
            logger.debug(f"Sent request to {topic}, correlation_id: {correlation_id}")
            
            try:
                response = await asyncio.wait_for(future, timeout)
                logger.debug(f"Received response for {correlation_id}")
                return response
            except asyncio.TimeoutError:
                logger.warning(f"Timeout waiting for response to {correlation_id}")
                self.pending_requests.pop(correlation_id, None)
                return None
                
        except Exception as send_error:
            logger.error(f"Failed to send request {correlation_id}: {str(send_error)}")
            self.pending_requests.pop(correlation_id, None)
            raise  # Re-raise to let caller handle it
            
        finally:
            if producer is not None:
                try:
                    await producer.stop()
                except Exception as stop_error:
                    logger.warning(f"Error stopping producer: {str(stop_error)}")