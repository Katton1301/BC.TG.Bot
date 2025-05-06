import queue
import uuid
from datetime import datetime
import asyncio
import os
from aiogram import Bot
from typing import Dict, Any
from kafka_handler import KafkaHandler

# Settings
SERVER_ID = 1

bot = Bot(token=os.environ["TG_API_BC_TOKEN"])

class EventHandler:
    def __init__(self):
        self.event_queue = queue.Queue()
        self.running = False
        self.thread = None

        kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        kafka_config = {
            "postgres_request_topic": os.environ['TOPIC_POSTGRES_SEND'],
            "postgres_responce_topic": os.environ['TOPIC_POSTGRES_REQUEST'],
            "game_request_topic": os.environ['TOPIC_GAME_SEND'],
            "game_response_topic": os.environ['TOPIC_GAME_REQUEST'],
        }
        
        self.kafka = KafkaHandler(kafka_bootstrap_servers, kafka_config)

        self.loop = asyncio.new_event_loop()
        asyncio.run_coroutine_threadsafe(self.kafka.start(), self.loop)

    def stop(self):
        self.running = False
        asyncio.run_coroutine_threadsafe(self.kafka.stop(), self.loop).result()
        if self.thread:
            self.thread.join()
        self.loop.close()

    async def change_player(self, message: Any):
        response = {
            "command": "change_player",
            "player_id": message.from_user.id,
            "username": message.from_user.username,
            "timestamp": str(datetime.now())
        }
        
        asyncio.run_coroutine_threadsafe(
            self.kafka.send(response),
            self.loop
        )

    async def create_game(self, message: Any):
        create_msg = {
            "command": "create_game",
            "server_id": SERVER_ID,
            "timestamp": str(datetime.now())
        }
        
        future = asyncio.run_coroutine_threadsafe(
            self.kafka.request(create_msg, timeout=5),
            self.loop
        )
        
        try:
            response = future.result()
            if not response or 'game_id' not in response:
                raise Exception("Failed to create game")
            
            game_id = response['game_id']
            
            player_msg = {
                "command": "add_player_game",
                "player_id": message.from_user.id,
                "game_id": game_id
            }
            asyncio.run_coroutine_threadsafe(
                self.kafka.send(player_msg),
                self.loop
            )
            
            self._reply_to_user(f"Game {game_id} created", message)
            
        except Exception as e:
            self._reply_to_user(f"Error: {str(e)}", message)


    def _reply_to_user(self, text: str, message: Any):
        try:
            asyncio.run_coroutine_threadsafe(
                bot.send_message(
                    chat_id=message.chat.id,
                    text=text,
                    reply_to_message_id=message.message_id
                ),
                asyncio.get_event_loop()
            )
        except Exception as e:
            print(f"Failed to send message to user {message.from_user.id}: {str(e)}")