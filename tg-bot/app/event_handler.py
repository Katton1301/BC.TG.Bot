from datetime import datetime
import asyncio
import os
from typing import Dict,Any
from aiogram import types
from kafka_handler import KafkaHandler
from phrases import phrases
import keyboards as kb

# Settings
SERVER_ID = 1

import logging
logger = logging.getLogger(__name__)

class EventHandler:
    def __init__(self):
        self.event_queue = asyncio.Queue()
        self.running = False
        kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        kafka_config = {
            "db_listen_topic": os.environ['TOPIC_DB_LISTEN'],
            "db_send_topic": os.environ['TOPIC_DB_SEND'],
            "game_listen_topic": os.environ['TOPIC_GAME_LISTEN'],
            "game_send_topic": os.environ['TOPIC_GAME_SEND'],
        }
        
        self.kafka = KafkaHandler(kafka_bootstrap_servers, kafka_config)

    async def start(self):
        await self.kafka.start()

    async def stop(self):
        await self.kafka.stop()

    async def initGameServer(self):
        try:
            init_server = {
                "command": 1,  # Assuming 1 is INIT_GAME command
                "server_id": SERVER_ID,
            }
            game_response = await self.kafka.request_to_game(init_server, timeout=5)
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid response from game service")
            
            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")

            server_data = {
                "command": "get_server_games",
                "data": SERVER_ID,
            }            
            db_response = await self.kafka.request_to_db(server_data, timeout=5)
            if not db_response or 'games' not in db_response:
                raise Exception("Invalid response from database: missing games")
            
            for game in db_response['games']:
                await self._restore_game(game)

        except asyncio.TimeoutError:
            logger.error(f"Timeout during init game server")
            
        except Exception as e:
            logger.exception(f"Failed to init game server")
            

    async def change_player(self, message: Any):
        player_data = {
            "command": "change_player",
            "data": {
                "player_id": message.from_user.id,
                "firstname": message.from_user.first_name,
                "lastname": message.from_user.last_name,
                "fullname": message.from_user.full_name,
                "username": message.from_user.username,
                "lang": message.from_user.language_code,
            },
            "timestamp": str(datetime.now())
        }
        
        try:
            await self.kafka.send_to_bd(player_data)
            logger.info(f"Successfully sent player update for user_id: {message.from_user.id}")
        except Exception as e:
            logger.error(f"Failed to send player data to Kafka: {str(e)}")

    async def _create_game(self, player_id) -> None:
        logger.info(f"Starting game creation for user_id: {player_id}")
        
        try:
            create_msg = {
                "command": "create_game",
                "data": {
                    "id": 0,
                    "server_id": SERVER_ID,
                    "stage": "WAIT_A_NUMBER",
                    "step": 0,
                    "secret_value": 0
                },
                "timestamp": str(datetime.now())
            }
            
            db_response = await self.kafka.request_to_db(create_msg, timeout=5)
            if not db_response or 'id' not in db_response:
                raise Exception("Invalid response from database: missing game ID")
            
            if 'table' not in db_response or db_response['table'] != 'games':
                raise Exception("Invalid response from database: wrong table")
            
            game_id = db_response['id']
            logger.info(f"Game created with ID: {game_id}")

            player_msg = {
                "command": "add_player_game",
                "data": {
                    "player_id": player_id,
                    "server_id": SERVER_ID,
                    "game_id": game_id,
                    "is_current_game": False,
                    "is_host": True,
                },
                "timestamp": str(datetime.now())
            }
            
            await self.kafka.send_to_bd(player_msg)
            logger.info(f"Player {player_id} added to game {game_id}")

            game_msg = {
                "command": 4,  # Assuming 4 is CREATE_GAME command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid create game response from game service")
            
            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")
            
            return game_id
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout during game creation for user {player_id}")
            
        except Exception as e:
            logger.exception(f"Failed to create game for user {player_id}")

        return 0


    async def _restore_game(self, game: Dict[str, Any]):
        logger.info(f"restore game {game['id']}")
        #ToDo : implement game restore logic
        pass

    async def _start_game(self, game_id, player_id) -> None:
        try:
            set_current_game = {
                "command": "set_current_game",
                "data": {
                    "player_id": player_id,
                    "server_id": SERVER_ID,
                    "game_id": game_id,
                    "is_current_game": True,
                    "is_host": True,
                },
                "timestamp": str(datetime.now())
            }
            await self.kafka.send_to_bd(set_current_game)
            logger.info(f"Player {player_id} set current game {game_id}") 

            game_msg = {
                "command": 7,  # Assuming 7 is START_GAME command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid start game response from game service")
            
            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")
            
            start_game_message = {
                "command": "update_game",
                "data": {
                    "id": game_id,
                    "server_id": SERVER_ID,
                    "stage": game_response["game_stage"],
                    "step": 0,
                    "secret_value": game_response["secret_value"],
                },
                "timestamp": str(datetime.now())
            }
            await self.kafka.send_to_bd(start_game_message)
            logger.info(f"Game {game_id} fully initialized")

            return True
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout during game creation for user {player_id}")
            return False
            
        except Exception as e:
            logger.exception(f"Failed to create game for user {player_id}")
            return False
        

    async def start_single_game(self, message: types.Message):
        game_id = await self._create_game(message.from_user.id)
        if game_id == 0:
            await message.answer("Failed to create game")
            return False
        logger.info(f"Starting single game for user_id: {message.from_user.id}")
        ok = await self._start_game(game_id, message.from_user.id)
        if ok:
            lang = message.from_user.language_code
            await message.answer(f"{phrases.dict("gameCreated", lang)} {phrases.dict("yourTurn", lang)}")
        else:
            await message.answer("Failed to start game")
        return ok
        
    async def do_step( self, message: types.Message ):
        try:
            if not message.text.isdigit():
                raise Exception(phrases.dict("invalidNumberFormat", message.from_user.language_code))
            
            get_current_game_msg = {
                "command": "get_current_game",
                "data": message.from_user.id,
            }
            db_response = await self.kafka.request_to_db(get_current_game_msg, timeout=5)
            if not db_response or 'id' not in db_response:
                raise Exception("Invalid get current game response from game service")
            
            if 'table' not in db_response or db_response['table'] != 'games':
                raise Exception("Invalid response from database: wrong table")
            game_id = db_response['id']
            game_value = int(message.text)
            game_msg = {
                "command": 8,  # Assuming 8 is PLAYER_STEP command
                "server_id": SERVER_ID,
                "player_id": message.from_user.id,
                "game_id": game_id,
                "game_value": game_value,
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid do step response from game service")
            
            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")
            
            if "steps" not in game_response:
                raise Exception(f"Game service not have this player steps")
            
            steps = game_response['steps']
            player_i = -1
            for i in range(len(steps)):
                if steps[i]['player']:
                    if steps[i]['id'] == message.from_user.id:
                        player_i = i
                    else:
                        continue

                step_message = {
                    "command": "add_step",
                    "data": {
                        "game_id": game_id,
                        "player_id": steps[i],
                        "server_id": SERVER_ID,
                        "step": steps[i]["step"],
                        "game_value": steps[i]["game_value"],
                        "bulls": steps[i]["bulls"],
                        "cows": steps[i]["cows"],
                        "is_computer": not steps[i]["player"],
                    },
                    "timestamp": str(datetime.now())
                }
                await self.kafka.send_to_bd(step_message)
                logger.info(f"Player {message.from_user.id} do step in game {game_id}")
            
            if player_i == -1:
                raise Exception(f"Game service not have this player steps")
            
            update_game_message = {
                "command": "update_game",
                "data": {
                    "id": game_id,
                    "server_id": SERVER_ID,
                    "stage": game_response["game_stage"],
                    "step": steps[player_i]["step"],
                    "secret_value": 0,
                },
                "timestamp": str(datetime.now())
            }
            await self.kafka.send_to_bd(update_game_message)
            logger.info(f"Player {message.from_user.id} do step in game {game_id}")
            
            place =  0
            if 'place' in game_response:
                place = game_response['place']
            lang = message.from_user.language_code
            result = ""
            bots = 0
            players = 0
            unfinished_players = 0

            result += f"{phrases.dict('step', lang)} {steps[player_i]["step"]}"
            if len(steps) > 1:
                result += f"\n{phrases.dict("you", lang)} -  {game_value:04}: {steps[player_i]["bulls"]}{phrases.dict("bulls", lang)} {steps[player_i]["cows"]}{phrases.dict("cows", lang)}"
                for i in range(len(steps)):
                    if i == player_i:
                        continue
                    elif steps[i]["player"]:
                        result += f"\n{phrases.dict('player', lang)} {str(players + 1)}"
                        players += 1
                        if not steps[i]["finished"]:
                            unfinished_players += 1
                    else:
                        result += f"\n{phrases.dict('bot', lang)} {str(bots + 1)}"
                        bots += 1
                    result += f" - {'*'*4}: {steps[i]["bulls"]}{phrases.dict("bulls", lang)} {steps[i]["cows"]}{phrases.dict("cows", lang)}"
            else:
                result += f"\n{game_value:04}: {steps[player_i]["bulls"]}{phrases.dict("bulls", lang)} {steps[player_i]["cows"]}{phrases.dict("cows", lang)}"

            if game_response['game_stage'] == 'IN_PROGRESS_WINNER_DEFINED' and steps[player_i]['finished'] and unfinished_players == 0:
                game_msg = {
                "command": 15,  # Assuming 15 is FINISH_GAME command
                "server_id": SERVER_ID,
                "player_id": message.from_user.id,
                "game_id": game_id,
                "game_value": game_value,
                }
                game_response = await self.kafka.request_to_game(game_msg, timeout=5)
                if not game_response or 'result' not in game_response:
                    raise Exception("Invalid do step response from game service")
            
                if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                    raise Exception(f"Game service error: {game_response['result']}")
                
                steps = game_response['steps']
                result += f"\n{steps[0]["step"]} {game_value}: {steps[0]["bulls"]}{phrases.dict("bulls", lang)} {steps[0]["cows"]}{phrases.dict("cows", lang)}"

            if game_response['game_stage'] == 'FINISHED':
                if len(steps) == 2 and steps[0]['finished'] and steps[1]['finished']:
                    await message.answer(
                        f"{result}\n{phrases.dict('gameFinished', lang)} {phrases.dict("draw", lang)}",
                        reply_markup=kb.main[lang]
                        )
                elif place == 1:
                    await message.answer(
                        f"{result}\n{phrases.dict("gameFinished", lang)} {phrases.dict("youWon", lang)}",
                        reply_markup=kb.main[lang]
                        )
                else:
                    await message.answer(
                        f"{result}\n{phrases.dict("gameFinished", lang)} {phrases.dict("yourPlace", lang)} - {place}",   
                        reply_markup=kb.main[lang]
                        )
                return True
            else:
                if(len(steps) < game_response["players"]):
                    result += f"\n {game_response["players"] - len(steps)} {phrases.dict('waitPlayers', lang)}"
                await message.answer(result)
                return False

        except asyncio.TimeoutError:
            error_msg = "Do step timeout. Please try again later."
            logger.error(f"Timeout during game creation for user {message.from_user.id}")
            await message.answer(error_msg)
            return False
            
        except Exception as e:
            logger.exception(f"Failed to do step for user {message.from_user.id}")
            await message.answer(str(e))
            return False
        
    async def start_bot_play( self, message: types.Message ):
        game_id = await self._create_game(message.from_user.id)
        if game_id == 0:
            return False
        try:
            level = None
            if phrases.checkPhrase("easy", str(message.text)):
                level = "Easy"
            if phrases.checkPhrase("medium", str(message.text)):
                level = "Medium"
            if phrases.checkPhrase("hard", str(message.text)):
                level = "Hard"
            if level is None:
                raise Exception(phrases.dict("invalidBotLevel", message.from_user.language_code))
            
            logger.info(f"Starting game for user_id: {message.from_user.id}")

            create_msg = {
                "command": "create_computer",
                "data": {
                    "computer_id": 0,
                    "player_id": message.from_user.id,
                    "server_id": SERVER_ID,
                    "game_id": game_id,
                    "game_brain": level,
                },
                "timestamp": str(datetime.now())
            }
            
            db_response = await self.kafka.request_to_db(create_msg, timeout=5)
            if not db_response or 'id' not in db_response:
                raise Exception("Invalid response from database: missing computer ID")
            
            if 'table' not in db_response or db_response['table'] != 'computers':
                raise Exception("Invalid response from database: wrong table")
            
            computer_id = db_response['id']

            game_msg = {
                "command": 6,  # Assuming 6 is ADD_COMPUTER command
                "computer_id": computer_id,
                "player_id": message.from_user.id,
                "server_id": SERVER_ID,
                "game_id": game_id,
                "game_brain": level,
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid add computer response from game service")
            
            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")


            logger.info(f"Starting single game for user_id: {message.from_user.id}")
            ok = await self._start_game(game_id, message.from_user.id)            
            if ok:
                lang = message.from_user.language_code
                await message.answer(f"{phrases.dict("gameCreated", lang)} {phrases.dict("yourTurn", lang)}")
            else:
                await message.answer("Failed to start game")
            return ok
            
        except asyncio.TimeoutError:
            error_msg = "Game start timeout. Please try again later."
            logger.error(f"Timeout during game creation for user {message.from_user.id}")
            await message.answer(error_msg)
            return False
            
        except Exception as e:
            logger.exception(f"Failed to create game for user {message.from_user.id}")
            await message.answer(str(e))
            return False