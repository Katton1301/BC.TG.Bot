from datetime import datetime
import asyncio
import os
from typing import Any
from aiogram import types
from kafka_handler import KafkaHandler
from phrases import phrases
import keyboards as kb
from aiogram.fsm.state import State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from player_state import PlayerStates
from aiogram import Dispatcher
from aiogram import Bot
from aiogram.types import ReplyKeyboardRemove
from error import Error, ErrorLevel
import sys
# Settings
SERVER_ID = 1
MAX_RETRIES = 5
RETRY_DELAY = 60


import logging
logger = logging.getLogger(__name__)

class EventHandler:
    def __init__(self, bot, dp):
        self.bot: Bot = bot
        self.dp: Dispatcher = dp
        self.running = False
        self.server_available = False
        kafka_bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        kafka_config = {
            "db_listen_topic": os.environ['TOPIC_DB_LISTEN'],
            "db_send_topic": os.environ['TOPIC_DB_SEND'],
            "game_listen_topic": os.environ['TOPIC_GAME_LISTEN'],
            "game_send_topic": os.environ['TOPIC_GAME_SEND'],
        }
        self.waiting_player = None
        self.langs = dict()

        kb.create_keyboards()

        self.kafka = KafkaHandler(kafka_bootstrap_servers, kafka_config)

    async def start(self):
        await self.kafka.start()
        self.running = True
        self.server_check_task = asyncio.create_task(self._check_server_availability())

    async def stop(self):
        await self.kafka.stop()
        self.running = False
        if self.server_check_task:
            self.server_check_task.cancel()
            try:
                await self.server_check_task
            except asyncio.CancelledError:
                pass

    async def _check_server_availability(self):
        while self.running:
            try:
                if self.server_available:
                    if not await self._ping_game_server():
                        self.server_available = False
                        logger.error("Game server disconnected")
                    else:
                        logger.info("Ping game server successfully")
                else:
                    logger.info("Attempting to reconnect to game server...")
                    if await self.initGameServer():
                        self.server_available = True
                        logger.info("Game server reconnected successfully")
                    else:
                        logger.warning("Failed to reconnect to game server")
                await asyncio.sleep(60)
            except Exception as e:
                msg = f"Failed to connect to game server: {str(e)}"
                logger.error(msg)
                await asyncio.sleep(60)


    async def _ping_game_server(self):
        try:
            init_server = {
                "command": 2,  # SERVER_INFO command
                "server_id": SERVER_ID,
            }
            game_response = await self.kafka.request_to_game(init_server, timeout=5)
            ok = await self._verify_server_response(game_response)
            ok = ok and game_response['result'] == 1
            return ok
        except Exception as e:
            msg = f"Failed to ping game server: {str(e)}"
            logger.error(msg)
            return False

    async def _verify_server_response(self, response, user_id=None):
        if response is None or "timeout" in response and response["timeout"]:
            if self.server_available:
                logger.error("Game server timeout detected, marking as unavailable")
                self.server_available = False
                if user_id:
                    lang = self.langs.get(user_id, "en")
                    await self.bot.send_message(
                        chat_id=user_id,
                        text=phrases.dict("serverUnavailable", lang)
                    )
            return False
        return True

    async def _handle_server_unavailable(self, user_id):
        if not self.server_available and user_id:
            lang = self.langs.get(user_id, "en")
            await self.bot.send_message(
                chat_id=user_id,
                text=phrases.dict("serverUnavailableTryLater", lang),
                reply_markup=kb.main[lang]
            )
        return False

    async def handle_error(self, user_id: int, error: Error):
        try:
            lang = self.langs.get(user_id, "en")
            if error.level == ErrorLevel.INFO:
                logger.info(error)
            elif error.level == ErrorLevel.WARNING:
                logger.info(error)
                await self.bot.send_message(
                    chat_id=user_id,
                    text=str(error)
                )
            elif error.level == ErrorLevel.ERROR:
                logger.error(error)
                await self.bot.send_message(
                    chat_id=user_id,
                    text=phrases.dict("errorMessage", lang)
                )
            elif error.level == ErrorLevel.GAME_ERROR:
                logger.error(error)
                await self.bot.send_message(
                    chat_id=user_id,
                    text=phrases.dict("errorGame", lang)
                )
                if error.game_id != 0:
                    await self._give_up_in_game(user_id, error.game_id)

                await self.change_player_state_by_id(user_id, PlayerStates.main_menu_state)

                await self.bot.send_message(
                    chat_id=user_id,
                    text=phrases.dict('menu', lang),
                    reply_markup=kb.main[lang]
                )
            elif error.level == ErrorLevel.CRITICAL:
                logger.critical(error)
                await self.bot.send_message(
                    chat_id=user_id,
                    text=phrases.dict("errorMessage", lang)
                )

        except Exception as e:
            logger.error(f"Failed to handle error for user {user_id}: {str(e)}")

        if error.level == ErrorLevel.CRITICAL:
            sys.exit(1)

    async def initGameServer(self):
        retries = 0
        while retries < MAX_RETRIES:
            try:

                server_up = False
                init_server = {
                    "command": 1,  # INIT_GAME command
                    "server_id": SERVER_ID,
                }
                game_response = await self.kafka.request_to_game(init_server, timeout=5)
                if "timeout" in game_response and game_response["timeout"]:
                    raise asyncio.TimeoutError()
                if not game_response or 'result' not in game_response:
                    raise Exception("Invalid response from game service")
                if game_response['result'] != 1:  # SUCCESS code
                    if game_response['result'] == 4: # SERVER_ALREADY_REGESTERED
                        server_up = True
                    else:
                        raise Exception(f"Game service error: {game_response['result']}")

                server_data = {
                    "command": "get_server_games",
                    "data": SERVER_ID,
                }
                db_response = await self.kafka.request_to_db(server_data, timeout=10)
                if "timeout" in db_response and db_response["timeout"]:
                    raise asyncio.TimeoutError()
                if not db_response:
                    raise Exception("No response from database")

                required_fields = ['games', 'players', 'player_games', 'computer_games', 'history']
                for field in required_fields:
                    if field not in db_response:
                        raise Exception(f"Invalid response from database: missing {field}")

                for player in db_response['players']:
                    user_id = player['player_id']
                    state_name = player['state']
                    storage_key = StorageKey(chat_id=user_id, user_id=user_id, bot_id=self.bot.id)
                    state = FSMContext(storage=self.dp.fsm.storage, key=storage_key)
                    await state.set_state(state_name)
                    self.langs[user_id] = player['lang']

                    if await state.get_state() == PlayerStates.waiting_a_rival:
                        self.waiting_player = player
                    logger.info(f"restore user - {user_id} state - {await state.get_state()}")

                if server_up:
                    return True

                games = []
                for game in db_response['games']:
                    game_dict = {
                        "command": 18,  # RESTORE_GAME command
                        "server_id": SERVER_ID,
                        "game_id": game['id'],
                        "game_value": game['secret_value'],
                        "restore_data": [],
                        "players": [],
                    }
                    games.append(game_dict)

                for player_game in db_response['player_games']:
                    for game in games:
                        if player_game['game_id'] == game['game_id']:
                            game['players'].append({
                                "id": player_game['player_id'],
                                "is_host": player_game['is_host'],
                            })
                            break

                for history in db_response['history']:
                    for game in games:
                        if history['game_id'] == game['game_id']:
                            game['restore_data'].append({
                                "id": history['player_id'],
                                "player": not history['is_computer'],
                                "bulls": history['bulls'],
                                "cows": history['cows'],
                                "step": history['step'],
                                "game_value": history['game_value'],
                                "give_up": history['is_give_up']
                            })
                            break

                for computer in db_response['computer_games']:
                    for game in games:
                        if computer['game_id'] == game['game_id']:
                            if 'brains' not in game:
                                game['brains'] = []
                            game['brains'].append({
                                "id": computer['computer_id'],
                                "player_id": computer['player_id'],
                                "brain": computer['game_brain'],
                            })

                for game in games:
                    logger.info(f"restore game {game['game_id']}")
                    game_response = await self.kafka.request_to_game(game, timeout=5)
                    if "timeout" in game_response and game_response["timeout"]:
                        raise asyncio.TimeoutError()
                    if not game_response or 'result' not in game_response:
                        raise Exception("Invalid start game response from game service")


                    if game_response['result'] != 1:  # SUCCESS code
                        raise Exception(f"Game service error: {game_response['result']}")

                return True

            except asyncio.TimeoutError:
                retries += 1
                logger.error(f"Timeout during init game server (attempt {retries}/{MAX_RETRIES})")
                if retries < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
                continue

            except Exception as e:
                retries += 1
                logger.exception(f"Failed to init game server (attempt {retries}/{MAX_RETRIES}): {str(e)}")
                if retries < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
                continue

        logger.critical("Failed to initialize game server after maximum retries")
        return False

    async def insert_player(self, message: Any, state: FSMContext):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False
        await state.set_state(PlayerStates.main_menu_state)
        self.langs[message.from_user.id] = message.from_user.language_code
        player = {
            "player_id": message.from_user.id,
            "firstname": message.from_user.first_name,
            "lastname": message.from_user.last_name,
            "fullname": message.from_user.full_name,
            "username": message.from_user.username,
            "lang": message.from_user.language_code,
            "state": await state.get_state()
        }
        return await self._insert_player(player)


    async def change_player(self, callback: types.CallbackQuery, state: FSMContext, new_state: State):
        if not self.server_available:
            await self._handle_server_unavailable(callback.from_user.id)
            return False
        await state.set_state(new_state)
        if callback.from_user.id not in self.langs:
            self.langs[callback.from_user.id] = callback.from_user.language_code
        player = {
            "player_id": callback.from_user.id,
            "firstname": callback.from_user.first_name,
            "lastname": callback.from_user.last_name,
            "fullname": callback.from_user.full_name,
            "username": callback.from_user.username,
            "lang": callback.from_user.language_code,
            "state": await state.get_state()
        }
        return await self._update_player(player)


    async def change_player(self, message: types.Message, state: FSMContext, new_state: State):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False
        await state.set_state(new_state)
        if message.from_user.id not in self.langs:
            self.langs[message.from_user.id] = message.from_user.language_code
        player = {
            "player_id": message.from_user.id,
            "firstname": message.from_user.first_name,
            "lastname": message.from_user.last_name,
            "fullname": message.from_user.full_name,
            "username": message.from_user.username,
            "lang": message.from_user.language_code,
            "state": await state.get_state()
        }
        return await self._update_player(player)

    async def change_player_state_by_id(self, player_id, player_state : PlayerStates):
        storage_key = StorageKey(chat_id=player_id, user_id=player_id, bot_id=self.bot.id)
        state = FSMContext(storage=self.dp.fsm.storage, key=storage_key)
        await state.set_state(player_state)
        player = {
            "player_id": player_id,
            "state": await state.get_state()
        }
        return await self._update_player(player)

    async def _insert_player(self, player: Any):
        player_data = {
            "command": "insert_player",
            "data": player,
            "timestamp": str(datetime.now())
        }
        try:
            await self.kafka.send_to_db(player_data)

            logger.info(f"Successfully sent player insert for user_id: {player['player_id']}")
            return True
        except Exception as e:
            msg = f"Failed to send player data to Kafka: {str(e)}"
            await self.handle_error(player['player_id'], Error(ErrorLevel.ERROR, msg))
            return False

    async def _update_player(self, player: Any):
        player_data = {
            "command": "change_player",
            "data": player,
            "timestamp": str(datetime.now())
        }
        try:
            await self.kafka.send_to_db(player_data)
            logger.info(f"Successfully sent player update for user_id: {player['player_id']}")
            return True
        except Exception as e:
            msg = f"Failed to send player data to Kafka: {str(e)}"
            await self.handle_error(player['player_id'], Error(ErrorLevel.ERROR, msg))
            return False

    async def _create_game(self, player_id, mode) -> None:
        logger.info(f"Starting game creation for user_id: {player_id}")
        try:
            create_msg = {
                "command": "create_game",
                "data": {
                    "id": 0,
                    "server_id": SERVER_ID,
                    "stage": "WAIT_A_NUMBER",
                    "step": 0,
                    "secret_value": 0,
                    "mode": mode,
                },
                "timestamp": str(datetime.now())
            }

            db_response = await self.kafka.request_to_db(create_msg, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
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

            await self.kafka.send_to_db(player_msg)
            logger.info(f"Player {player_id} added to game {game_id}")

            game_msg = {
                "command": 4,  # Assuming 4 is CREATE_GAME command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not await self._verify_server_response(game_response, player_id):
                return False
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid create game response from game service")

            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")

            return game_id

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to create game for user {player_id}"
            await self.handle_error(player_id, msg)

        return 0

    async def _add_player_to_game(self, game_id, player_id) -> bool:
        try:
            player_msg = {
                "command": "add_player_game",
                "data": {
                    "player_id": player_id,
                    "server_id": SERVER_ID,
                    "game_id": game_id,
                    "is_current_game": False,
                    "is_host": False,
                },
                "timestamp": str(datetime.now())
            }

            await self.kafka.send_to_db(player_msg)
            logger.info(f"Player {player_id} added to game {game_id}")

            set_current_game = {
                "command": "set_current_game",
                "data": {
                    "player_id": player_id,
                    "server_id": SERVER_ID,
                    "game_id": game_id,
                    "is_current_game": True,
                    "is_host": False,
                },
                "timestamp": str(datetime.now())
            }
            await self.kafka.send_to_db(set_current_game)
            logger.info(f"Player {player_id} set current game {game_id}")

            game_msg = {
                "command": 5,  # Assuming 5 is ADD_PLAYER command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not await self._verify_server_response(game_response, player_id):
                return False
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid start game response from game service")

            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")

            return True

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to add player for user {player_id}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False

    async def _add_computer_to_game(self, game_id, player_id, level) -> bool:
        try:
            create_msg = {
                "command": "create_computer",
                "data": {
                    "computer_id": 0,
                    "player_id": player_id,
                    "server_id": SERVER_ID,
                    "game_id": game_id,
                    "game_brain": level,
                },
                "timestamp": str(datetime.now())
            }

            db_response = await self.kafka.request_to_db(create_msg, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
            if not db_response or 'id' not in db_response:
                raise Exception("Invalid response from database: missing computer ID")

            if 'table' not in db_response or db_response['table'] != 'computers':
                raise Exception("Invalid response from database: wrong table")

            computer_id = db_response['id']

            game_msg = {
                "command": 6,  # Assuming 6 is ADD_COMPUTER command
                "computer_id": computer_id,
                "player_id": player_id,
                "server_id": SERVER_ID,
                "game_id": game_id,
                "game_brain": level,
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not await self._verify_server_response(game_response, player_id):
                return False
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid add computer response from game service")

            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")

            return True

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to add player for user {player_id}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False

    async def _start_game(self, game_id, player_id) -> bool:
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
            await self.kafka.send_to_db(set_current_game)
            logger.info(f"Player {player_id} set current game {game_id}")

            game_msg = {
                "command": 7,  # Assuming 7 is START_GAME command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not await self._verify_server_response(game_response, player_id):
                return False
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
                    "mode": "unchangeable",
                },
                "timestamp": str(datetime.now())
            }
            await self.kafka.send_to_db(start_game_message)
            logger.info(f"Game {game_id} fully initialized")

            return True

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to create game for user {player_id}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False

    async def _give_up_in_game(self, player_id, game_id):
        try:
            lang = self.langs.get(player_id, "en")
            game_msg = {
                "command": 11,  # Assuming 11 is PLAYER_GIVE_UP command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not await self._verify_server_response(game_response, player_id):
                return False
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid give up game response from game service")

            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")

            if "steps" not in game_response:
                raise Exception(f"Game service not have this player steps")

            steps = game_response['steps']

            unfinished_players = 0
            player_i = next((i for i, step in enumerate(steps) if step['player'] and step['id'] == player_id), -1)
            for i in range(len(steps)):
                if i != player_i and steps[i]["player"] and not steps[i]["finished"]:
                    unfinished_players += 1

            if player_i == -1:
                raise Exception(f"Game service not have this player steps")

            step_message = {
                "command": "add_step",
                "data": {
                    "game_id": game_id,
                    "player_id": player_id,
                    "server_id": SERVER_ID,
                    "step": 0,
                    "game_value": 0,
                    "bulls": 0,
                    "cows": 0,
                    "is_computer": False,
                    "is_give_up": True,
                    "timestamp": datetime.now().isoformat(),
                },
                "timestamp": str(datetime.now())
            }
            await self.kafka.send_to_db(step_message)
            logger.info(f"Player {player_id} do step in game {game_id}")

            if game_response["game_stage"] != "IN_PROGRESS":
                update_game_message = {
                    "command": "update_game",
                    "data": {
                        "id": game_id,
                        "server_id": SERVER_ID,
                        "stage": game_response["game_stage"],
                        "step": steps[player_i]["step"],
                        "secret_value": 0,
                        "mode": "unchangeable",
                    },
                    "timestamp": str(datetime.now())
                }
                await self.kafka.send_to_db(update_game_message)
                logger.info(f"Player {player_id} give up in game {game_id}")

            names = await self._get_game_names(player_id, game_id)
            if names is None:
                return False

            ok = await self._send_give_up_to_other_players(steps[player_i], lang, names)
            if not ok: return False

            if game_response['game_stage'] == 'FINISHED':
                return await self._send_game_results(player_id, game_id, lang, names)
            elif steps[player_i]['finished'] and unfinished_players == 0:
                ok = await self._finish_game(player_id, game_id, names)
                if not ok: return False

            ok = await self.change_player_state_by_id(player_id, PlayerStates.main_menu_state)
            if not ok: return False
            await self.bot.send_message( chat_id=player_id, text=phrases.dict("youGaveUp", lang), reply_markup=kb.main[lang] )
            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to give up in game for user {player_id} error - {e}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False

    async def _finish_game(self, player_id, game_id, names):
        try:
            lang = self.langs.get(player_id, "en")
            game_msg = {
                "command": 15,  # Assuming 15 is FINISH_GAME command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id,
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not await self._verify_server_response(game_response, player_id):
                return False
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid do step response from game service")

            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")

            steps = game_response['steps']
            ok = await self._save_computer_steps(steps, player_id, game_id)
            if not ok: return False

            max_step = max(step['step'] for step in steps)
            update_game_message = {
                "command": "update_game",
                "data": {
                    "id": game_id,
                    "server_id": SERVER_ID,
                    "stage": game_response["game_stage"],
                    "step": max_step,
                    "secret_value": 0,
                    "mode": "unchangeable",
                },
                "timestamp": str(datetime.now())
            }
            await self.kafka.send_to_db(update_game_message)
            logger.info(f"Game {game_id} force finish")
            return await self._send_game_results(player_id, game_id, lang, names)

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to finish game in game for user {player_id}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False

    async def _send_game_results(self, player_id, game_id, lang, names):
        try:

            get_current_players_msg = {
                "command": "get_current_players",
                "data": game_id,
            }
            db_response = await self.kafka.request_to_db(get_current_players_msg, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
            if not db_response or 'player_ids' not in db_response:
                raise Exception("Invalid get current players response from database")

            current_player_ids = set(db_response['player_ids'])

            game_msg = {
                "command": 16,  # Assuming 16 is GAME_RESULT command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id,
                }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not await self._verify_server_response(game_response, player_id):
                return False
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid do step response from game service")

            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")

            game_result = game_response['game_results']
            player_i = next((i for i, step in enumerate(game_result) if step['player'] and step['id'] == player_id), -1)
            if player_i == -1:
                raise Exception(f"Game service not have this player for game result")

            for i in range(len(game_result)):
                if game_result[i]['player'] and game_result[i]['id'] in current_player_ids:
                    result = self._generate_game_results(game_result, i, lang, names)
                    await self.bot.send_message( chat_id=game_result[i]['id'], text=result, reply_markup=kb.full_game[lang] )

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = str(e)
            error = Error(ErrorLevel.GAME_ERROR, msg)
            error.game_id = game_id
            await self.handle_error(player_id, error)

    async def _send_game_report(self, player_id, game_id, lang, names):
        try:
            get_game_report_msg = {
                    "command": "get_game_report",
                    "data": game_id,
                }
            db_response = await self.kafka.request_to_db(get_game_report_msg, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
            if not db_response or 'steps' not in db_response:
                raise Exception("Invalid get game report response from database")

            game_report = db_response['steps']

            report_text = self._generate_game_report(game_report, lang, names)
            await self.bot.send_message(
                chat_id=player_id,
                text=report_text,
                reply_markup=kb.main[lang]
            )

            return True

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = str(e)
            error = Error(ErrorLevel.GAME_ERROR, msg)
            error.game_id = game_id
            await self.handle_error(player_id, error)

        return False


    async def start_single_game(self, message: types.Message, state: FSMContext):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False
        game_id = await self._create_game(message.from_user.id, "single")
        if game_id == 0: return False
        logger.info(f"Starting single game for user_id: {message.from_user.id}")
        ok = await self._start_game(game_id, message.from_user.id)
        if ok:
            lang = self.langs.get(message.from_user.id, "en")
            await message.answer(f"{phrases.dict("gameCreated", lang)} {phrases.dict("yourTurn", lang)}", reply_markup=ReplyKeyboardRemove())
            await self.change_player(message, state, PlayerStates.waiting_for_number)
        else:
            await message.answer("Failed to start game")
        return ok


    async def start_random_game(self, message: types.Message, state: FSMContext):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False
        player = {
            "player_id": message.from_user.id,
            "firstname": message.from_user.first_name,
            "lastname": message.from_user.last_name,
            "fullname": message.from_user.full_name,
            "username": message.from_user.username,
            "lang": self.langs[message.from_user.id],
            "state": await state.get_state()
        }
        if self.waiting_player is None or self.waiting_player["player_id"] == message.from_user.id:
            lang = self.langs.get(message.from_user.id, "en")
            ok = await self.change_player(message, state, PlayerStates.waiting_a_rival)
            if not ok: return False
            player['state'] = await state.get_state()
            self.waiting_player = player
            asyncio.create_task(self._remove_waiting_player_after_timeout(message.from_user.id, lang))
            await message.answer(f"{phrases.dict('waitingForOpponent', lang)}", reply_markup=ReplyKeyboardRemove())
        else:
            _waiting_player = self.waiting_player
            self.waiting_player = None
            ok = await self._start_random_game(_waiting_player, player)
            if not ok: return False

    async def _remove_waiting_player_after_timeout(self, player_id: int, lang: str):
        await asyncio.sleep(60)

        if self.waiting_player and self.waiting_player["player_id"] == player_id:
            try:
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict('waitingTimeout', lang),
                    reply_markup=kb.main[lang]
                )

                ok = await self.change_player_state_by_id(
                    player_id,
                    PlayerStates.main_menu_state
                )
                if not ok: return False

            except Exception as e:
                logging.error(f"Error notifying player about timeout: {e}")
            finally:
                self.waiting_player = None
                return True

    async def _start_random_game(self, player1, player2):
        try:
            game_id = await self._create_game(player1['player_id'], "random")
            if game_id == 0: return False

            ok = await self._add_player_to_game(game_id, player2['player_id'])
            if not ok: return False

            ok = await self._start_game(game_id, player1['player_id'])
            if not ok: return False

            for player in [player1, player2]:
                ok = await self.change_player_state_by_id(player['player_id'], PlayerStates.waiting_for_number)
                if not ok: continue

                lang = self.langs.get(player['player_id'], "en")
                await self.bot.send_message(
                    chat_id=player['player_id'],
                    text=f"{phrases.dict('gameCreated', lang)}\n{phrases.dict('yourTurn', lang)}",
                    reply_markup=ReplyKeyboardRemove()
                )

            return True

        except Exception as e:
            for player in [player1, player2]:
                msg = f"Failed to start random game: {e}"
                await self.handle_error(player2["player_id"], Error(ErrorLevel.ERROR, msg))

    def _generate_step_result(self, player_step, lang):
        result = ""
        game_value = player_step["game_value"]
        result += f"{phrases.dict('step', lang)} {player_step["step"]}"
        result += f"\n{game_value:04}: {player_step["bulls"]}{phrases.dict("bulls", lang)} {player_step["cows"]}{phrases.dict("cows", lang)}"
        return result

    async def _get_current_game(self, player_id):
        try:
            get_current_game_msg = {
                    "command": "get_current_game",
                    "data": player_id,
                }
            db_response = await self.kafka.request_to_db(get_current_game_msg, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
            if not db_response or 'id' not in db_response or 'finished' not in db_response:
                raise Exception("Invalid get current game response from game service")

            return [db_response['id'], db_response['finished']]

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = str(e)
            error = Error(ErrorLevel.ERROR, msg)
            await self.handle_error(player_id, error)

        return 0

    async def _get_game_names(self, player_id, game_id):
        try:
            create_msg = {
                "command": "get_game_names",
                "data": game_id,
                "timestamp": str(datetime.now())
            }

            db_response = await self.kafka.request_to_db(create_msg, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()

            if not db_response or 'names' not in db_response:
                raise Exception("Invalid response from database: wrong names")

            return db_response['names']

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = str(e)
            if game_id != 0:
                error = Error(ErrorLevel.GAME_ERROR, msg)
                await self.handle_error(player_id, error)
            else:
                error = Error(ErrorLevel.ERROR, msg)
                await self.handle_error(player_id, error)

        return None


    def _generate_steps_result(self, steps, player_i, lang, names, step_number):
        result = ""
        player_step = steps[player_i]
        game_value = player_step["game_value"]
        result += f"{phrases.dict('step', lang)} {step_number}"
        cur_steps = [d for d in steps if d['step'] == step_number and d['id'] != player_step['id']]
        if len(cur_steps) > 0:
            result += f"\n{phrases.dict("you", lang)} -  {game_value:04}: {player_step["bulls"]}{phrases.dict("bulls", lang)} {player_step["cows"]}{phrases.dict("cows", lang)}"
            for i in range(len(cur_steps)):
                name = next((d['name'] for d in names if d['id'] == cur_steps[i]['id'] and d['is_player'] == cur_steps[i]['player']), "Unknown")
                if cur_steps[i]["player"]:
                    result += f"\n{phrases.dict('player', lang)} {name}"
                else:
                    result += f"\n{phrases.dict('bot', lang)} {name}"
                result += f" - {'*'*4}: {cur_steps[i]["bulls"]}{phrases.dict("bulls", lang)} {cur_steps[i]["cows"]}{phrases.dict("cows", lang)}"
        else:
            result += f"\n{game_value:04}: {player_step["bulls"]}{phrases.dict("bulls", lang)} {player_step["cows"]}{phrases.dict("cows", lang)}"
        return result

    async def _send_give_up_to_other_players(self, player_step, lang, names):
        name = next((d['name'] for d in names if d['id'] == player_step['id'] and d['is_player']), "Unknown")
        result = f"{phrases.dict('player', lang)} {name} {phrases.dict('giveUp', lang)}"
        for name in names:
                if name['id'] != player_step['id'] and name['is_player']:
                    await self.bot.send_message( chat_id=name['id'], text=result )

    async def _send_step_to_other_players(self, player_step, lang, names, current_player_ids):
        name = next((d['name'] for d in names if d['id'] == player_step['id'] and d['is_player']), "Unknown")
        result = f"{phrases.dict('player', lang)} {name} {phrases.dict('makeStep', lang)} {player_step['step']}"
        for name in names:
                if name['id'] != player_step['id'] and name['is_player'] and player_step['id'] in current_player_ids:
                    await self.bot.send_message( chat_id=name['id'], text=result )

    def _generate_game_report(self, game_steps, lang, names):
        if not game_steps:
            return phrases.dict('noGameSteps', lang)

        sorted_steps = sorted(game_steps, key=lambda x: x.get('timestamp', '1970-01-01T00:00:00Z'))

        report = f"{phrases.dict('fullGame', lang)}\n"
        report += "=" * 30 + "\n"

        for step in sorted_steps:
            step_num = step.get('step', 0)
            player_id = step.get('player_id', 0)
            game_value = step.get('game_value', 0)
            bulls = step.get('bulls', 0)
            cows = step.get('cows', 0)
            is_computer = step.get('is_computer', False)
            is_give_up = step.get('is_give_up', False)
            timestamp = step.get('timestamp', '')

            name = "Unknown"
            for n in names:
                if n['id'] == player_id and n['is_player'] != is_computer:
                    name = n['name']
                    break

            time_display = ""
            if timestamp:
                try:
                    if 'T' in timestamp:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        time_display = f" [{dt.strftime('%Y.%m.%d %H:%M:%S')}]"
                    else:
                        time_display = f" [{timestamp}]"
                except:
                    time_display = f" [{timestamp}]"

            if is_give_up:
                if is_computer:
                    report += f"{time_display} {phrases.dict('bot', lang)} {name}: {phrases.dict('giveUp', lang)}\n"
                else:
                    report += f"{time_display} {phrases.dict('player', lang)} {name}: {phrases.dict('giveUp', lang)}\n"
            else:
                if is_computer:
                    report += f"{time_display} {step_num}. {phrases.dict('bot', lang)} {name} {game_value:04d} {bulls}{phrases.dict('bulls', lang)} {cows}{phrases.dict('cows', lang)}\n"
                else:
                    report += f"{time_display} {step_num}. {phrases.dict('player', lang)} {name} {game_value:04d} {bulls}{phrases.dict('bulls', lang)} {cows}{phrases.dict('cows', lang)}\n"

        return report


    def _generate_game_results(self, game_results, player_i, lang, names):
        result = f"{phrases.dict('gameFinished', lang)}"
        if len(game_results) == 2 and game_results[0]['place'] == game_results[1]['place']:
            return f"{result} {phrases.dict("draw", lang)}"
        elif game_results[player_i]['place'] == 1:
            if game_results[player_i]['give_up']:
                result += f" {phrases.dict("gaveUp", lang)}"
            else:
                result += f" {phrases.dict("youWon", lang)}"
        if len(game_results) > 1:
            sorted_results = sorted(game_results, key=lambda x: x['place'])
            result += f"\n{phrases.dict('gameResults', lang)}\n"
            for i in range(len(sorted_results)):
                name = next((d['name'] for d in names if d['id'] == sorted_results[i]['id'] and d['is_player'] == sorted_results[i]['player']), "Unknown")
                if i == player_i:
                    name = phrases.dict('you', lang)
                elif sorted_results[i]['player']:
                    name = f"{phrases.dict('player', lang)} {name}"
                else:
                    name = f"{phrases.dict('bot', lang)} {name}"
                result += f'{sorted_results[i]['place']}. {name} - {sorted_results[i]['step']} {phrases.dict('steps', lang)}'
                if sorted_results[i]['give_up']:
                    result += f" {phrases.dict('gaveUp', lang)}\n"
                else:
                    result += f"\n"
        return result


    async def _save_computer_steps(self, steps, player_id, game_id):
        try:
            for i in range(len(steps)):
                if steps[i]["player"]:
                    continue
                step_message = {
                    "command": "add_step",
                    "data": {
                        "game_id": game_id,
                        "player_id": steps[i]["id"],
                        "server_id": SERVER_ID,
                        "step": steps[i]["step"],
                        "game_value": steps[i]["game_value"],
                        "bulls": steps[i]["bulls"],
                        "cows": steps[i]["cows"],
                        "is_computer": not steps[i]["player"],
                        "is_give_up": False,
                        "timestamp": datetime.now().isoformat(),
                    },
                    "timestamp": str(datetime.now())
                }
                await self.kafka.send_to_db(step_message)
                logger.info(f"Computer {steps[i]["id"]} do step in game {game_id}")

            return True

        except Exception as e:
            msg = str(e)
            if game_id != 0:
                error = Error(ErrorLevel.GAME_ERROR, msg)
                await self.handle_error(player_id, error)
            else:
                error = Error(ErrorLevel.ERROR, msg)
                await self.handle_error(player_id, error)

        return False

    async def _enter_by_lobby_id(self, player_id, lobby_id):
        lang = self.langs.get(player_id, "en")
        try:
            join_data = {
                "command": "join_lobby",
                "data": {
                    "lobby_id": lobby_id,
                    "player_id": player_id,
                    "password": ""
                },
                "timestamp": str(datetime.now())
            }

            db_response = await self.kafka.request_to_db(join_data, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
            if not db_response or 'success' not in db_response:
                raise Exception("Invalid response from database")

            if not db_response['success']:
                error_msg = db_response.get('error', 'Unknown error')
                if error_msg == "DBAnswerPasswordNeeded":
                    await self.change_player_state_by_id(player_id, PlayerStates.enter_password)
                    await self.bot.send_message(
                        chat_id=player_id,
                        text=phrases.dict("enterLobbyPassword", lang),
                        reply_markup=ReplyKeyboardRemove()
                    )
                    return True
                else:
                    await self.bot.send_message(
                        chat_id=player_id,
                        text=phrases.dict(error_msg, lang),
                        reply_markup=kb.game[lang]
                    )
                    return False

            game_id = db_response.get('id', 0)
            if game_id == 0: return False

            ok = await self._add_player_to_game(game_id, player_id)
            if not ok: return False

            await self.change_player_state_by_id(player_id, PlayerStates.in_lobby)
            await self.bot.send_message(
                chat_id=player_id,
                text=phrases.dict("youEnteredLobby", lang),
                reply_markup=kb.get_lobby_keyboard(False, False, lang)
            )

            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to enter lobby for user {player_id}: {str(e)}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False

    async def _get_lobby_id(self, player_id):
        try:
            lang = self.langs.get(player_id, "en")
            get_lobby_msg = {
                "command": "get_lobby_id",
                "data": player_id,
                "timestamp": str(datetime.now())
            }

            db_response = await self.kafka.request_to_db(get_lobby_msg, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
            if not db_response or 'id' not in db_response:
                raise Exception("Invalid response from database: missing lobby ID")

            if db_response["id"] == 0:
                raise Exception("Invalid response from database: Player is not in the lobby")
            return db_response['id']

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to enter lobby with password for user {player_id}: {str(e)}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return 0


    async def do_step(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        if not self.server_available:
            await self._handle_server_unavailable(player_id)
            return False
        game_id = 0
        steps = None
        try:
            lang = self.langs.get(player_id, "en")
            if not message.text.isdigit():
                msg = phrases.dict("invalidNumberFormat", lang)
                await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))
                return False

            [game_id, _] = await self._get_current_game(player_id)
            if game_id == 0:
                return False

            game_value = int(message.text)
            game_msg = {
                "command": 8,  # Assuming 8 is PLAYER_STEP command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id,
                "game_value": game_value,
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if not await self._verify_server_response(game_response, player_id):
                return False
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid do step response from game service")

            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code

                if game_response['result'] == 13:  # Assuming 13 is INVALID_GAME_VALUE code
                    msg = phrases.dict("errorInvalidGameValue", lang)
                    await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))
                    return False

                if game_response['result'] == 20:  # Assuming 20 is ERROR_PLAYER_ALREADY_MADE_STEP code
                    msg = phrases.dict("errorNotAllPlayersMadeStep", lang)
                    await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))
                    return False

                raise Exception(f"Game service error: {game_response['result']}")

            if "steps" not in game_response:
                raise Exception(f"Game service not have this player steps")

            steps = game_response['steps']
            player_i = next((i for i, step in enumerate(steps) if step['player'] and step['id'] == player_id), -1)
            if player_i == -1:
                raise Exception(f"Game service not have this player steps")

            step_number = steps[player_i]["step"]

            step_message = {
                "command": "add_step",
                "data": {
                    "game_id": game_id,
                    "player_id": player_id,
                    "server_id": SERVER_ID,
                    "step": step_number,
                    "game_value": steps[player_i]["game_value"],
                    "bulls": steps[player_i]["bulls"],
                    "cows": steps[player_i]["cows"],
                    "is_computer": not steps[player_i]["player"],
                    "is_give_up": False,
                    "timestamp": datetime.now().isoformat(),
                },
                "timestamp": str(datetime.now())
            }
            await self.kafka.send_to_db(step_message)
            logger.info(f"Player {player_id} do step in game {game_id}")
            computer_steps = [step for step in steps if (not step["player"] and step["step"] == step_number)]
            ok = await self._save_computer_steps(computer_steps, player_id, game_id)
            if not ok: return False

            update_game_message = {
                "command": "update_game",
                "data": {
                    "id": game_id,
                    "server_id": SERVER_ID,
                    "stage": game_response["game_stage"],
                    "step": step_number,
                    "secret_value": 0,
                    "mode": "unchangeable",
                },
                "timestamp": str(datetime.now())
            }
            await self.kafka.send_to_db(update_game_message)
            logger.info(f"Player {player_id} do step in game {game_id}")

            names = await self._get_game_names(player_id, game_id)
            if names is None:
                return False

            unfinished_players = 0
            unstepped_players = 0
            if 'unstepped_players' in game_response:
                unstepped_players = game_response["unstepped_players"]

            for i in range(len(steps)):
                if i != player_i and steps[i]["player"] and not steps[i]["finished"]:
                    unfinished_players += 1

            get_current_players_msg = {
                "command": "get_current_players",
                "data": game_id,
            }
            db_response = await self.kafka.request_to_db(get_current_players_msg, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
            if not db_response or 'player_ids' not in db_response:
                raise Exception("Invalid get current players response from database")

            current_player_ids = set(db_response['player_ids'])

            await self._send_step_to_other_players(steps[player_i], lang, names, current_player_ids)
            if unstepped_players > 0:
                result = self._generate_step_result(steps[player_i], lang)
                result += f"\n{unstepped_players} {phrases.dict('waitPlayers', lang)}"
                await message.answer(result, reply_markup=ReplyKeyboardRemove())
            else:
                for i in range(len(steps)):
                    if steps[i]["player"] and not steps[i]['give_up']:
                        result = self._generate_steps_result(steps, i, lang, names, step_number)
                        await self.bot.send_message( chat_id=steps[i]['id'], text=result, reply_markup=ReplyKeyboardRemove() )


            if game_response['game_stage'] == 'FINISHED':
                return await self._send_game_results(player_id, game_id, lang, names)
            elif steps[player_i]['finished'] and unfinished_players == 0:
                ok = await self._finish_game(player_id, game_id, names)
                if not ok: return False
            return True

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = str(e)
            if game_id != 0:
                error = Error(ErrorLevel.GAME_ERROR, msg)
                if steps != None and player_i != -1 and "finished" in steps[player_i] and not steps[player_i]["finished"]:
                    error.game_id = game_id
                await self.handle_error(player_id, error)
            else:
                error = Error(ErrorLevel.ERROR, msg)
                await self.handle_error(player_id, error)

        return False

    async def start_bot_play( self, message: types.Message, state: FSMContext ):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False
        player_id = message.from_user.id
        game_id = await self._create_game(player_id, "versus_bot")
        if game_id == 0:
            return False
        try:
            lang = self.langs.get(message.from_user.id, "en")
            level = None
            if phrases.checkPhrase("easy", str(message.text)):
                level = "Easy"
            if phrases.checkPhrase("medium", str(message.text)):
                level = "Medium"
            if phrases.checkPhrase("hard", str(message.text)):
                level = "Hard"
            if level is None:
                msg = phrases.dict("invalidBotLevel", lang)
                await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

            logger.info(f"Starting game for user_id: {player_id}")

            ok = await self._add_computer_to_game(game_id, player_id, level)
            if not ok: return False

            logger.info(f"Starting single game for user_id: {player_id}")
            ok = await self._start_game(game_id, player_id)
            if not ok: return False

            await message.answer(f"{phrases.dict("gameCreated", lang)} {phrases.dict("yourTurn", lang)}\n{phrases.dict("giveUpInfo", lang)}", reply_markup=ReplyKeyboardRemove())
            return await self.change_player(message, state, PlayerStates.waiting_for_number)

        except asyncio.TimeoutError:
            lang = self.langs.get(message.from_user.id, "en")
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(message.from_user.id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to create game for user {player_id}"
            await self.handle_error(message.from_user.id, Error(ErrorLevel.ERROR, msg))

        return False

    async def send_feedback(self, message: types.Message, state: FSMContext ):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False
        username = message.from_user.username
        feedback = message.text
        player_id = message.from_user.id

        try:
            feedback_msg = {
                "command": "feedback",
                "data": {
                    "username": username,
                    "message": feedback,
                },
                "timestamp": str(datetime.now())
            }

            await self.kafka.send_to_db(feedback_msg)
            logger.info(f"Player {player_id} send feedback")

            lang = self.langs.get(message.from_user.id, "en")
            await message.answer(f"{phrases.dict("feedbackSent", lang)}", reply_markup=kb.main[lang])
            return await self.change_player(message, state, PlayerStates.main_menu_state)

        except asyncio.TimeoutError:
            lang = self.langs.get(message.from_user.id, "en")
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(message.from_user.id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to send feedback for user {player_id}, error - {e}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False

    async def change_lang(self, message: types.Message, state: FSMContext ):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False
        for lang in phrases.langs():
            if phrases.dict('name', lang) == str(message.text):
                player = {
                    "player_id": message.from_user.id,
                    "firstname": message.from_user.first_name,
                    "lastname": message.from_user.last_name,
                    "fullname": message.from_user.full_name,
                    "username": message.from_user.username,
                    "lang": lang,
                    "state": await state.get_state()
                }
                player_data = {
                    "command": "update_lang_player",
                    "data": player,
                    "timestamp": str(datetime.now())
                }
                try:
                    await self.kafka.send_to_db(player_data)
                    logger.info(f"Successfully sent player update lang for user_id: {player['player_id']}")
                except Exception as e:
                    msg = f"Failed to send player data to Kafka: {str(e)}"
                    await self.handle_error(message.from_user.id, Error(ErrorLevel.ERROR, msg))

                self.langs[message.from_user.id] = lang
                await message.answer(f"{phrases.dict("langChanged", lang)}", reply_markup=kb.main[lang])
                return await self.change_player(message, state, PlayerStates.main_menu_state)

    async def give_up(self, message: types.Message, state: FSMContext ):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False

        player_id = message.from_user.id
        [game_id, _] = await self._get_current_game(player_id)
        if game_id == 0:
            return False
        return await self._give_up_in_game(player_id, game_id)


    async def game_report(self, callback: types.CallbackQuery, state: FSMContext ):
        if not self.server_available:
            await self._handle_server_unavailable(callback.from_user.id)
            return False
        try:
            player_id = callback.from_user.id
            lang = self.langs.get(player_id, "en")
            [game_id, finished] = await self._get_current_game(player_id)
            if game_id == 0:
                return False

            if not finished:
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("gameNotFinished", lang)
                )
                return True

            names = await self._get_game_names(player_id, game_id)
            if names is None:
                return False

            await self.change_player_state_by_id(player_id, PlayerStates.main_menu_state)
            return await self._send_game_report(player_id, game_id, lang, names)

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to send give up for user {player_id}, error - {e}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False


    async def exit_to_menu(self, callback: types.CallbackQuery, state: FSMContext ):
        if not self.server_available:
            await self._handle_server_unavailable(callback.from_user.id)
            return False
        try:
            player_id = callback.from_user.id
            lang = self.langs.get(player_id, "en")
            [game_id, finished] = await self._get_current_game(player_id)
            if game_id == 0:
                return False

            if not finished:
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("gameNotFinished", lang)
                )
                return True

            await self.change_player_state_by_id(player_id, PlayerStates.main_menu_state)
            await self.bot.send_message(
                chat_id=player_id,
                text=phrases.dict("menu", lang),
                reply_markup=kb.main[lang]
            )
            return True


        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to send give up for user {player_id}, error - {e}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False


    async def create_lobby(self, message: types.Message, state: FSMContext):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False

        lang = self.langs.get(message.from_user.id, "en")
        await self.change_player(message, state, PlayerStates.wait_password)
        await message.answer(
            phrases.dict("enterLobbyPassword", lang),
            reply_markup=ReplyKeyboardRemove()
        )
        return True

    async def create_lobby_with_password(self, message: types.Message, state: FSMContext):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False

        player_id = message.from_user.id
        password = message.text
        lang = self.langs.get(player_id, "en")

        try:
            game_id = await self._create_game(player_id, "lobby")
            if game_id == 0:
                return False

            lobby_data = {
                "command": "create_lobby",
                "data": {
                    "id": 0,
                    "server_id": SERVER_ID,
                    "host_id": player_id,
                    "game_id": game_id,
                    "is_private": bool(password.strip()),
                    "password": password.strip(),
                    "status": "WAITING"
                },
                "timestamp": str(datetime.now())
            }

            db_response = await self.kafka.request_to_db(lobby_data, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
            if not db_response or 'id' not in db_response:
                raise Exception("Invalid response from database: missing lobby ID")

            if 'table' not in db_response or db_response['table'] != 'lobbies':
                raise Exception("Invalid response from database: wrong table")

            lobby_id = db_response['id']
            logger.info(f"Lobby created with ID: {lobby_id}")

            await self.change_player(message, state, PlayerStates.in_lobby)
            msg = f"{phrases.dict('lobbyCreated', lang)} \n{phrases.dict('lobbyId', lang)} - {lobby_id}"
            if password.strip():
                msg += f"\n{phrases.dict('lobbyPassword', lang)} - {password}\n{phrases.dict('sendPrivate', lang)}"
            else:
                msg += f"\n{phrases.dict('lobbyPublic', lang)}"
            await message.answer(
                msg,
                reply_markup=kb.get_lobby_keyboard(True, False, lang)
            )

            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to create lobby for user {player_id}: {str(e)}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False


    async def enter_lobby(self, message: types.Message, state: FSMContext):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False

        lang = self.langs.get(message.from_user.id, "en")
        await self.change_player(message, state, PlayerStates.choose_lobby_type)
        await message.answer(
            phrases.dict("chooseLobby", lang),
            reply_markup=kb.lobby_types[lang]
        )
        return True

    async def enter_by_lobby_id(self, message: types.Message, state: FSMContext):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False

        player_id = message.from_user.id
        lobby_id = int(message.text)
        return await self._enter_by_lobby_id(player_id, lobby_id)

    async def enter_by_lobby_id_and_password(self, message: types.Message, state: FSMContext):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False

        player_id = message.from_user.id
        password = message.text
        lang = self.langs.get(player_id, "en")

        try:
            lobby_id = await self._get_lobby_id(player_id)
            if lobby_id == 0: return False

            join_data = {
                "command": "join_lobby",
                "data": {
                    "lobby_id": lobby_id,
                    "player_id": player_id,
                    "password": password
                },
                "timestamp": str(datetime.now())
            }

            db_response = await self.kafka.request_to_db(join_data, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
            if not db_response or 'success' not in db_response:
                raise Exception("Invalid response from database")

            if not db_response['success']:
                error_msg = db_response.get('error', 'Unknown error')
                if error_msg == "DBAnswerInvalidPassword":
                    await message.answer(
                        phrases.dict("wrongPassword", lang),
                        reply_markup=kb.game[lang]
                    )
                    await self.change_player(message, state, PlayerStates.choose_game)
                    return False
                else:
                    await message.answer(phrases.dict(error_msg, lang), reply_markup=kb.game[lang])
                    await self.change_player(message, state, PlayerStates.choose_game)
                    return False

            game_id = db_response.get('id', 0)
            if game_id == 0:
                await self.change_player(message, state, PlayerStates.choose_game)
                return False

            ok = await self._add_player_to_game(game_id, player_id)
            if not ok:
                await self.change_player(message, state, PlayerStates.choose_game)
                return False

            await self.change_player(message, state, PlayerStates.in_lobby)
            await message.answer(
                phrases.dict("youEnteredLobby", lang),
                reply_markup=kb.get_lobby_keyboard(False, False, lang)
            )

            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to enter lobby with password for user {player_id}: {str(e)}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        await self.change_player(message, state, PlayerStates.choose_game)
        return False

    async def enter_to_random_lobby(self, message: types.Message, state: FSMContext):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False

        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")

        try:
            get_random_lobby_msg = {
                "command": "get_random_lobby_id",
                "data": SERVER_ID,
                "timestamp": str(datetime.now())
            }

            db_response = await self.kafka.request_to_db(get_random_lobby_msg, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
            if not db_response or 'id' not in db_response:
                raise Exception("Invalid response from database: missing lobby ID")

            lobby_id = db_response['id']

            if lobby_id == 0:
                await message.answer(
                    phrases.dict("noPublicLobbies", lang),
                    reply_markup=kb.game[lang]
                )
                await self.change_player(message, state, PlayerStates.choose_game)
                return True

            return await self._enter_by_lobby_id(player_id, lobby_id)

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to enter random lobby for user {player_id}: {str(e)}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False

    async def set_player_ready_state(self, message: types.Message, state: FSMContext, is_ready: bool):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False

        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")

        try:
            lobby_id = await self._get_lobby_id(player_id)
            if lobby_id == 0: return False

            set_ready_msg = {
                "command": "set_player_ready",
                "data": {
                    "lobby_id": lobby_id,
                    "player_id": player_id,
                    "is_ready": is_ready
                },
                "timestamp": str(datetime.now())
            }

            await self.kafka.send_to_db(set_ready_msg)
            logger.info(f"Player {player_id} set ready state to {is_ready} in lobby {lobby_id}")

            current_username = message.from_user.username or message.from_user.full_name or "Player"

            get_lobby_players_msg = {
                "command": "get_lobby_players",
                "data": lobby_id,
                "timestamp": str(datetime.now())
            }

            players_response = await self.kafka.request_to_db(get_lobby_players_msg, timeout=5)
            if "timeout" in players_response and players_response["timeout"]:
                raise asyncio.TimeoutError()
            if not players_response or 'players' not in players_response:
                raise Exception("Invalid response from database: missing players")

            lobby_players = players_response['players']

            current_player_is_host = False
            for player in lobby_players:
                if player['player_id'] == player_id:
                    current_player_is_host = player['host']
                    break

            for player in lobby_players:
                if player['player_id'] != player_id:
                    other_player_lang = self.langs.get(player['player_id'], "en")
                    other_ready_status = phrases.dict("playerIsReady", other_player_lang) if is_ready else phrases.dict("playerIsNotReady", other_player_lang)
                    other_notification = f"{current_username} {other_ready_status}"

                    try:
                        await self.bot.send_message(
                            chat_id=player['player_id'],
                            text=other_notification
                        )
                    except Exception as e:
                        logger.warning(f"Failed to notify player {player['player_id']} about ready state change: {e}")

            ready_msg = phrases.dict("youReady", lang) if is_ready else phrases.dict("youNotReady", lang)
            await message.answer(
                ready_msg,
                reply_markup=kb.get_lobby_keyboard(current_player_is_host, is_ready, lang)
            )

            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to set ready state for user {player_id}: {str(e)}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False

    async def leave_lobby(self, message: types.Message, state: FSMContext):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False

        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")

        try:
            lobby_id = await self._get_lobby_id(player_id)
            if lobby_id == 0: return False

            leave_msg = {
                "command": "leave_lobby",
                "data": {
                    "lobby_id": lobby_id,
                    "player_id": player_id
                },
                "timestamp": str(datetime.now())
            }

            await self.kafka.send_to_db(leave_msg)
            logger.info(f"Player {player_id} left lobby {lobby_id}")

            await self.change_player(message, state, PlayerStates.choose_game)
            await message.answer(
                phrases.dict("chooseGameMode", lang),
                reply_markup=kb.game[lang]
            )

            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to leave lobby for user {player_id}: {str(e)}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        await self.change_player(message, state, PlayerStates.choose_game)
        await message.answer(
            phrases.dict("chooseGameMode", lang),
            reply_markup=kb.game[lang]
        )
        return False

    async def start_lobby_game(self, message: types.Message, state: FSMContext):
        if not self.server_available:
            await self._handle_server_unavailable(message.from_user.id)
            return False

        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")

        try:
            lobby_id = await self._get_lobby_id(player_id)
            if lobby_id == 0: return False

            start_game_msg = {
                "command": "start_lobby_game",
                "data": {
                    "lobby_id": lobby_id,
                    "player_id": player_id
                },
                "timestamp": str(datetime.now())
            }

            db_response = await self.kafka.request_to_db(start_game_msg, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
            if not db_response or 'id' not in db_response:
                raise Exception("Invalid response from database: missing game ID")

            game_id = db_response['id']
            logger.info(f"Player {player_id} started game {game_id} from lobby {lobby_id}")

            await self.change_player(message, state, PlayerStates.waiting_for_number)
            await message.answer(
                f"{phrases.dict('gameCreated', lang)} {phrases.dict('yourTurn', lang)}",
                reply_markup=ReplyKeyboardRemove()
            )
            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to start lobby game for user {player_id}: {str(e)}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False