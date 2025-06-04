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

    async def stop(self):
        await self.kafka.stop()
        self.running = False

    async def handle_error(self, user_id: int, error: Error):
        try:
            lang = self.langs[user_id]
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
                await self._leave_from_game(user_id, error.game_id)
                storage_key = StorageKey(chat_id=user_id, user_id=user_id, bot_id=self.bot.id)
                state = FSMContext(storage=self.dp.fsm.storage, key=storage_key)
                await state.set_state(PlayerStates.main_menu_state)
                player = {
                    "player_id": user_id,
                    "state": await state.get_state()
                }
                await self._update_player(player)
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

    async def change_player(self, message: Any, state: FSMContext, new_state: State):
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

    async def _insert_player(self, player: Any):
        player_data = {
            "command": "insert_player",
            "data": player,
            "timestamp": str(datetime.now())
        }
        try:
            await self.kafka.send_to_bd(player_data)
            
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
            await self.kafka.send_to_bd(player_data)
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

            await self.kafka.send_to_bd(player_msg)
            logger.info(f"Player {player_id} added to game {game_id}")

            game_msg = {
                "command": 4,  # Assuming 4 is CREATE_GAME command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if "timeout" in game_response and game_response["timeout"]:
                raise asyncio.TimeoutError()
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid create game response from game service")

            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")

            return game_id

        except asyncio.TimeoutError:
            lang = self.langs[player_id]
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

            await self.kafka.send_to_bd(player_msg)
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
            await self.kafka.send_to_bd(set_current_game)
            logger.info(f"Player {player_id} set current game {game_id}")

            game_msg = {
                "command": 5,  # Assuming 5 is ADD_PLAYER command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if "timeout" in game_response and game_response["timeout"]:
                raise asyncio.TimeoutError()
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid start game response from game service")

            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")

            return True

        except asyncio.TimeoutError:
            lang = self.langs[player_id]
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
            if "timeout" in game_response and game_response["timeout"]:
                raise asyncio.TimeoutError()
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid add computer response from game service")

            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                raise Exception(f"Game service error: {game_response['result']}")
            
            return True

        except asyncio.TimeoutError:
            lang = self.langs[player_id]
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
            await self.kafka.send_to_bd(set_current_game)
            logger.info(f"Player {player_id} set current game {game_id}")

            game_msg = {
                "command": 7,  # Assuming 7 is START_GAME command
                "server_id": SERVER_ID,
                "player_id": player_id,
                "game_id": game_id
            }
            game_response = await self.kafka.request_to_game(game_msg, timeout=5)
            if "timeout" in game_response and game_response["timeout"]:
                raise asyncio.TimeoutError()
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
            await self.kafka.send_to_bd(start_game_message)
            logger.info(f"Game {game_id} fully initialized")

            return True

        except asyncio.TimeoutError:
            lang = self.langs[player_id]
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(player_id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to create game for user {player_id}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            
        return False
    
    async def _leave_from_game(self, player_id, game_id):
        pass

    async def start_single_game(self, message: types.Message, state: FSMContext):
        game_id = await self._create_game(message.from_user.id, "single")
        if game_id == 0: return False
        logger.info(f"Starting single game for user_id: {message.from_user.id}")
        ok = await self._start_game(game_id, message.from_user.id)
        if ok:
            lang = self.langs[message.from_user.id]
            await message.answer(f"{phrases.dict("gameCreated", lang)} {phrases.dict("yourTurn", lang)}", reply_markup=ReplyKeyboardRemove())
            await self.change_player(message, state, PlayerStates.waiting_for_number)
        else:
            await message.answer("Failed to start game")
        return ok


    async def start_random_game(self, message: types.Message, state: FSMContext):
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
            lang = self.langs[message.from_user.id]
            ok = await self.change_player(message, state, PlayerStates.waiting_a_rival)
            if not ok: return False
            player['state'] = await state.get_state()
            self.waiting_player = player
            await message.answer(f"{phrases.dict('waitingForOpponent', lang)}", reply_markup=ReplyKeyboardRemove())
        else:
            ok = await self._start_random_game(self.waiting_player, player)
            if not ok: return False
            self.waiting_player = None

    async def _start_random_game(self, player1, player2):
        try:
            game_id = await self._create_game(player1['player_id'], "random")
            if game_id == 0: return False

            ok = await self._add_player_to_game(game_id, player2['player_id'])
            if not ok: return False

            ok = await self._start_game(game_id, player1['player_id'])
            if not ok: return False
            
            for player in [player1, player2]:
                storage_key = StorageKey(chat_id=player['player_id'], user_id=player['player_id'], bot_id=self.bot.id)
                state = FSMContext(storage=self.dp.fsm.storage, key=storage_key)
                await state.set_state(PlayerStates.waiting_for_number)
                player['state'] = await state.get_state()
                ok = await self._update_player(player)
                if not ok: continue
                
                lang = self.langs[player['player_id']]
                await self.bot.send_message(
                    chat_id=player['player_id'],
                    text=f"{phrases.dict('gameCreated', lang)}\n{phrases.dict('yourTurn', lang)}", 
                    reply_markup=ReplyKeyboardRemove()
                )

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
    
    def _generate_steps_result(self, steps, player_i, lang, names):
        result = ""
        game_value = steps[player_i]["game_value"]
        result += f"{phrases.dict('step', lang)} {steps[player_i]["step"]}"
        if len(steps) > 1:
            result += f"\n{phrases.dict("you", lang)} -  {game_value:04}: {steps[player_i]["bulls"]}{phrases.dict("bulls", lang)} {steps[player_i]["cows"]}{phrases.dict("cows", lang)}"
            for i in range(len(steps)):
                if i == player_i:
                    continue
                name = next((d['name'] for d in names if d['id'] == steps[i]['id'] and d['is_player'] == steps[i]['player']), "Unknown")
                if steps[i]["player"]:
                    result += f"\n{phrases.dict('player', lang)} {name}"
                else:
                    result += f"\n{phrases.dict('bot', lang)} {name}"
                result += f" - {'*'*4}: {steps[i]["bulls"]}{phrases.dict("bulls", lang)} {steps[i]["cows"]}{phrases.dict("cows", lang)}"
        else:
            result += f"\n{game_value:04}: {steps[player_i]["bulls"]}{phrases.dict("bulls", lang)} {steps[player_i]["cows"]}{phrases.dict("cows", lang)}"
        return result
    
    async def _send_step_to_other_players(self, player_step, lang, names):
        name = next((d['name'] for d in names if d['id'] == player_step['id'] and d['is_player']), "Unknown")
        result = f"{phrases.dict('player', lang)} {name} {phrases.dict('makeStep', lang)} {player_step['step']}"
        for name in names:
                if name['id'] != player_step['id'] and name['is_player']:
                    await self.bot.send_message( chat_id=name['id'], text=result )

    def _generate_game_results(self, game_results, player_i, lang, names):
        result = f"{phrases.dict('gameFinished', lang)}"
        if len(game_results) == 2 and game_results[0]['place'] == game_results[1]['place']:
            return f"{result} {phrases.dict("draw", lang)}"
        elif game_results[player_i]['place'] == 1:
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
                    result += f"{phrases.dict('gaveUp', lang)}\n"
                else:
                    result += f"\n"
        return result

    async def do_step(self, message: types.Message, state: FSMContext):
        game_id = 0
        try:
            lang = self.langs[message.from_user.id]
            if not message.text.isdigit():
                msg = phrases.dict("invalidNumberFormat", lang)
                await self.handle_error(message.from_user.id, Error(ErrorLevel.WARNING, msg))
                return False

            get_current_game_msg = {
                "command": "get_current_game",
                "data": message.from_user.id,
            }
            db_response = await self.kafka.request_to_db(get_current_game_msg, timeout=5)
            if "timeout" in db_response and db_response["timeout"]:
                raise asyncio.TimeoutError()
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
            if "timeout" in game_response and game_response["timeout"]:
                raise asyncio.TimeoutError()
            if not game_response or 'result' not in game_response:
                raise Exception("Invalid do step response from game service")

            if game_response['result'] != 1:  # Assuming 1 is SUCCESS code

                if game_response['result'] == 13:  # Assuming 13 is INVALID_GAME_VALUE code
                    msg = phrases.dict("errorInvalidGameValue", lang)
                    await self.handle_error(message.from_user.id, Error(ErrorLevel.WARNING, msg))
                    return False

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
                        "player_id": steps[i]["id"],
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
                    "mode": "unchangeable",
                },
                "timestamp": str(datetime.now())
            }
            await self.kafka.send_to_bd(update_game_message)
            logger.info(f"Player {message.from_user.id} do step in game {game_id}")

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

            names = db_response['names']

            unfinished_players = 0
            unstepped_players = 0
            if 'unstepped_players' in game_response:
                unstepped_players = game_response["unstepped_players"]

            for i in range(len(steps)):
                if i != player_i and steps[i]["player"] and not steps[i]["finished"]:
                    unfinished_players += 1
            
            await self._send_step_to_other_players(steps[player_i], lang, names)
            if unstepped_players > 0:
                result = self._generate_step_result(steps[player_i], lang)
                result += f"\n{unstepped_players} {phrases.dict('waitPlayers', lang)}"
                await message.answer(result, reply_markup=ReplyKeyboardRemove())
            else:
                for i in range(len(steps)):
                    if steps[i]["player"]:
                        result = self._generate_steps_result(steps, i, lang, names)
                        await self.bot.send_message( chat_id=steps[i]['id'], text=result, reply_markup=ReplyKeyboardRemove() )

            if game_response['game_stage'] == 'IN_PROGRESS_WINNER_DEFINED' and steps[player_i]['finished'] and unfinished_players == 0:
                game_msg = {
                "command": 15,  # Assuming 15 is FINISH_GAME command
                "server_id": SERVER_ID,
                "player_id": message.from_user.id,
                "game_id": game_id,
                "game_value": game_value,
                }
                game_response = await self.kafka.request_to_game(game_msg, timeout=5)
                if "timeout" in game_response and game_response["timeout"]:
                    raise asyncio.TimeoutError()
                if not game_response or 'result' not in game_response:
                    raise Exception("Invalid do step response from game service")

                if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                    raise Exception(f"Game service error: {game_response['result']}")

            if game_response['game_stage'] == 'FINISHED':

                game_msg = {
                "command": 16,  # Assuming 16 is GAME_RESULT command
                "server_id": SERVER_ID,
                "player_id": message.from_user.id,
                "game_id": game_id,
                }
                game_response = await self.kafka.request_to_game(game_msg, timeout=5)
                if "timeout" in game_response and game_response["timeout"]:
                    raise asyncio.TimeoutError()
                if not game_response or 'result' not in game_response:
                    raise Exception("Invalid do step response from game service")

                if game_response['result'] != 1:  # Assuming 1 is SUCCESS code
                    raise Exception(f"Game service error: {game_response['result']}")

                game_result = game_response['game_results']
                player_i = next((i for i, step in enumerate(game_result) if step['player'] and step['id'] == message.from_user.id), -1)
                if player_i == -1:
                    raise Exception(f"Game service not have this player for game result")

                for i in range(len(game_result)):
                    if game_result[i]['player']:
                        result = self._generate_game_results(game_result, i, lang, names)
                        await self.bot.send_message( chat_id=game_result[i]['id'], text=result, reply_markup=kb.main[lang] )
                return await self.change_player(message, state, PlayerStates.main_menu_state)
            else:
                return False

        except asyncio.TimeoutError:
            lang = self.langs[message.from_user.id]
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(message.from_user.id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = str(e)
            if game_id != 0:
                error = Error(ErrorLevel.GAME_ERROR, msg)
                error.game_id = game_id
                await self.handle_error(message.from_user.id, error)
            else:
                error = Error(ErrorLevel.ERROR, msg)
                await self.handle_error(message.from_user.id, error)

        return False

    async def start_bot_play( self, message: types.Message, state: FSMContext ):
        player_id = message.from_user.id
        game_id = await self._create_game(player_id, "versus_bot")
        if game_id == 0:
            return False
        try:
            lang = self.langs[message.from_user.id]
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

            await message.answer(f"{phrases.dict("gameCreated", lang)} {phrases.dict("yourTurn", lang)}", reply_markup=ReplyKeyboardRemove())
            return await self.change_player(message, state, PlayerStates.waiting_for_number)

        except asyncio.TimeoutError:
            lang = self.langs[message.from_user.id]
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(message.from_user.id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to create game for user {player_id}"
            await self.handle_error(message.from_user.id, Error(ErrorLevel.ERROR, msg))

        return False

    async def send_feedback(self, message: types.Message, state: FSMContext ):
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

            await self.kafka.send_to_bd(feedback_msg)
            logger.info(f"Player {player_id} send feedback")

            lang = self.langs[message.from_user.id]
            await message.answer(f"{phrases.dict("feedbackSent", lang)}", reply_markup=kb.main[lang])
            return await self.change_player(message, state, PlayerStates.main_menu_state)

        except asyncio.TimeoutError:
            lang = self.langs[message.from_user.id]
            msg = phrases.dict("errorTimeout", lang)
            await self.handle_error(message.from_user.id, Error(ErrorLevel.WARNING, msg))

        except Exception as e:
            msg = f"Failed to send feedback for user {player_id}"
            await self.handle_error(player_id, Error(ErrorLevel.ERROR, msg))

        return False
        
    async def change_lang(self, message: types.Message, state: FSMContext ):
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
                    await self.kafka.send_to_bd(player_data)
                    logger.info(f"Successfully sent player update lang for user_id: {player['player_id']}")
                except Exception as e:
                    msg = f"Failed to send player data to Kafka: {str(e)}"
                    await self.handle_error(message.from_user.id, Error(ErrorLevel.ERROR, msg))

                self.langs[message.from_user.id] = lang
                await message.answer(f"{phrases.dict("langChanged", lang)}", reply_markup=kb.main[lang])
                return await self.change_player(message, state, PlayerStates.main_menu_state)


