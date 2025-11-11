from datetime import datetime
from error import Error, ErrorLevel
from phrases import phrases
import asyncio
from typing import Any

import logging
logger = logging.getLogger(__name__)
MAX_RETRIES = 5
RETRY_DELAY = 60

class DBCommunicator:
    def __init__(self, _event_handler, _server_id):
        self.eh = _event_handler
        self.server_id = _server_id
        self.server_connected = False
        self.running = False

    async def start(self):
        self.running = True
        self.server_connected = False
        self.server_check_task = asyncio.create_task(self._check_server_availability())

    async def stop(self):
        await self.eh.kafka.stop()
        self.running = False
        self.server_connected = False
        if self.server_check_task:
            self.server_check_task.cancel()
            try:
                await self.server_check_task
            except asyncio.CancelledError:
                pass

    async def _check_server_availability(self):
        while self.running:
            try:
                attempts = 0
                while attempts < MAX_RETRIES:
                    if await self.ping():
                        break
                    else:
                        attempts += 1
                        logger.info("Database server is not responding")
                        await asyncio.sleep(RETRY_DELAY / 2)

                if attempts >= MAX_RETRIES:
                    self.server_connected = False
                    logger.error("Database server disconnected")
                else:
                    self.server_connected = True
                    logger.info("Database server is online")
                await asyncio.sleep(RETRY_DELAY)
            except Exception as e:
                msg = f"Failed to connect to database server: {str(e)}"
                logger.error(msg)
                await asyncio.sleep(RETRY_DELAY)

    def _handle_db_server_response(self, response, lang):
        if response is None:
            return [None, Error(ErrorLevel.ERROR, "Database response is none")]
        if "timeout" in response and response["timeout"]:
            msg = phrases.dict("errorTimeout", lang)
            return [None, Error(ErrorLevel.WARNING, msg)]
        if ("answer" not in response) or ("error" not in response) or ("data" not in response):
            return [None, Error(ErrorLevel.ERROR, "Wrong database response format")]
        if response["answer"] == "OK":
            return [response["data"], None]
        if response["answer"] == "Error":
            return [None, Error(ErrorLevel.ERROR, response["error"])]
        if response["answer"] == "Warning":
            return [None, Error(ErrorLevel.WARNING, response["error"])]
        if response["answer"] == "Critical":
            return [None, Error(ErrorLevel.CRITICAL, response["error"])]
        if response["answer"] == "Info":
            return [None, Error(ErrorLevel.INFO, response["error"])]

        return [None, Error(ErrorLevel.ERROR, response["error"])]

    def online(self):
        return self.running and self.server_connected

    async def ping(self):
        try:
            ping_msg = {
                "command": "ping",
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(ping_msg, timeout=10), "en")
            if db_response is None:
                return False
            return True
        except Exception as e:
            msg = f"Failed to ping db controller: {str(e)}"
            logger.error(msg)
            return False

    async def insert_player(self, player: Any):
        try:
            lang = self.eh.langs.get(player['player_id'], "en")
            player_data = {
                "command": "insert_player",
                "data": player,
                "timestamp": str(datetime.now())
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(player_data, timeout=5), lang)
            if db_response is None: return [False, error]
            logger.info(f"Successfully sent player insert for user_id: {player['player_id']}")
            return [True, None]

        except Exception as e:
            msg = f"Failed to send player data to Kafka: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def change_player(self, player: Any):
        try:
            lang = self.eh.langs.get(player['player_id'], "en")
            player_data = {
                "command": "change_player",
                "data": player,
                "timestamp": str(datetime.now())
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(player_data, timeout=5), lang)
            if db_response is None: return [False, error]
            logger.info(f"Successfully sent player update for user_id: {player['player_id']}")
            return [True, None]

        except Exception as e:
            msg = f"Failed to send player data to Kafka: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def update_lang_player(self, message, state, new_lang):
        try:
            player_id = message.from_user.id
            player = {
                "player_id": player_id,
                "firstname": message.from_user.first_name,
                "lastname": message.from_user.last_name,
                "fullname": message.from_user.full_name,
                "username": message.from_user.username,
                "lang": new_lang,
                "state": await state.get_state()
            }
            player_data = {
                "command": "update_lang_player",
                "data": player,
                "timestamp": str(datetime.now())
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(player_data, timeout=5), new_lang)
            if db_response is None: return [False, error]
            return [True, None]

        except Exception as e:
            msg = f"Failed to update lang for user {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def create_game(self, player_id, mode):
        lang = self.eh.langs.get(player_id, "en")
        try:
            create_msg = {
                "command": "create_game",
                "data": {
                    "id": 0,
                    "server_id": self.server_id,
                    "stage": "WAIT_A_NUMBER",
                    "step": 0,
                    "secret_value": 0,
                    "mode": mode,
                },
                "timestamp": str(datetime.now())
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(create_msg, timeout=5), lang)
            if db_response is None:
                return [None, error]
            if 'id' not in db_response:
                raise Exception("Invalid response from database: missing game ID")
            if 'table' not in db_response or db_response['table'] != 'games':
                raise Exception("Invalid response from database: wrong table")

            game_id = db_response['id']
            logger.info(f"Game created with ID: {game_id}")
            return [game_id, None]

        except Exception as e:
            msg = f"Failed to create game for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def update_game(self, game_id, player_id, game_stage, step, secret_value):
        lang = self.eh.langs.get(player_id, "en")
        try:
            start_game_message = {
                "command": "update_game",
                "data": {
                    "id": game_id,
                    "server_id": self.server_id,
                    "stage": game_stage,
                    "step": step,
                    "secret_value": secret_value,
                    "mode": "unchangeable",
                },
                "timestamp": str(datetime.now())
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(start_game_message, timeout=5), lang)
            if db_response is None:
                return [False, error]

            return [True, None]

        except Exception as e:
            msg = f"Failed to update game for user {player_id} with error: {e}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def create_computer(self, player_id, game_id, level):
        lang = self.eh.langs.get(player_id, "en")
        try:
            create_msg = {
                "command": "create_computer",
                "data": {
                    "computer_id": 0,
                    "player_id": player_id,
                    "server_id": self.server_id,
                    "game_id": game_id,
                    "game_brain": level,
                },
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(create_msg, timeout=5), lang)
            if db_response is None:
                return [False, error]
            if 'id' not in db_response:
                raise Exception("Invalid response from database: missing computer ID")
            if 'table' not in db_response or db_response['table'] != 'computers':
                raise Exception("Invalid response from database: wrong table")

            return [db_response['id'], None]

        except Exception as e:
            msg = f"Failed to create computer for user {player_id} with error: {e}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def add_player_game(self, user_id, player_id, game_id, is_host):
        lang = self.eh.langs.get(user_id, "en")
        try:
            add_player_msg = {
                "command": "add_player_game",
                "data": {
                    "player_id": player_id,
                    "server_id": self.server_id,
                    "game_id": game_id,
                    "is_current_game": True,
                    "is_host": is_host,
                },
                "timestamp": str(datetime.now())
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(add_player_msg, timeout=5), lang)
            if db_response is None: return [False, error]
            logger.info(f"Player {player_id} added to game {game_id}")
            return [True, None]

        except Exception as e:
            msg = f"Failed to add player to game in db for user {user_id} with error: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def feedback(self, player_id, username, feedback_message):
        lang = self.eh.langs.get(player_id, "en")
        try:
            feedback_msg = {
                "command": "feedback",
                "data": {
                    "username": username,
                    "message": feedback_message,
                },
                "timestamp": str(datetime.now())
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(feedback_msg, timeout=5), lang)
            if db_response is None: return [False, error]
            return [True, None]

        except Exception as e:
            msg = f"Failed to send feedback for user {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def get_current_game(self, player_id):
        lang = self.eh.langs.get(player_id, "en")
        try:
            get_current_game_msg = {
                    "command": "get_current_game",
                    "data": player_id,
                }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(get_current_game_msg, timeout=5), lang)
            if db_response is None:
                return [None, error]
            if 'id' not in db_response or 'finished' not in db_response:
                raise Exception("Invalid get current game response from game service")

            return [db_response, None]

        except Exception as e:
            msg = f"Failed to get current game for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def exit_from_game(self, player_id, game_id):
        lang = self.eh.langs.get(player_id, "en")
        try:
            exit_from_game_msg = {
                "command": "exit_from_game",
                "data": {
                    "player_id": player_id,
                    "server_id": self.server_id,
                    "game_id": game_id,
                    "is_current_game": False,
                    "is_host": False,
                },
                "timestamp": str(datetime.now())
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(exit_from_game_msg, timeout=5), lang)
            if db_response is None: return [False, error]
            return [True, None]

        except Exception as e:
            msg = f"Failed to exit from game for user {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def get_game_report(self, player_id, game_id):
        lang = self.eh.langs.get(player_id, "en")
        try:
            get_game_report_msg = {
                    "command": "get_game_report",
                    "data": game_id,
                }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(get_game_report_msg, timeout=5), lang)
            if db_response is None:
                return [None, error]
            if 'steps' not in db_response:
                raise Exception("Invalid get game report response from database")

            return [db_response['steps'], None]

        except Exception as e:
            msg = f"Failed to get game report for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def get_server_games(self):
        try:
            server_data = {
                "command": "get_server_games",
                "data": self.server_id,
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(server_data, timeout=10), "en")
            if db_response is None:
                raise Exception(error.Message())

            required_fields = ['games', 'players', 'player_games', 'computer_games', 'history']
            for field in required_fields:
                if field not in db_response:
                    raise Exception(f"Invalid response from database: missing {field}")

            return [db_response, None]

        except Exception as e:
            msg = f"Failed to get server games: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def add_step(self, player_id, game_id, step, game_value, bulls, cows, is_computer, is_give_up):
        lang = self.eh.langs.get(player_id, "en")
        try:
            step_message = {
                "command": "add_step",
                "data": {
                    "game_id": game_id,
                    "player_id": player_id,
                    "server_id": self.server_id,
                    "step": step,
                    "game_value": game_value,
                    "bulls": bulls,
                    "cows": cows,
                    "is_computer": is_computer,
                    "is_give_up": is_give_up,
                    "timestamp": datetime.now().isoformat(),
                },
                "timestamp": str(datetime.now())
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(step_message, timeout=5), lang)
            if db_response is None:
                return [False, error]
            return [True, None]

        except Exception as e:
            msg = f"Failed to add step in game {game_id}, for user {player_id} error - {e}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def get_game_names(self, player_id, game_id):
        lang = self.eh.langs.get(player_id, "en")
        try:
            create_msg = {
                "command": "get_game_names",
                "data": game_id,
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(create_msg, timeout=5), lang)
            if db_response is None:
                return [None, error]
            if 'names' not in db_response:
                raise Exception("Invalid response from database: wrong names")

            return [db_response['names'], None]

        except Exception as e:
            msg = f"Failed to get game names: {e}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def get_current_players(self, player_id, game_id):
        lang = self.eh.langs.get(player_id, "en")
        try:
            get_current_players_msg = {
                "command": "get_current_players",
                "data": game_id,
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(get_current_players_msg, timeout=5), lang)
            if db_response is None:
                return [None, error]
            if 'players' not in db_response:
                raise Exception("Invalid get current players response from database")

            return [db_response['players'], None]

        except Exception as e:
            msg = f"Failed to get current players in game {game_id}, for user {player_id} error - {e}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def create_lobby(self, player_id, password):
        lang = self.eh.langs.get(player_id, "en")
        try:
            lobby_data = {
                "command": "create_lobby",
                "data": {
                    "id": 0,
                    "server_id": self.server_id,
                    "host_id": player_id,
                    "game_id": 0,
                    "is_private": bool(password.strip()),
                    "password": password.strip(),
                    "status": "WAITING"
                },
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(lobby_data, timeout=5), lang)
            if db_response is None:
                return [None, error]
            if 'id' not in db_response:
                raise Exception("Invalid response from database: missing lobby ID")
            if 'table' not in db_response or db_response['table'] != 'lobbies':
                raise Exception("Invalid response from database: wrong table")

            lobby_id = db_response['id']
            logger.info(f"Lobby created with ID: {lobby_id}")

            return [lobby_id, None]

        except Exception as e:
            msg = f"Failed to create lobby for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def join_lobby(self, player_id, lobby_id, password):
        lang = self.eh.langs.get(player_id, "en")
        try:
            join_data = {
                "command": "join_lobby",
                "data": {
                    "lobby_id": lobby_id,
                    "player_id": player_id,
                    "password": password
                },
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(join_data, timeout=5), lang)
            if db_response is None:
                return [None, error]
            return [True, None]

        except Exception as e:
            msg = f"Failed to join lobby for user {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def leave_lobby(self, player_id, lobby_id):
        lang = self.eh.langs.get(player_id, "en")
        try:
            leave_msg = {
                "command": "leave_lobby",
                "data": {
                    "lobby_id": lobby_id,
                    "player_id": player_id
                },
                "timestamp": str(datetime.now())
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(leave_msg), lang)
            if db_response is None:
                return [None, error]
            if 'is_host' not in db_response:
                raise Exception("Invalid response from database: missing is_host field")
            logger.info(f"Player {player_id} left lobby {lobby_id}")

            return [db_response['is_host'], None]

        except Exception as e:
            msg = f"Failed to leave lobby for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def set_player_ready(self, player_id, lobby_id, is_ready):
        lang = self.eh.langs.get(player_id, "en")
        try:
            set_ready_msg = {
                "command": "set_player_ready",
                "data": {
                    "lobby_id": lobby_id,
                    "player_id": player_id,
                    "is_ready": is_ready
                },
                "timestamp": str(datetime.now())
            }
            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(set_ready_msg, timeout=5), lang)
            if db_response is None: return [False, error]
            return [True, None]

        except Exception as e:
            msg = f"Failed to set player ready for user {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def start_lobby_game(self, player_id, lobby_id, game_id):
        lang = self.eh.langs.get(player_id, "en")
        try:
            start_game_msg = {
                "command": "start_lobby_game",
                "data": {
                    "id": lobby_id,
                    "game_id": game_id,
                    "host_id": player_id
                },
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(start_game_msg, timeout=5), lang)
            if db_response is None:
                return [False, error]
            if 'id' not in db_response:
                raise Exception("Invalid response from database: missing game ID")

            game_id = db_response['id']
            logger.info(f"Player {player_id} started game {game_id} from lobby {lobby_id}")
            return [True, None]

        except Exception as e:
            msg = f"Failed to start lobby game for user {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def get_random_lobby_id(self, player_id):
        lang = self.eh.langs.get(player_id, "en")
        try:
            get_random_lobby_msg = {
                "command": "get_random_lobby_id",
                "data": {
                    "server_id": self.server_id,
                    "player_id": player_id,
                },
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(get_random_lobby_msg, timeout=5), lang)
            if db_response is None:
                return [None, error]
            if 'lobby_id' not in db_response:
                raise Exception("Invalid response from database: missing lobby ID")

            return [db_response['lobby_id'], None]

        except Exception as e:
            msg = f"Failed to get random lobby id for {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def get_lobby_id(self, player_id, isNecessary):
        lang = self.eh.langs.get(player_id, "en")
        try:
            get_lobby_msg = {
                "command": "get_lobby_id",
                "data": player_id,
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(get_lobby_msg, timeout=5), lang)
            if db_response is None:
                if error.Level() == ErrorLevel.WARNING and error.Message() == "DBAnswerPlayerNotInLobby":
                    if isNecessary:
                        raise Exception("Invalid response from database: Player is not in the lobby")
                    else:
                        return [{'lobby_id': 0, 'is_host': False}, None]
                return [None, error]
            if 'lobby_id' not in db_response:
                raise Exception("Invalid response from database: missing lobby ID")
            if 'is_host' not in db_response:
                raise Exception("Invalid response from database: missing host flag")
            return [db_response, None]

        except Exception as e:
            msg = f"Failed to get lobby id for {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def get_lobby_players(self, player_id, lobby_id):
        lang = self.eh.langs.get(player_id, "en")
        try:
            get_lobby_players_msg = {
                "command": "get_lobby_players",
                "data": lobby_id,
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(get_lobby_players_msg, timeout=5), lang)
            if db_response is None:
                return [None, error]
            if 'players' not in db_response:
                raise Exception("Invalid response from database: missing players")

            return [db_response['players'], None]

        except Exception as e:
            msg = f"Failed to get lobby players for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def get_lobby_names(self, player_id, lobby_id):
        lang = self.eh.langs.get(player_id, "en")
        try:
            get_lobby_names_msg = {
                "command": "get_lobby_names",
                "data": lobby_id,
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(get_lobby_names_msg, timeout=5), lang)
            if db_response is None:
                return [None, error]
            if 'names' not in db_response:
                raise Exception("Invalid response from database: missing names")

            return [db_response['names'], None]

        except Exception as e:
            msg = f"Failed to get lobby names for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def prepare_to_start_lobby(self, player_id, lobby_id):
        lang = self.eh.langs.get(player_id, "en")
        try:
            check_ready_msg = {
                "command": "prepare_to_start_lobby",
                "data": {
                    "lobby_id": lobby_id,
                    "player_id": player_id,
                },
                "timestamp": str(datetime.now())
            }
            [bd_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(check_ready_msg, timeout=5), lang)
            if bd_response is None:
                return [None, error]
            return [True, None]

        except Exception as e:
            msg = f"Failed to prepare lobby for {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def ban_player(self, lobby_id, host_id, player_id):
        try:
            lang = self.eh.langs.get(host_id, "en")
            ban_msg = {
                "command": "ban_player",
                "data": {
                    "lobby_id": lobby_id,
                    "host_id": host_id,
                    "player_id": player_id
                },
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(ban_msg, timeout=5), lang)
            if db_response is None:
                return [False, error]
            return [True, None]

        except Exception as e:
            msg = f"Failed to ban player for user {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def unban_player(self, lobby_id, host_id, player_id):
        lang = self.eh.langs.get(host_id, "en")
        try:
            remove_msg = {
                "command": "unban_player",
                "data": {
                    "lobby_id": lobby_id,
                    "host_id": host_id,
                    "player_id": player_id
                },
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(remove_msg, timeout=5), lang)
            if db_response is None:
                return [False, error]
            return [True, None]

        except Exception as e:
            msg = f"Failed to unban player {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def get_blacklist(self, lobby_id, host_id):
        lang = self.eh.langs.get(host_id, "en")
        try:
            get_msg = {
                "command": "get_blacklist",
                "data": {
                    "lobby_id": lobby_id,
                    "host_id": host_id,
                },
                "timestamp": str(datetime.now())
            }

            [db_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_db(get_msg, timeout=5), lang)
            if db_response is None:
                return [None, error]
            if 'blacklist' not in db_response:
                raise Exception("Invalid response from database: missing blacklist data")
            if 'banned_players' not in db_response:
                raise Exception("Invalid response from database: missing banned_players data")

            blacklist = []
            names = db_response['banned_players']
            for player in db_response['blacklist']:
                blacklist.append(player)
                blacklist[-1]['name'] = next((n['name'] for n in names if n['id'] == player['player_id']), "Unknown")

            return [blacklist, None]

        except Exception as e:
            msg = f"Failed to get blacklist: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]
