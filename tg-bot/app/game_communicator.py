from error import Error, ErrorLevel
from phrases import phrases
import asyncio

import logging
logger = logging.getLogger(__name__)
MAX_RETRIES = 5
RETRY_DELAY = 60

class GameCommunicator:
    def __init__(self, _event_handler, _server_id):
        self.eh = _event_handler
        self.server_id = _server_id
        self.running = False
        self.server_connected = False

    async def start(self):
        self.running = True
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
                    if self.server_connected:
                        if await self.ping():
                            break
                    else:
                        if await self.eh.initGameServer():
                            break
                    attempts += 1
                    logger.info("Game server is not responding")
                    await asyncio.sleep(RETRY_DELAY / 2)
                    
                if attempts >= MAX_RETRIES:
                    self.server_connected = False
                    logger.error("Game server disconnected")
                else:
                    self.server_connected = True
                    logger.info("Game server is online")
                await asyncio.sleep(RETRY_DELAY)
            except Exception as e:
                msg = f"Failed to connect to game server: {str(e)}"
                logger.error(msg)
                await asyncio.sleep(RETRY_DELAY)

    def _handle_db_server_response(self, response, lang):
        if response is None:
            return [None, Error(ErrorLevel.ERROR, "Game response is none")]
        if "timeout" in response and response["timeout"]:
            msg = phrases.dict("errorTimeout", lang)
            return [None, Error(ErrorLevel.WARNING, msg)]
        if ("result" not in response) or ("server_id" not in response):
            return [None, Error(ErrorLevel.ERROR, "Wrong database response format")]
        if response["server_id"] != self.server_id:
            return [None, Error(ErrorLevel.ERROR, "Wrong server id in response")]
        if response["result"] == 1:
            return [response, None]
        else:
            return [None, Error(ErrorLevel.ERROR, f"GameAnswerError{response['result']}")]
        
    def online(self):
        return self.running and self.server_connected

    async def init_game(self):
        try:
            init_server = {
                "command": 1,  # INIT_GAME command
                "server_id": self.server_id,
            }
            [game_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_game(init_server, timeout=5), "en")
            if game_response is None:
                return [False, error]
            return [True, None]

        except Exception as e:
            msg = f"Failed to initialize game server: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

    async def ping(self):
        try:
            init_server = {
                "command": 2,  # SERVER_INFO command
                "server_id": self.server_id,
            }
            [game_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_game(init_server, timeout=10), "en")
            if game_response is None:
                return False
            return True
        except Exception as e:
            return False

    async def create_game(self, player_id, game_id):
        try:
            lang = self.eh.langs.get(player_id, "en")
            game_msg = {
                "command": 4,  # Assuming 4 is CREATE_GAME command
                "server_id": self.server_id,
                "player_id": player_id,
                "game_id": game_id
            }
            [game_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_game(game_msg, timeout=5), lang)
            if game_response is None:
                return [False, error]
            return [True, None]
        except Exception as e:
            msg = f"Failed to create game in game server for user {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

    async def add_player(self, player_id, game_id):
        try:
            lang = self.eh.langs.get(player_id, "en")
            game_msg = {
                "command": 5,  # Assuming 5 is ADD_PLAYER command
                "server_id": self.server_id,
                "player_id": player_id,
                "game_id": game_id
            }
            [game_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_game(game_msg, timeout=5), lang)
            if game_response is None:
                return [False, error]
            return [True, None]
        except Exception as e:
            msg = f"Failed to add player in game server for user {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

    async def add_computer(self, player_id, game_id, computer_id, level):
        try:
            lang = self.eh.langs.get(player_id, "en")
            game_msg = {
                "command": 6,  # Assuming 6 is ADD_COMPUTER command
                "computer_id": computer_id,
                "player_id": player_id,
                "server_id": self.server_id,
                "game_id": game_id,
                "game_brain": level,
            }
            [game_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_game(game_msg, timeout=5), lang)
            if game_response is None:
                return [False, error]
            return [True, None]
        except Exception as e:
            msg = f"Failed to add computer in game server for user {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

    async def start_game(self, player_id, game_id):
        try:
            lang = self.eh.langs.get(player_id, "en")
            game_msg = {
                "command": 7,  # Assuming 7 is START_GAME command
                "server_id": self.server_id,
                "player_id": player_id,
                "game_id": game_id
            }
            [game_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_game(game_msg, timeout=5), lang)
            if game_response is None:
                return [None, error]
            return [game_response, None]
        except Exception as e:
            msg = f"Failed to start game in game server for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

    async def player_step(self, player_id, game_id, game_value):
        try:
            lang = self.eh.langs.get(player_id, "en")
            game_msg = {
                "command": 8,  # Assuming 8 is PLAYER_STEP command
                "server_id": self.server_id,
                "player_id": player_id,
                "game_id": game_id,
                "game_value": game_value,
            }
            [game_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_game(game_msg, timeout=5), lang)
            if game_response is None:
                return [None, error]

            if "game_stage" not in game_response:
                raise Exception(f"Game server does not have game stage")
            if "steps" not in game_response:
                raise Exception(f"Game server does not have player's steps")

            return [game_response, None]
        except Exception as e:
            msg = f"Failed to do step in game server for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

    async def player_give_up(self, player_id, game_id):
        try:
            lang = self.eh.langs.get(player_id, "en")
            game_msg = {
                "command": 11,  # Assuming 11 is PLAYER_GIVE_UP command
                "server_id": self.server_id,
                "player_id": player_id,
                "game_id": game_id
            }
            [game_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_game(game_msg, timeout=5), lang)
            if game_response is None:
                return [None, error]

            if "game_stage" not in game_response:
                raise Exception(f"Game server does not have game stage")
            if "steps" not in game_response:
                raise Exception(f"Game server does not have player's steps")

            return [game_response, None]
        except Exception as e:
            msg = f"Failed to give up in game server for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

    async def finish_game(self, player_id, game_id):
        try:
            lang = self.eh.langs.get(player_id, "en")
            game_msg = {
                "command": 15,  # Assuming 15 is FINISH_GAME command
                "server_id": self.server_id,
                "player_id": player_id,
                "game_id": game_id,
            }
            [game_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_game(game_msg, timeout=5), lang)
            if game_response is None:
                return [None, error]

            if "game_stage" not in game_response:
                raise Exception(f"Game server does not have game stage")
            if "steps" not in game_response:
                raise Exception(f"Game server does not have player's steps")

            return [game_response, None]
        except Exception as e:
            msg = f"Failed to finish game in game server for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

    async def game_result(self, player_id, game_id):
        try:
            lang = self.eh.langs.get(player_id, "en")
            game_msg = {
                "command": 16,  # Assuming 16 is GAME_RESULT command
                "server_id": self.server_id,
                "player_id": player_id,
                "game_id": game_id,
            }
            [game_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_game(game_msg, timeout=5), lang)
            if game_response is None:
                return [None, error]

            if "game_results" not in game_response:
                raise Exception(f"Game server does not have game results")

            return [game_response, None]
        except Exception as e:
            msg = f"Failed to get game result in game server for user {player_id}: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

    async def restore_game(self, game_dict):
        try:
            game_msg = {
                "command": 18,  # RESTORE_GAME command
                "server_id": self.server_id,
                "game_id": game_dict['game_id'],
                "game_value": game_dict['secret_value'],
                "restore_data": game_dict['restore_data'],
                "players": game_dict['players'],
            }
            [game_response, error] = self._handle_db_server_response(await self.eh.kafka.request_to_game(game_msg, timeout=5), "en")
            if game_response is None:
                return [False, error]

            return [True, None]
        except Exception as e:
            msg = f"Failed to restore game in game server for game {game_dict['game_id']}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]