from datetime import datetime
import asyncio
import os
from typing import Any
from aiogram import types
from kafka_handler import KafkaHandler
from database_communicator import DBCommunicator
from game_communicator import GameCommunicator
from phrases import phrases
import keyboards as kb
from aiogram.fsm.state import State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from player_state import PlayerStates, isFreeState
from aiogram import Dispatcher
from aiogram import Bot
from aiogram.types import ReplyKeyboardRemove
from error import Error, ErrorLevel
import sys
# Settings
SERVER_ID = 1


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
        self.db_communicator = DBCommunicator(self, SERVER_ID)
        self.game_communicator = GameCommunicator(self, SERVER_ID)

    async def start(self):
        await self.kafka.start()
        await self.db_communicator.start()
        await self.game_communicator.start()
        self.running = True

    async def stop(self):
        await self.kafka.stop()
        await self.db_communicator.stop()
        await self.game_communicator.stop()
        self.running = False
        if self.server_check_task:
            self.server_check_task.cancel()
            try:
                await self.server_check_task
            except asyncio.CancelledError:
                pass

    """private-------------------------------------------------------------------------------------------------"""

    async def _handle_server_available(self, user_id, lang):
        if not self.db_communicator.online():
            await self.bot.send_message(
                chat_id=user_id,
                text=phrases.dict("dbServerUnavailableTryLater", lang),
                reply_markup=kb.main[lang]
            )
            return False
        if not self.game_communicator.online():
            lang = self.langs.get(user_id, "en")
            await self.bot.send_message(
                chat_id=user_id,
                text=phrases.dict("gameServerUnavailableTryLater", lang),
                reply_markup=kb.main[lang]
            )
            return False
        return True

    async def _handle_error(self, user_id: int, error: Error):
        try:
            lang = self.langs.get(user_id, "en")
            if error.Level() == ErrorLevel.INFO:
                logger.info(error)
                return
            elif error.Level() == ErrorLevel.WARNING:
                logger.info(error)
                await self.bot.send_message(
                    chat_id=user_id,
                    text=str(error)
                )
                return
            elif error.Level() == ErrorLevel.ERROR:
                logger.error(error)
                await self.bot.send_message(
                    chat_id=user_id,
                    text=phrases.dict("errorMessage", lang),
                    reply_markup=kb.main[lang]
                )
                [ok, inner_error] = await self._change_player_state_by_id(user_id, PlayerStates.main_menu_state)
                if not ok: await self._handle_error(user_id, inner_error)
                return
            elif error.Level() == ErrorLevel.GAME_ERROR:
                logger.error(error)
                await self.bot.send_message(
                    chat_id=user_id,
                    text=phrases.dict("errorGame", lang)
                )
                if error.game_id != 0:
                    [ok, inner_error] = await self._give_up_in_game(user_id, error.game_id, True)
                    if not ok:
                        inner_error = Error(ErrorLevel.CRITICAL, inner_error.Message())
                        inner_error.game_id = None
                        await self._handle_error(user_id, inner_error)

                [ok, inner_error] = await self._change_player_state_by_id(user_id, PlayerStates.main_menu_state)
                if not ok: await self._handle_error(user_id, inner_error)

                await self.bot.send_message(
                    chat_id=user_id,
                    text=phrases.dict('menu', lang),
                    reply_markup=kb.main[lang]
                )
                return
            elif error.Level() == ErrorLevel.LOBBY_ERROR:
                logger.error(error)
                await self.bot.send_message(
                    chat_id=user_id,
                    text=phrases.dict("errorLobby", lang)
                )
                if error.lobby_id != 0:
                    [ok, inner_error] = await self._leave_lobby(user_id, error.lobby_id)
                    if not ok:
                        inner_error = Error(ErrorLevel.CRITICAL, inner_error.Message())
                        inner_error.lobby_id = None
                        await self._handle_error(user_id, inner_error)

                [ok, inner_error] = await self._change_player_state_by_id(user_id, PlayerStates.main_menu_state)
                if not ok: await self._handle_error(user_id, inner_error)
                await self.bot.send_message(
                    chat_id=user_id,
                    text=phrases.dict('menu', lang),
                    reply_markup=kb.main[lang]
                )
                return
            elif error.Level() == ErrorLevel.CRITICAL:
                logger.critical(error)
                await self.bot.send_message(
                    chat_id=user_id,
                    text=phrases.dict("errorMessage", lang)
                )

        except Exception as e:
            logger.error(f"Failed to handle error for user {user_id}: {str(e)}")

        if error.Level() == ErrorLevel.CRITICAL:
            sys.exit(1)

    async def _change_player_state_by_id(self, player_id, player_state : PlayerStates):
        storage_key = StorageKey(chat_id=player_id, user_id=player_id, bot_id=self.bot.id)
        state = FSMContext(storage=self.dp.fsm.storage, key=storage_key)
        await state.set_state(player_state)
        player = {
            "player_id": player_id,
            "state": await state.get_state()
        }
        return await self.db_communicator.change_player(player)

    async def _create_game(self, player_id, mode):
        lang = self.langs.get(player_id, "en")
        try:
            logger.info(f"Starting game creation for user_id: {player_id}")
            [game_id, error] = await self.db_communicator.create_game(player_id, mode)
            if game_id is None:
                return [None, error]

            [ok, error] = await self.db_communicator.add_player_game(player_id, player_id, game_id, True)
            if not ok: return [None, error]

            [ok, error] = await self.game_communicator.create_game(player_id, game_id)
            if not ok: return [None, error]

            return [game_id, None]

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            return [None, Error(ErrorLevel.WARNING, msg)]

        except Exception as e:
            msg = f"Failed to create game for user {player_id} with error: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def _add_player_to_game(self, game_id, player_id):
        [ok, error] = await self.db_communicator.add_player_game(player_id, player_id, game_id, False)
        if not ok: return [False, error]

        [ok, error] = await self.game_communicator.add_player(player_id, game_id)
        if not ok: return [False, error]

        return [True, None]

    async def _add_computer_to_game(self, game_id, player_id, level):
        [computer_id, error] = await self.db_communicator.create_computer(player_id, game_id, level)
        if computer_id is None:
            return [False, error]

        [ok, error] = await self.game_communicator.add_computer(player_id, game_id, computer_id, level)
        if not ok: return [False, error]

        return [True, None]

    async def _start_game(self, game_id, player_id):
        [game_response, error] = await self.game_communicator.start_game(player_id, game_id)
        if game_response is None: return [False, error]

        [ok, error] = await self.db_communicator.update_game(game_id, player_id, game_response["game_stage"], 0, game_response["secret_value"])
        if not ok: return [False, error]

        logger.info(f"Game {game_id} fully initialized")

        return [True, None]

    async def _give_up_in_game(self, player_id, game_id, force):
        lang = self.langs.get(player_id, "en")
        try:
            [game_response, error] = await self.game_communicator.player_give_up(player_id, game_id)
            if game_response is None: return [False, error]

            steps = game_response['steps']

            unfinished_players = 0
            player_i = next((i for i, step in enumerate(steps) if step['player'] and step['id'] == player_id), -1)
            for i in range(len(steps)):
                if i != player_i and steps[i]["player"] and not steps[i]["finished"]:
                    unfinished_players += 1

            if player_i == -1:
                raise Exception(f"Game server not have this player steps")

            [ok, error] = await self.db_communicator.add_step(player_id, game_id, 0, 0, 0, 0, False, True)
            if not ok: return [False, error]

            if game_response["game_stage"] != "IN_PROGRESS":
                [ok, error] = await self.db_communicator.update_game(game_id, player_id, game_response["game_stage"], steps[player_i]["step"], 0)
                if not ok: return [False, error]
            logger.info(f"Player {player_id} give up in game {game_id}")

            [names, error] = await self.db_communicator.get_game_names(player_id, game_id)
            if names is None:
                return [False, error]

            [players, error] = await self.db_communicator.get_current_players(player_id, game_id)
            if players is None:
                return [False, error]

            current_players = []
            player_in_game = False
            for cur_player in players:
                if cur_player['player_id'] == player_id:
                    player_in_game = True
                name = next((d for d in names if cur_player['player_id'] == d['id']), None)
                if name is not None and (not cur_player['give_up'] or name['id'] == player_id):
                    current_players.append(name)

            if player_in_game:
                [ok, error] = await self._send_give_up_to_other_players(steps[player_i], lang, current_players)
                if not ok: return [ok, error]

            await self.bot.send_message( chat_id=player_id, text=phrases.dict("youGaveUp", lang))

            if game_response['game_stage'] == 'FINISHED':
                [ok, error] = await self._finish_game(player_id, game_id, names, False)
                if not ok:
                    if error.Level() == ErrorLevel.GAME_ERROR:
                        error = Error(ErrorLevel.ERROR, error.Message())
                    return [ok, error]
            elif steps[player_i]['finished'] and unfinished_players == 0:
                [ok, error] = await self._finish_game(player_id, game_id, names, True)
                if not ok:
                    if error.Level() == ErrorLevel.GAME_ERROR:
                        error = Error(ErrorLevel.ERROR, error.Message())
                    return [ok, error]
            else:
                if force:
                    [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.main_menu_state)
                    if not ok: return [ok, error]
                else:
                    [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.choose_exit_game_after_give_up)
                    if not ok: return [ok, error]
                    await self.bot.send_message( chat_id=player_id, text=phrases.dict("stayInGame", lang), reply_markup=kb.exit_or_not[lang] )

            return [True, None]

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]
        except Exception as e:
            msg = f"Failed to give up in game for user {player_id} error - {e}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def _finish_game(self, player_id, game_id, names, force):
        lang = self.langs.get(player_id, "en")
        try:
            if force:
                [game_response, error] = await self.game_communicator.finish_game(player_id, game_id)
                if game_response is None: return [False, error]

                steps = game_response['steps']
                [ok, error] = await self._save_computer_steps(steps, player_id, game_id)
                if not ok: return [ok, error]

                max_step = max(step['step'] for step in steps)
                [ok, error] = await self.db_communicator.update_game(game_id, player_id, game_response["game_stage"], max_step, 0)
                if not ok: return [False, error]

                logger.info(f"Game {game_id} force finish")

            return await self._send_game_results(player_id, game_id, lang, names)

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]

        except Exception as e:
            msg = f"Failed to finish game in game for user {player_id} with error - {e}"
            error = Error(ErrorLevel.GAME_ERROR, msg)
            error.setGameId(game_id)
            return [False, error]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def _send_game_results(self, player_id, game_id, lang, names):
        try:
            [players, error] = await self.db_communicator.get_current_players(player_id, game_id)
            if players is None:
                return [False, error]

            current_player_ids = [d['player_id'] for d in players]

            [game_response, error] = await self.game_communicator.game_result(player_id, game_id)
            if game_response is None: return [False, error]

            game_results = game_response['game_results']
            player_i = next((i for i, step in enumerate(game_results) if step['player'] and step['id'] == player_id), -1)
            if player_i == -1:
                raise Exception(f"Game server not have this player for game result")

            [db_response, error] = await self.db_communicator.get_lobby_id(player_id, False)
            if db_response is None: return [False, error]
            lobby_id = db_response['lobby_id']
            for i in range(len(game_results)):
                if game_results[i]['player'] and game_results[i]['id'] in current_player_ids:
                    [ok, result] = self._generate_game_results(game_results, i, lang, names)
                    if not ok: return [ok, Error(ErrorLevel.ERROR, result)]
                    if lobby_id == 0:
                        await self.bot.send_message( chat_id=game_results[i]['id'], text=result, reply_markup=kb.full_game_menu[lang] )
                    else:
                        await self.bot.send_message( chat_id=game_results[i]['id'], text=result, reply_markup=kb.full_game_menu_with_lobby[lang] )
            return [True, None]

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]
        except Exception as e:
            msg = f"Error in sending game results - {str(e)}"
            error = Error(ErrorLevel.GAME_ERROR, msg)
            error.setGameId(game_id)
            return [False, error]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def _send_players_states(self, player_id, lobby_id, lobby_players, lobby_names):
        try:
            active_lobby_players = [player for player in lobby_players if player.get('in_lobby', False)]

            for player in active_lobby_players:
                player_lang = self.langs.get(player['player_id'], "en")

                state_msg = f"{phrases.dict('youInLobby', player_lang)}\n"
                state_msg += f"{phrases.dict('lobbyId', player_lang)} - {lobby_id}\n"
                state_msg += f"{phrases.dict('playersState', player_lang)}:\n"

                for p in active_lobby_players:
                    if p['player_id'] == player['player_id']:
                        name = phrases.dict('you', player_lang)
                    else:
                        name = next((n['name'] for n in lobby_names if n['id'] == p['player_id']), phrases.dict('player', player_lang))

                    ready_status = phrases.dict('playerIsReady', player_lang) if p['is_ready'] else phrases.dict('playerIsNotReady', player_lang)
                    host_indicator = " (Host)" if p['host'] else ""

                    state_msg += f"• {name}{host_indicator} - {ready_status}\n"

                is_host = player['host']
                is_ready = player['is_ready']

                await self.bot.send_message(
                    chat_id=player['player_id'],
                    text=state_msg,
                    reply_markup=kb.get_lobby_keyboard(is_host, is_ready, player_lang)
                )
            return [True, None]

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]
        except Exception as e:
            msg = f"Failed to send players states for user {player_id}: {str(e)}"
            error = Error(ErrorLevel.LOBBY_ERROR, msg)
            error.setLobbyId(lobby_id)
            return [False, error]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def _send_player_leave_lobby(self, leaving_player_id, is_host, lobby_id, lobby_players, lobby_names):
        try:
            leaving_player_name = "Player"
            new_host_name = "Player"
            if lobby_names:
                leaving_player_name = next((n['name'] for n in lobby_names if n['id'] == leaving_player_id), "Player")
            if is_host:
                for player in lobby_players:
                    if player['host']:
                        new_host_name = next((n['name'] for n in lobby_names if n['id'] == player['player_id']), "Player")
                        break

            for player in lobby_players:
                if player['in_lobby'] and player['player_id'] != leaving_player_id:

                    player_lang = self.langs.get(player['player_id'], "en")
                    leave_msg = f"{leaving_player_name} {phrases.dict('playerLeftLobby', player_lang)}"
                    if is_host:
                        leave_msg += f"\n{phrases.dict('hostLeftLobbyHost', player_lang)}.{phrases.dict('newHost', player_lang)} - {new_host_name}"
                    await self.bot.send_message(
                        chat_id=player['player_id'],
                        text=leave_msg,
                        reply_markup=kb.get_lobby_keyboard(player['host'], player['is_ready'], player_lang)
                    )

            [ok, error] = await self._send_players_states(leaving_player_id, lobby_id, lobby_players, lobby_names)
            if not ok: return [ok, error]

            return [True, None]

        except asyncio.TimeoutError:
            lang = self.langs.get(leaving_player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]

        except Exception as e:
            msg = f"Failed to send leave notification for user {leaving_player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def _remove_waiting_player_after_timeout(self, player_id: int, lang: str):
        await asyncio.sleep(60)

        if self.waiting_player and self.waiting_player["player_id"] == player_id:
            try:
                self.waiting_player = None
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict('waitingTimeout', lang),
                    reply_markup=kb.main[lang]
                )

                [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.main_menu_state)
                if not ok: await self._handle_error(player_id, error)

            except Exception as e:
                logging.error(f"Error notifying player about timeout: {e}")


    async def _start_random_game(self, player1, player2):
        try:
            [game_id, error] = await self._create_game(player1['player_id'], "random")
            if game_id is None: return [False, error]

            [ok, error] = await self._add_player_to_game(game_id, player2['player_id'])
            if not ok: return [ok, error]

            [ok, error] = await self._start_game(game_id, player1['player_id'])
            if not ok: return [ok, error]

            for player in [player1, player2]:
                [ok, error] = await self._change_player_state_by_id(player['player_id'], PlayerStates.waiting_for_number)
                if not ok:
                    if error.Level() == ErrorLevel.INFO or error.Level() == ErrorLevel.WARNING:
                        continue
                    else:
                        return [ok, error]

                lang = self.langs.get(player['player_id'], "en")
                await self.bot.send_message(
                    chat_id=player['player_id'],
                    text=f"{phrases.dict('gameCreated', lang)}\n{phrases.dict('yourTurn', lang)}\n{phrases.dict('giveUpInfo', lang)}",
                    reply_markup=ReplyKeyboardRemove()
                )

            return [True, None]

        except asyncio.TimeoutError:
            lang = self.langs.get(player1['player_id'], "en")
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]
        except Exception as e:
            msg = f"Failed to start random game: {e}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    def _generate_step_result(self, player_step, lang):
        try:
            result = ""
            game_value = player_step["game_value"]
            result += f"{phrases.dict('step', lang)} {player_step['step']}"
            result += f"\n{game_value:04}: {player_step['bulls']}{phrases.dict('bulls', lang)} {player_step['cows']}{phrases.dict('cows', lang)}"
            return [True, result]
        except Exception as e:
            msg = f"Failed to generate step results: {e}"
            return [False, msg]

    def _generate_steps_result(self, steps, player_i, lang, names, step_number):
        try:
            result = ""
            player_step = steps[player_i]
            result += f"{phrases.dict('step', lang)} {step_number}"
            cur_steps = [d for d in steps if (d['step'] == step_number and d['id'] != player_step['id'])]
            if len(cur_steps) > 0:
                if step_number == player_step['step'] and not player_step['give_up']:
                    result += f"\n{phrases.dict('you', lang)} -  {player_step["game_value"]:04}: {player_step['bulls']}{phrases.dict('bulls', lang)} {player_step['cows']}{phrases.dict('cows', lang)}"
                for i in range(len(cur_steps)):
                    name = next((d['name'] for d in names if d['id'] == cur_steps[i]['id'] and d['is_player'] == cur_steps[i]['player']), "Unknown")
                    if cur_steps[i]["player"]:
                        result += f"\n{phrases.dict('player', lang)} {name}"
                    else:
                        result += f"\n{phrases.dict('bot', lang)} {name}"
                    result += f" - {'*'*4}: {cur_steps[i]['bulls']}{phrases.dict('bulls', lang)} {cur_steps[i]['cows']}{phrases.dict('cows', lang)}"
            else:
                result += f"\n{player_step["game_value"]:04}: {player_step['bulls']}{phrases.dict('bulls', lang)} {player_step['cows']}{phrases.dict('cows', lang)}"
            return [True, result]

        except Exception as e:
            msg = f"Failed to generate steps result: {str(e)}"
            return [False, msg]

    async def _send_give_up_to_other_players(self, player_step, lang, names):
        try:
            name = next((d['name'] for d in names if d['id'] == player_step['id'] and d['is_player']), "Unknown")
            result = f"{phrases.dict('player', lang)} {name} {phrases.dict('giveUp', lang)}"
            for name in names:
                    if name['id'] != player_step['id'] and name['is_player']:
                        await self.bot.send_message( chat_id=name['id'], text=result )
            return [True, None]

        except asyncio.TimeoutError:
            lang = self.langs.get(player_step['id'], "en")
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]
        except Exception as e:
            msg = f"Failed to send give up to other players about player - {player_step['id']}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

    async def _send_step_to_other_players(self, player_step, lang, names):
        try:
            player_name = next((d['name'] for d in names if d['id'] == player_step['id'] and d['is_player']), "Unknown")
            result = f"{phrases.dict('player', lang)} {player_name} {phrases.dict('makeStep', lang)} {player_step['step']}"
            for name in names:
                    if name['id'] != player_step['id'] and name['is_player']:
                        await self.bot.send_message( chat_id=name['id'], text=result )
            return [True, None]

        except asyncio.TimeoutError:
            lang = self.langs.get(player_step['id'], "en")
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]
        except Exception as e:
            msg = f"Failed to send step to other players: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]


    def _generate_game_report(self, game_steps, lang, names):
        try:
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

            return [True, report]

        except Exception as e:
            msg = f"Invalid game report: {str(e)}"
            return [False, msg]


    def _generate_game_results(self, game_results, player_i, lang, names):
        try:
            result = f"{phrases.dict('gameFinished', lang)}"
            if len(game_results) == 2 and game_results[0]['place'] == game_results[1]['place']:
                return [True, f"{result} {phrases.dict('draw', lang)}"]
            elif game_results[player_i]['place'] == 1:
                if game_results[player_i]['give_up']:
                    result += f" {phrases.dict('gaveUp', lang)}"
                else:
                    result += f" {phrases.dict('youWon', lang)}"
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
                    result += f'{sorted_results[i]["place"]}. {name} - {sorted_results[i]["step"]} {phrases.dict("steps", lang)}'
                    if sorted_results[i]['give_up']:
                        result += f" {phrases.dict('gaveUp', lang)}\n"
                    else:
                        result += f"\n"
            return [True, result]

        except Exception as e:
            msg = f"Invalid game results: {str(e)}"
            return [False, msg]


    async def _save_computer_steps(self, steps, player_id, game_id):
        try:
            for i in range(len(steps)):
                if steps[i]["player"]:
                    continue
                [ok, error] = await self.db_communicator.add_step(
                    steps[i]["id"],
                    game_id,
                    steps[i]["step"],
                    steps[i]["game_value"],
                    steps[i]["bulls"],
                    steps[i]["cows"],
                    not steps[i]["player"],
                    False
                    )
                if not ok: return [False, error]
                logger.info(f"Computer {steps[i]['id']} do step in game {game_id}")

            return [True, None]

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]
        except Exception as e:
            msg = f"Failed to save computer steps: {str(e)}"
            error = Error(ErrorLevel.GAME_ERROR, msg)
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def _enter_by_lobby_id(self, player_id, lobby_id):
        lang = self.langs.get(player_id, "en")
        try:
            [ok, error] = await self.db_communicator.join_lobby(player_id, lobby_id, "")
            if not ok:
                if error.Level() != ErrorLevel.WARNING:
                    return [False, error]
                error_msg = error.Message()

                if error_msg == "DBAnswerAlreadyInLobby":
                    return [False, Error(ErrorLevel.ERROR, f"Error! Player already in Lobby with lobby_id - {lobby_id}")]

                if error_msg == "DBAnswerPasswordNeeded":

                    [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.enter_password)
                    if not ok: return [ok, error]

                    await self.bot.send_message(
                        chat_id=player_id,
                        text=f"{phrases.dict('enterLobbyPassword', lang)} {phrases.dict('useOnlyDigits', lang)}",
                        reply_markup=ReplyKeyboardRemove()
                    )
                    return [True, None]

                [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.choose_lobby_type)
                if not ok: return [ok, error]

                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict(error_msg, lang),
                    reply_markup=kb.lobby_types[lang]
                )
                return [True, None]

            [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.in_lobby)
            if not ok: return [ok, error]
            await self.bot.send_message(
                chat_id=player_id,
                text=phrases.dict("youEnteredLobby", lang),
                reply_markup=kb.get_lobby_keyboard(False, False, lang)
            )

            [lobby_players, error] = await self.db_communicator.get_lobby_players(player_id, lobby_id)
            if lobby_players is None: return [False, error]

            [lobby_names, error] = await self.db_communicator.get_lobby_names(player_id, lobby_id)
            if lobby_names is None: return [False, error]

            [ok, error] = await self._send_players_states(player_id, lobby_id, lobby_players, lobby_names)
            if not ok: return [ok, error]

            return [True, None]

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]

        except Exception as e:
            msg = f"Failed to enter lobby for user {player_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

        return [False, Error(ErrorLevel.ERROR, "Unexpected end of function")]

    async def _start_game_with_players(self, lobby_id, lobby_players):
        try:
            host_player = next((p for p in lobby_players if p['host']), None)
            if not host_player:
                raise Exception(f"No host found in lobby {lobby_id}")

            host_id = host_player['player_id']
            logger.info(f"Creating game with host {host_id} for lobby {lobby_id}")

            [game_id, error] = await self._create_game(host_id, "lobby")
            if game_id is None: return [None, error]

            for player in lobby_players:
                if player['player_id'] != host_id:
                    [ok, error] = await self._add_player_to_game(game_id, player['player_id'])
                    if not ok: return [None, error]
                    logger.info(f"Added player {player['player_id']} to game {game_id}")

            logger.info(f"Successfully created game {game_id} with {len(lobby_players)} players")
            return [game_id, None]

        except Exception as e:
            msg = f"Failed to start game with players: {str(e)}"
            return [None, Error(ErrorLevel.ERROR, msg)]

        return [None, Error(ErrorLevel.ERROR, "Unexpected end of function")]


    async def _leave_lobby(self, player_id, lobby_id):
        try:
            [lobby_names, error] = await self.db_communicator.get_lobby_names(player_id, lobby_id)
            if lobby_names is None:
                raise Exception(error.Message())

            [is_host, error] = await self.db_communicator.leave_lobby(player_id, lobby_id)
            if is_host is None:
                raise Exception(error.Message())

            [lobby_players, error] = await self.db_communicator.get_lobby_players(player_id, lobby_id)
            if lobby_players is None:
                raise Exception(error.Message())

            [ok, error] = await self._send_player_leave_lobby(player_id, is_host, lobby_id, lobby_players, lobby_names)
            if not ok:
                raise Exception(error.Message())

            [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.choose_game)
            if not ok: return [ok, error]

            return [True, None]

        except asyncio.TimeoutError:
            lang = self.langs.get(player_id, "en")
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]

        except Exception as e:
            msg = f"Failed to leave lobby for user {player_id}: {str(e)}"
            error = Error(ErrorLevel.LOBBY_ERROR, msg)
            error.setLobbyId(lobby_id)
            return [False, error]

    async def _send_other_ban_player(self, host_id, player_id, lobby_names, lobby_players):
        try:
            banned_player_name = next((n['name'] for n in lobby_names if n['id'] == player_id), "Unknown")

            for player in lobby_players:
                player_lang = self.langs.get(player['player_id'], "en")
                if player['player_id'] == player_id:
                    await self.bot.send_message(
                        chat_id=player['player_id'],
                        text=phrases.dict("youHaveBeenBanned", player_lang),
                        reply_markup=kb.main[player_lang]
                    )
                else:
                    await self.bot.send_message(
                        chat_id=player['player_id'],
                        text=f"{banned_player_name} {phrases.dict('hasBeenBanned', player_lang)}",
                        reply_markup=kb.get_lobby_keyboard(player['host'], player['is_ready'], player_lang)
                    )

            return [True, None]

        except Exception as e:
            msg = f"Failed to send other ban player for user {host_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

    async def _send_other_unban_player(self, host_id, player_id, unbanned_name, lobby_id, lobby_names, lobby_players):
        lang = self.langs.get(host_id, "en")
        try:
            unban_player_lang = self.langs.get(player_id, "en")
            await self.bot.send_message(
                chat_id=player_id,
                text=f"{phrases.dict("youHaveBeenUnbanned", unban_player_lang)} {lobby_id}"
            )
            for player in lobby_players:
                if player['player_id'] != player_id:
                    player_lang = self.langs.get(player['player_id'], "en")
                    await self.bot.send_message(
                        chat_id=player['player_id'],
                        text=f"{unbanned_name} {phrases.dict('hasBeenUnbanned', player_lang)}",
                        reply_markup=kb.get_lobby_keyboard(player['host'], player['is_ready'], player_lang)
                    )

            return [True, None]

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            return [False, Error(ErrorLevel.WARNING, msg)]
        except Exception as e:
            msg = f"Failed to send other unban player {host_id}: {str(e)}"
            return [False, Error(ErrorLevel.ERROR, msg)]

    """public--------------------------------------------------------------------------------------------------"""
    async def initGameServer(self):
        try:
            server_up = False

            [ok, error] = await self.game_communicator.init_game()
            if not ok:
                if error.Message() == "GameAnswerError4": # SERVER_ALREADY_REGESTERED
                    server_up = True
                else:
                    raise Exception(error.Message())

            [games_data, error] = await self.db_communicator.get_server_games()
            if games_data is None:
                raise Exception(error.Message())

            for player in games_data['players']:
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
            for game in games_data['games']:
                game_dict = {
                    "game_id": game['id'],
                    "secret_value": game['secret_value'],
                    "restore_data": [],
                    "players": [],
                }
                games.append(game_dict)

            for player_game in games_data['player_games']:
                for game in games:
                    if player_game['game_id'] == game['game_id']:
                        game['players'].append({
                            "id": player_game['player_id'],
                            "is_host": player_game['is_host'],
                        })
                        break

            for history in games_data['history']:
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

            for computer in games_data['computer_games']:
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
                [ok, error] = await self.game_communicator.restore_game(game)
                if not ok:
                    raise Exception(f"Game server error: {error.Message()}")
            return True

        except Exception as e:
            logger.exception(f"Failed to init game server: {str(e)}")
            return False


    async def new_player_start(self, message: Any, state: FSMContext):
        player_id = message.from_user.id
        lang = message.from_user.language_code
        if player_id in self.langs:
            lang = self.langs[player_id]
        if not await self._handle_server_available(player_id, lang):
            return False
        if player_id not in self.langs:
            self.langs[player_id] = message.from_user.language_code
        lang = self.langs[player_id]
        cur_state = await state.get_state()
        if cur_state is not None and not isFreeState(cur_state):
            await message.answer(phrases.dict("notFreeState", lang))
            return False
        await state.set_state(PlayerStates.main_menu_state)
        self.langs[player_id] = lang
        player = {
            "player_id": player_id,
            "firstname": message.from_user.first_name,
            "lastname": message.from_user.last_name,
            "fullname": message.from_user.full_name,
            "username": message.from_user.username,
            "lang": lang,
            "state": await state.get_state()
        }
        [ok, error] = await self.db_communicator.insert_player(player)
        if not ok:
            await self._handle_error(player_id, error)
            return False

        if type(message) == types.Message:
            await message.answer(phrases.dict("greeting", lang), reply_markup=kb.main[lang])
        else:
            await self.bot.send_message( chat_id=player_id, text=phrases.dict("greeting", lang), reply_markup=kb.main[lang] )
        return True

    async def help_player(self, message: Any, state: FSMContext):
        player_id = message.from_user.id
        if player_id not in self.langs:
            self.langs[player_id] = message.from_user.language_code
        lang = self.langs[player_id]
        if not await self._handle_server_available(player_id, lang):
            return False

        cur_state = await state.get_state()
        if cur_state is not None and not isFreeState(cur_state):
            await message.answer(phrases.dict("notFreeState", lang))
            return False

        ok = await self.change_player(message, state, PlayerStates.main_menu_state)
        if not ok:
            return False
        await message.answer(
            phrases.dict("help",lang),
            reply_markup=kb.main[lang])
        return True


    async def change_player(self, callback: types.CallbackQuery, state: FSMContext, new_state: State):
        player_id = callback.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        await state.set_state(new_state)
        player = {
            "player_id": player_id,
            "firstname": callback.from_user.first_name,
            "lastname": callback.from_user.last_name,
            "fullname": callback.from_user.full_name,
            "username": callback.from_user.username,
            "lang": lang,
            "state": await state.get_state()
        }
        [ok, error] = await self.db_communicator.change_player(player)
        if not ok:
            await self._handle_error(player_id, error)
            return False
        return True


    async def change_player(self, message: types.Message, state: FSMContext, new_state: State):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        await state.set_state(new_state)
        player = {
            "player_id": player_id,
            "firstname": message.from_user.first_name,
            "lastname": message.from_user.last_name,
            "fullname": message.from_user.full_name,
            "username": message.from_user.username,
            "lang": lang,
            "state": await state.get_state()
        }
        [ok, error] = await self.db_communicator.change_player(player)
        if not ok:
            await self._handle_error(player_id, error)
            return False
        return True


    async def start_single_game(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        [game_id, error] = await self._create_game(player_id, "single")
        if game_id is None:
            await self._handle_error(player_id, error)
            return False
        logger.info(f"Starting single game for user_id: {player_id}")
        [ok, error] = await self._start_game(game_id, player_id)
        if ok:
            await message.answer(f"{phrases.dict('gameCreated', lang)} {phrases.dict('yourTurn', lang)}\n{phrases.dict('giveUpInfo', lang)}", reply_markup=ReplyKeyboardRemove())
            await self.change_player(message, state, PlayerStates.waiting_for_number)
        else:
            await self._handle_error(player_id, error)
            return False
        return True


    async def start_random_game(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        player = {
            "player_id": player_id,
            "firstname": message.from_user.first_name,
            "lastname": message.from_user.last_name,
            "fullname": message.from_user.full_name,
            "username": message.from_user.username,
            "lang": self.langs[player_id],
            "state": await state.get_state()
        }
        if self.waiting_player is None or self.waiting_player["player_id"] == player_id:
            ok = await self.change_player(message, state, PlayerStates.waiting_a_rival)
            if not ok: return
            player['state'] = await state.get_state()
            self.waiting_player = player
            asyncio.create_task(self._remove_waiting_player_after_timeout(player_id, lang))
            await message.answer(f"{phrases.dict('waitingForOpponent', lang)}", reply_markup=ReplyKeyboardRemove())
        else:
            _waiting_player = self.waiting_player
            self.waiting_player = None
            [ok, error] = await self._start_random_game(_waiting_player, player)
            if not ok:
                await self._handle_error(player_id, error)
                return False
        return True


    async def do_step(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        game_id = None
        steps = None
        try:
            if not message.text.isdigit():
                msg = phrases.dict("invalidNumberFormat", lang)
                await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
                return False

            [game_info, error] = await self.db_communicator.get_current_game(player_id)
            if game_info is None:
                await self._handle_error(player_id, error)
                return False
            game_id = game_info['id']

            game_value = int(message.text)
            [game_response, error] = await self.game_communicator.player_step(player_id, game_id, game_value)

            if game_response is None:  # Assuming 1 is SUCCESS

                if error.Message() == "GameAnswerError13":  # Assuming 13 is INVALID_GAME_VALUE
                    msg = phrases.dict("errorInvalidGameValue", lang)
                    await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
                    return False

                if error.Message() == "GameAnswerError20":  # Assuming 20 is ERROR_PLAYER_ALREADY_MADE_STEP
                    msg = phrases.dict("errorNotAllPlayersMadeStep", lang)
                    await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
                    return False

                msg = f"Game server error: {error.Message()}"
                await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
                return False

            if "steps" not in game_response:
                raise Exception(f"Game server not have this player steps")

            steps = game_response['steps']
            player_i = next((i for i, step in enumerate(steps) if step['player'] and step['id'] == player_id), -1)
            if player_i == -1:
                raise Exception(f"Game server not have this player steps")

            step_number = steps[player_i]["step"]
            [ok, error] = await self.db_communicator.add_step(
                player_id,
                game_id,
                step_number,
                steps[player_i]["game_value"],
                steps[player_i]["bulls"],
                steps[player_i]["cows"],
                not steps[player_i]["player"],
                False
                )
            if not ok: return [False, error]
            logger.info(f"Player {player_id} do step in game {game_id}")
            computer_steps = [step for step in steps if (not step["player"] and step["step"] == step_number)]
            [ok, error] = await self._save_computer_steps(computer_steps, player_id, game_id)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            [ok, error] = await self.db_communicator.update_game(game_id, player_id, game_response["game_stage"], step_number, 0)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            [names, error] = await self.db_communicator.get_game_names(player_id, game_id)
            if names is None:
                await self._handle_error(player_id, error)
                return False

            unfinished_players = 0
            unstepped_players = 0
            if 'unstepped_players' in game_response:
                unstepped_players = game_response["unstepped_players"]

            for i in range(len(steps)):
                if i != player_i and steps[i]["player"] and not steps[i]["finished"]:
                    unfinished_players += 1

            [players, error] = await self.db_communicator.get_current_players(player_id, game_id)
            if players is None:
                await self._handle_error(player_id, error)
                return False

            current_players = []
            player_in_game = False
            for name in names:
                if name['id'] == player_id:
                    player_in_game = True
                is_cur_player = next((True for d in players if d['player_id'] == name['id']), False)
                if not name['is_player'] or is_cur_player:
                    current_players.append(name)

            if player_in_game:
                [ok, error] = await self._send_step_to_other_players(steps[player_i], lang, current_players)
                if not ok:
                    await self._handle_error(player_id, error)
                    return False


            if unstepped_players > 0:
                [ok, result] = self._generate_step_result(steps[player_i], lang)
                if not ok: raise Exception(result)
                result += f"\n{unstepped_players} {phrases.dict('waitPlayers', lang)}"
                await message.answer(result, reply_markup=ReplyKeyboardRemove())
            else:
                for i in range(len(steps)):
                    is_cur_player = next((True for d in current_players if d['id'] == steps[i]['id']), False)
                    if steps[i]["player"] and is_cur_player:
                        [ok, result] = self._generate_steps_result(steps, i, lang, current_players, step_number)
                        if not ok:
                            await self._handle_error(steps[i]['id'], Error(ErrorLevel.WARNING, result))
                        await self.bot.send_message( chat_id=steps[i]['id'], text=result, reply_markup=ReplyKeyboardRemove() )


            if game_response['game_stage'] == 'FINISHED':
                [ok, error] = await self._finish_game(player_id, game_id, names, False)
                if not ok:
                    await self._handle_error(player_id, error)
                    return False
            elif steps[player_i]['finished'] and unfinished_players == 0:
                [ok, error] = await self._finish_game(player_id, game_id, names, True)
                if not ok:
                    await self._handle_error(player_id, error)
                    return False
            elif steps[player_i]['finished']:
                await self.change_player(message, state, PlayerStates.waiting_game_end)
                await message.answer(phrases.dict("youveFinished", lang), reply_markup=ReplyKeyboardRemove())

            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to do step for player - {player_id}: {str(e)}"
            if game_id != 0:
                error = Error(ErrorLevel.GAME_ERROR, msg)
                if steps != None and player_i != -1 and ("finished" in steps[player_i]) and not steps[player_i]["finished"]:
                    error.setGameId(game_id)
                await self._handle_error(player_id, error)
            else:
                error = Error(ErrorLevel.ERROR, msg)
                await self._handle_error(player_id, error)
            return False


    async def start_bot_play( self, message: types.Message, state: FSMContext ):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        [game_id, error] = await self._create_game(player_id, "versus_bot")
        if game_id is None:
            await self._handle_error(player_id, error)
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
                msg = phrases.dict("invalidBotLevel", lang)
                await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
                return False

            logger.info(f"Starting game for user_id: {player_id}")

            [ok, error] = await self._add_computer_to_game(game_id, player_id, level)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            logger.info(f"Starting single game for user_id: {player_id}")
            [ok, error] = await self._start_game(game_id, player_id)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            await message.answer(f"{phrases.dict('gameCreated', lang)} {phrases.dict('yourTurn', lang)}\n{phrases.dict('giveUpInfo', lang)}", reply_markup=ReplyKeyboardRemove())
            await self.change_player(message, state, PlayerStates.waiting_for_number)
            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to create game for user {player_id}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False

    async def send_feedback(self, message: types.Message, state: FSMContext ):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            username = message.from_user.username
            feedback_message = message.text
            [ok, error] = await self.db_communicator.feedback(player_id, username, feedback_message)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            logger.info(f"Player {player_id} send feedback")

            await message.answer(f"{phrases.dict('feedbackSent', lang)}", reply_markup=kb.main[lang])
            await self.change_player(message, state, PlayerStates.main_menu_state)
            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(message.from_user.id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to send feedback for user {player_id}, error - {e}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False


    async def change_lang(self, message: types.Message, state: FSMContext ):
        player_id = message.from_user.id
        lang_old = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang_old):
            return False
        try:
            for lang in phrases.langs():
                if phrases.dict('name', lang) == str(message.text):
                    [ok, error] = await self.db_communicator.update_lang_player(message, state, lang)
                    if not ok:
                        await self._handle_error(player_id, error)
                        return False

                    self.langs[player_id] = lang
                    await message.answer(f"{phrases.dict('langChanged', lang)}", reply_markup=kb.main[lang])
                    await self.change_player(message, state, PlayerStates.main_menu_state)
                    logger.info(f"Successfully sent player update lang for user_id: {player_id}")
                    return True

            lang = self.langs.get(player_id, "en")
            await message.answer(f"{phrases.dict('invalidLang', lang)}", reply_markup=kb.main[lang])
            return False

        except Exception as e:
            msg = f"Failed to send player data to Kafka: {str(e)}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False


    async def give_up(self, message: types.Message, state: FSMContext ):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False

        [game_info, error] = await self.db_communicator.get_current_game(player_id)
        if game_info is None:
            await self._handle_error(player_id, error)
            return False
        game_id = game_info['id']

        [ok, error] = await self._give_up_in_game(player_id, game_id, False)
        if not ok:
            await self._handle_error(player_id, error)
            return False

        return True


    async def game_report(self, callback: types.CallbackQuery, state: FSMContext ):
        player_id = callback.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            [game_info, error] = await self.db_communicator.get_current_game(player_id)
            if game_info is None:
                await self._handle_error(player_id, error)
                return False
            game_id = game_info['id']
            finished = game_info['finished']

            if game_id == 0:
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("youAreNotInGame", lang)
                )
                return False

            if not finished:
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("gameNotFinished", lang)
                )
                return False

            [names, error] = await self.db_communicator.get_game_names(player_id, game_id)
            if names is None:
                await self._handle_error(player_id, error)
                return False

            [game_steps, error] = await self.db_communicator.get_game_report(player_id, game_id)
            if game_steps is None:
                await self._handle_error(player_id, error)
                return False

            [ok, report_text] = self._generate_game_report(game_steps, lang, names)
            if not ok:
                raise Exception(report_text)

            await self.bot.send_message(
                chat_id=player_id,
                text=report_text
            )

            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to send give up for user {player_id}, error - {e}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False


    async def exit_to_menu(self, message: Any, force):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            [db_response, error] = await self.db_communicator.get_lobby_id(player_id, False)
            if db_response is None:
                await self._handle_error(player_id, error)
                return False
            lobby_id = db_response['lobby_id']

            if lobby_id != 0:
                [ok, error] = await self._leave_lobby(player_id, lobby_id)
                if not ok:
                    await self._handle_error(player_id, error)
                    return False

            [game_info, error] = await self.db_communicator.get_current_game(player_id)
            if game_info is None:
                await self._handle_error(player_id, error)
                return False
            game_id = game_info['id']
            finished = game_info['finished']


            if game_id == 0:
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("youAreNotInGame", lang)
                )
                return False

            if not finished and not force:
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("gameNotFinished", lang)
                )
                return False


            [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.main_menu_state)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            await self.bot.send_message(
                chat_id=player_id,
                text=phrases.dict("menu", lang),
                reply_markup=kb.main[lang]
            )

            [ok, error] = await self.db_communicator.exit_from_game(player_id, game_id)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            logger.info(f"Player {player_id} exited from game {game_id}")

            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to send give up for user {player_id}, error - {e}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False


    async def stay_in_lobby(self, callback: types.CallbackQuery, state: FSMContext):
        player_id = callback.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            [db_response, error] = await self.db_communicator.get_lobby_id(player_id, False)
            if db_response is None:
                await self._handle_error(player_id, error)
                return False
            lobby_id = db_response['lobby_id']

            if lobby_id == 0:
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("youAreNotInLobby", lang)
                )
                return False

            [ok, error] = await self.db_communicator.join_lobby(player_id, lobby_id, "")
            if not ok:
                if error.Level() == ErrorLevel.WARNING and error.Message() == "DBAnswerAlreadyInLobby":
                    error = Error(ErrorLevel.ERROR, f"Error! Player already in Lobby with lobby_id - {lobby_id}")
                    await self._handle_error(player_id, error)
                    return False

                await self._handle_error(player_id, error)
                return False

            [lobby_players, error] = await self.db_communicator.get_lobby_players(player_id, lobby_id)
            if lobby_players is None:
                await self._handle_error(player_id, error)
                return False

            [lobby_names, error] = await self.db_communicator.get_lobby_names(player_id, lobby_id)
            if lobby_names is None:
                await self._handle_error(player_id, error)
                return False

            current_player = next((p for p in lobby_players if p['player_id'] == player_id), None)
            if not current_player:
                [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.main_menu_state)
                if not ok:
                    await self._handle_error(player_id, error)
                    return False
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("youAreNotInLobby", lang),
                    reply_markup=kb.main[lang]
                )
                return False

            [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.in_lobby)
            if not ok:
                await self._handle_error(player_id, error)
                return False
            await self.bot.send_message(
                chat_id=player_id,
                text=phrases.dict("youBackLobby", lang),
                reply_markup=kb.get_lobby_keyboard(current_player['host'], current_player['is_ready'], lang)
            )

            [ok, error] = await self._send_players_states(player_id, lobby_id, lobby_players, lobby_names)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to stay in lobby for user {player_id}: {str(e)}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False


    async def create_lobby(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            await self.change_player(message, state, PlayerStates.choose_lobby_creation_type)
            await message.answer(
                phrases.dict("chooseLobbyCreationType", lang),
                reply_markup=kb.lobby_creation_types[lang]
            )
            return True
        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False
        except Exception as e:
            msg = f"Failed to create lobby for user {player_id}: {str(e)}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False


    async def create_private_lobby(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            await self.change_player(message, state, PlayerStates.wait_password)
            await message.answer(
                f"{phrases.dict('enterLobbyPassword', lang)} {phrases.dict('useOnlyDigits', lang)}",
                reply_markup=ReplyKeyboardRemove()
            )
            return True
        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to create lobby for user {player_id}: {str(e)}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False


    async def create_lobby_with_password(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            password = message.text
            [lobby_id, error] = await self.db_communicator.create_lobby(player_id, password)
            if lobby_id is None:
                await self._handle_error(player_id, error)
                return False

            [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.in_lobby)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            msg = f"{phrases.dict('lobbyCreated', lang)} \n{phrases.dict('lobbyId', lang)} - {lobby_id}\n{phrases.dict('lobbyPassword', lang)} - {password}\n{phrases.dict('sendPrivate', lang)}"
            await self.bot.send_message(
                chat_id=player_id,
                text=msg,
                reply_markup=kb.get_lobby_keyboard(True, False, lang)
            )
            return True

        except Exception as e:
            msg = f"Failed to create lobby with password for user {player_id}: {str(e)}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False


    async def create_public_lobby(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            [lobby_id, error] = await self.db_communicator.create_lobby(player_id, "")
            if lobby_id is None:
                await self._handle_error(player_id, error)
                return False

            [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.in_lobby)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            msg = f"{phrases.dict('lobbyCreated', lang)} \n{phrases.dict('lobbyId', lang)} - {lobby_id}\n{phrases.dict('lobbyPublic', lang)}"
            await self.bot.send_message(
                chat_id=player_id,
                text=msg,
                reply_markup=kb.get_lobby_keyboard(True, False, lang)
            )
            return True

        except Exception as e:
            msg = f"Failed to create public lobby for user {player_id}: {str(e)}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False

    async def enter_lobby(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            await self.change_player(message, state, PlayerStates.choose_lobby_type)
            await message.answer(
                phrases.dict("chooseLobby", lang),
                reply_markup=kb.lobby_types[lang]
            )
            return True
        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to create lobby for user {player_id}: {str(e)}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False


    async def enter_by_lobby_id(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        if not message.text.isdigit():
            msg = f"{phrases.dict('invalidLobbyId', lang)} {phrases.dict('useOnlyDigits', lang)}"
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        lobby_id = int(message.text)
        [ok, error] = await self._enter_by_lobby_id(player_id, lobby_id)
        if not ok:
            await self._handle_error(player_id, error)
            return False
        return True


    async def enter_by_lobby_id_and_password(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        password = message.text
        try:

            if not password.isdigit() or len(password) > 10:
                await message.answer(
                    f"{phrases.dict('invalidPassword', lang)} {phrases.dict('useOnlyDigits', lang)}",
                    reply_markup=kb.game[lang]
                )
                await self.change_player(message, state, PlayerStates.choose_game)
                return False
            [db_response, error] = await self.db_communicator.get_lobby_id(player_id, True)
            if db_response is None:
                await self._handle_error(player_id, error)
                return False
            lobby_id = db_response['lobby_id']
            [ok, error] = await self.db_communicator.join_lobby(player_id, lobby_id, password)
            if not ok:
                if error.Level() != ErrorLevel.WARNING:
                    await self._handle_error(player_id, error)
                    return False

                error_msg = error.Message()

                if error_msg == "DBAnswerAlreadyInLobby":
                    error = Error(ErrorLevel.ERROR, f"Error! Player already in Lobby with lobby_id - {lobby_id}")
                    await self._handle_error(player_id, error)
                    return False

                await self.change_player(message, state, PlayerStates.choose_lobby_type)
                await message.answer(phrases.dict(error_msg, lang), reply_markup=kb.lobby_types[lang])
                return False

            await self.change_player(message, state, PlayerStates.in_lobby)
            await message.answer(
                phrases.dict("youEnteredLobby", lang),
                reply_markup=kb.get_lobby_keyboard(False, False, lang)
            )

            [lobby_players, error] = await self.db_communicator.get_lobby_players(player_id, lobby_id)
            if lobby_players is None:
                error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False

            [lobby_names, error] = await self.db_communicator.get_lobby_names(player_id, lobby_id)
            if lobby_names is None:
                error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False

            [ok, error] = await self._send_players_states(player_id, lobby_id, lobby_players, lobby_names)
            if not ok:
                error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False
            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to enter lobby with password for user {player_id}: {str(e)}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False

    async def enter_to_random_lobby(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            [lobby_id, error] = await self.db_communicator.get_random_lobby_id(player_id)
            if lobby_id is None:
                await self._handle_error(player_id, error)
                return False

            if lobby_id == 0:
                await message.answer(
                    phrases.dict("noPublicLobbies", lang),
                    reply_markup=kb.game[lang]
                )
                await self.change_player(message, state, PlayerStates.choose_game)
                return False

            [ok, error] = await self._enter_by_lobby_id(player_id, lobby_id)
            if not ok:
                await self._handle_error(player_id, error)
                return False
            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to enter random lobby for user {player_id}: {str(e)}"
            await self.change_player(message, state, PlayerStates.choose_game)
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False


    async def delete_player(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            [db_response, error] = await self.db_communicator.get_lobby_id(player_id, True)
            if db_response is None:
                await self._handle_error(player_id, error)
                return False
            lobby_id = db_response['lobby_id']
            is_host = db_response['is_host']

            if not is_host:
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("youDontHaveSufficientPermissions", lang)
                )
                return False


            [lobby_players, error] = await self.db_communicator.get_lobby_players(player_id, lobby_id)
            if lobby_players is None:
                error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False

            [lobby_names, error] = await self.db_communicator.get_lobby_names(player_id, lobby_id)
            if lobby_names is None:
                error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False

            players_to_ban = []
            for player in lobby_players:
                if player['player_id'] != player_id:
                    players_to_ban.append(player)
                    players_to_ban[-1]['name'] = next((n['name'] for n in lobby_names if n['id'] == player['player_id']), "Unknown")

            if not players_to_ban:
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("noPlayersToBan", lang)
                )
                return False

            keyboard_markup = kb.get_player_keyboard(players_to_ban, lang)

            await self.change_player(message, state, PlayerStates.ban_player_choose)
            await self.bot.send_message(
                chat_id=player_id,
                text=phrases.dict("whichPlayerNeedBan", lang),
                reply_markup=keyboard_markup
            )

            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to initiate player ban process for user {player_id}: {str(e)}"
            if lobby_id is None:
                await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            else:
                error = Error(ErrorLevel.LOBBY_ERROR, msg)
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
            return False


    async def choose_ban_player(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        lobby_id = None
        try:
            [db_response, error] = await self.db_communicator.get_lobby_id(player_id, True)
            if db_response is None:
                await self._handle_error(player_id, error)
                return False
            lobby_id = db_response['lobby_id']

            [lobby_players, error] = await self.db_communicator.get_lobby_players(player_id, lobby_id)
            if lobby_players is None:
                error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False

            [lobby_names, error] = await self.db_communicator.get_lobby_names(player_id, lobby_id)
            if lobby_names is None:
                error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False

            player_to_ban = None
            for name_data in lobby_names:
                if name_data['name'] == message.text and name_data['id'] != player_id:
                    player_to_ban = name_data['id']
                    break

            player_back = False
            if player_to_ban is None:
                if phrases.checkPhrase("back", str(message.text)):
                    player_back = True
                else:
                    await self.bot.send_message(
                        chat_id=player_id,
                        text=phrases.dict("invalidPlayerName", lang)
                    )
                    return False

            if not player_back:
                [ok, error] = await self.db_communicator.ban_player(lobby_id, player_id, player_to_ban)
                if not ok:
                    await self._handle_error(player_id, error)
                    return False

                [ok, error] = await self._change_player_state_by_id(player_to_ban, PlayerStates.main_menu_state)
                if not ok:
                    await self._handle_error(player_id, error)
                    return False

                [ok, error] = await self._send_other_ban_player(player_id, player_to_ban, lobby_names, lobby_players)
                if not ok:
                    await self._handle_error(player_id, error)
                    return False

                [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.in_lobby)
                if not ok:
                    await self._handle_error(player_id, error)
                    return False

                return True


            [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.in_lobby)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            current_player = next((p for p in lobby_players if p['player_id'] == player_id), None)
            if current_player:
                is_host = current_player['host']
                is_ready = current_player['is_ready']
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("youInLobby", lang),
                    reply_markup=kb.get_lobby_keyboard(is_host, is_ready, lang)
                )
            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to ban player for user {player_id}: {str(e)}"
            if lobby_id is None:
                await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            else:
                error = Error(ErrorLevel.LOBBY_ERROR, msg)
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
            return False

    async def set_player_ready_state(self, message: types.Message, state: FSMContext, is_ready: bool):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        lobby_id = None
        try:
            [db_response, error] = await self.db_communicator.get_lobby_id(player_id, True)
            if db_response is None:
                await self._handle_error(player_id, error)
                return False
            lobby_id = db_response['lobby_id']

            [ok, error] = await self.db_communicator.set_player_ready(player_id, lobby_id, is_ready)
            if not ok:
                await self._handle_error(player_id, error)
                return False
            logger.info(f"Player {player_id} set ready state to {is_ready} in lobby {lobby_id}")

            [lobby_players, error] = await self.db_communicator.get_lobby_players(player_id, lobby_id)
            if lobby_players is None:
                error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False

            [lobby_names, error] = await self.db_communicator.get_lobby_names(player_id, lobby_id)
            if lobby_names is None:
                error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False

            [ok, error] = await self._send_players_states(player_id, lobby_id, lobby_players, lobby_names)
            if not ok:
                error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False
            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to set ready state for user {player_id}: {str(e)}"
            if lobby_id is None:
                await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            else:
                error = Error(ErrorLevel.LOBBY_ERROR, msg)
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
            return False


    async def leave_lobby(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        [db_response, error] = await self.db_communicator.get_lobby_id(player_id, True)
        if db_response is None:
            await self._handle_error(player_id, error)
            return False
        lobby_id = db_response['lobby_id']
        [ok, error] = await self._leave_lobby(player_id, lobby_id)
        if not ok:
            await self._handle_error(player_id, error)
            return False
        await self.bot.send_message(
                chat_id=player_id,
                text=phrases.dict("chooseGameMode", lang),
                reply_markup=kb.game[lang]
            )
        return True

    async def start_lobby_game(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        lobby_id = None
        try:
            [db_response, error] = await self.db_communicator.get_lobby_id(player_id, True)
            if db_response is None:
                await self._handle_error(player_id, error)
                return False
            lobby_id = db_response['lobby_id']

            [ok, error] = await self.db_communicator.prepare_to_start_lobby(player_id, lobby_id)
            if not ok:
                if error.Level() == ErrorLevel.WARNING and error.Message() == "DBAnswerOnlyHostCanStartGame":
                    await self.bot.send_message(
                        chat_id=player_id,
                        text=phrases.dict("youDontHaveSufficientPermissions", lang)
                    )
                    return False
                else:
                    await self._handle_error(player_id, error)
                return False

            [lobby_players, error] = await self.db_communicator.get_lobby_players(player_id, lobby_id)
            if lobby_players is None:
                error = Error(ErrorLevel.LOBBY_ERROR, msg)
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False

            [game_id, error] = await self._start_game_with_players(lobby_id, lobby_players)
            if game_id is None:
                error = Error(ErrorLevel.LOBBY_ERROR, msg)
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False

            [ok, error] = await self.db_communicator.start_lobby_game(player_id, lobby_id, game_id)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            [ok, error] = await self._start_game(game_id, player_id)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            for player in lobby_players:
                [ok, error] = await self._change_player_state_by_id(player['player_id'], PlayerStates.waiting_for_number)
                if not ok:
                    if error.Level() == ErrorLevel.INFO or error.Level() == ErrorLevel.WARNING: continue
                    await self._handle_error(player['player_id'], error)
                    return False

                lang_i = self.langs.get(player['player_id'], "en")
                await self.bot.send_message(
                    chat_id=player['player_id'],
                    text=f"{phrases.dict('gameCreated', lang_i)}\n{phrases.dict('yourTurn', lang_i)}\n{phrases.dict('giveUpInfo', lang_i)}",
                    reply_markup=ReplyKeyboardRemove()
                )

            return True

        except asyncio.TimeoutError:
            msg = phrases.dict("errorTimeout", lang)
            await self._handle_error(player_id, Error(ErrorLevel.WARNING, msg))
            return False

        except Exception as e:
            msg = f"Failed to start lobby game for user {player_id}: {str(e)}"
            if lobby_id is None:
                await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            else:
                error = Error(ErrorLevel.LOBBY_ERROR, msg)
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
            return False


    async def unban_player(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            [db_response, error] = await self.db_communicator.get_lobby_id(player_id, True)
            if db_response is None:
                await self._handle_error(player_id, error)
                return False
            lobby_id = db_response['lobby_id']

            [blacklist, error] = await self.db_communicator.get_blacklist(lobby_id, player_id)
            if blacklist is None:
                if error.Message() == "DBAnswerOnlyHostCanUnbanPlayer":
                    error = Error(ErrorLevel.ERROR, phrases.dict("youDontHaveSufficientPermissions", lang))
                await self._handle_error(player_id, error)
                return False

            players_to_unban = []
            for player in blacklist:
                if player['player_id'] != player_id:
                    players_to_unban.append(player)

            if not players_to_unban:
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("emptyBlacklist", lang)
                )
                return False

            keyboard_markup = kb.get_player_keyboard(players_to_unban, lang)

            await self.change_player(message, state, PlayerStates.unban_player_choose)
            await self.bot.send_message(
                chat_id=player_id,
                text=phrases.dict("whichPlayerNeedUnban", lang),
                reply_markup=keyboard_markup
            )
            return True

        except Exception as e:
            msg = f"Failed to get banned players for user {player_id}: {str(e)}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False


    async def choose_unban_player(self, message: types.Message, state: FSMContext):
        player_id = message.from_user.id
        lang = self.langs.get(player_id, "en")
        if not await self._handle_server_available(player_id, lang):
            return False
        try:
            unbanned_name = str(message.text)
            [db_response, error] = await self.db_communicator.get_lobby_id(player_id, True)
            if db_response is None:
                await self._handle_error(player_id, error)
                return False
            lobby_id = db_response['lobby_id']

            [blacklist, error] = await self.db_communicator.get_blacklist(lobby_id, player_id)
            if blacklist is None:
                if error.Message() == "DBAnswerOnlyHostCanUnbanPlayer":
                    error = Error(ErrorLevel.ERROR, phrases.dict("youDontHaveSufficientPermissions", lang))
                await self._handle_error(player_id, error)
                return False

            player_to_unban = None
            for banned_player in blacklist:
                if banned_player['name'] == unbanned_name:
                    player_to_unban = banned_player['player_id']
                    break

            player_back = False
            if player_to_unban is None:
                if phrases.checkPhrase("back", unbanned_name):
                    player_back = True
                else:
                    await self.bot.send_message(
                        chat_id=player_id,
                        text=phrases.dict("invalidPlayerName", lang)
                    )
                    return False

            if not player_back:
                [ok, error] = await self.db_communicator.unban_player(lobby_id, player_id, player_to_unban)
                if not ok:
                    if error.Level() == ErrorLevel.WARNING and error.Message() == "DBAnswerOnlyHostCanUnbanPlayer":
                        await self.bot.send_message(
                            chat_id=player_id,
                            text=phrases.dict("youDontHaveSufficientPermissions", lang)
                        )
                        return False
                    await self._handle_error(player_id, error)
                    return False

            [lobby_players, error] = await self.db_communicator.get_lobby_players(player_id, lobby_id)
            if lobby_players is None:
                error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                error.setLobbyId(lobby_id)
                await self._handle_error(player_id, error)
                return False

            if not player_back:
                [lobby_names, error] = await self.db_communicator.get_lobby_names(player_id, lobby_id)
                if lobby_names is None:
                    error = Error(ErrorLevel.LOBBY_ERROR, error.Message())
                    error.setLobbyId(lobby_id)
                    await self._handle_error(player_id, error)
                    return False
                [ok, error] = await self._send_other_unban_player(player_id, player_to_unban, unbanned_name, lobby_id, lobby_names, lobby_players)
                if not ok:
                    await self._handle_error(player_id, error)
                    return False

            [ok, error] = await self._change_player_state_by_id(player_id, PlayerStates.in_lobby)
            if not ok:
                await self._handle_error(player_id, error)
                return False

            current_player = next((p for p in lobby_players if p['player_id'] == player_id), None)
            if current_player:
                is_host = current_player['host']
                is_ready = current_player['is_ready']
                await self.bot.send_message(
                    chat_id=player_id,
                    text=phrases.dict("youInLobby", lang),
                    reply_markup=kb.get_lobby_keyboard(is_host, is_ready, lang)
                )
            return True

        except Exception as e:
            msg = f"Failed to unban player: {str(e)}"
            await self._handle_error(player_id, Error(ErrorLevel.ERROR, msg))
            return False