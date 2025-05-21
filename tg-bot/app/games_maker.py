import asyncio
from aiogram import types
from threading import Thread
from typing import List, Optional
from aiogram.fsm.context import FSMContext
from phrases import phrases
from message_handler import PlayerStates
import time

import logging
logger = logging.getLogger(__name__)

class RandomMatchmaker:
    def __init__(self, gameserver):
        self.gameserver = gameserver
        self.queued_players = []
        self.lock = asyncio.Lock()
        self._running = False
        self._thread: Optional[Thread] = None
        self.loop = asyncio.get_event_loop()

    async def add_player_to_queue(self, message: types.Message, state : FSMContext):
        async with self.lock:
            self.queued_players.append([message, state])
            if len(self.queued_players) < 2:
                lang = message.from_user.language_code
                await state.set_state(PlayerStates.waiting_a_rival)
                await message.answer(f"{phrases.dict('waitingForOpponent', lang)}")

    async def _start_random_game(self, players):
        try:
            [message1, _] = players[0]
            [message2, _] = players[1]
            game_id = await self.gameserver._create_game(message1.from_user.id)
            if game_id == 0:
                raise Exception("Failed to start random game")

            ok = await self.gameserver._add_player_to_game(game_id, message2.from_user.id)
            if not ok:
                raise Exception("Failed to start random game")

            ok = await self.gameserver._start_game(game_id, message1.from_user.id)
            if ok:
                for [message,state] in players:
                    await state.set_state(PlayerStates.waiting_for_number)
                    lang = message.from_user.language_code
                    await message.answer(
                        f"{phrases.dict('gameCreated', lang)}\n{phrases.dict('yourTurn', lang)}"
                    )
            else:
                raise Exception("Failed to start game")

        except Exception as e:
            logger.exception(f"Failed to start random game: {e}")
            for message in players:
                await message.answer(e)

    async def start(self):
        if not self._running:
            self._running = True
            self._thread = Thread(target=self._run, daemon=True)
            self._thread.start()

    async def stop(self):
        self._running = False
        if self._thread:
            self._thread.join()

    def _run(self):
        while self._running:
            asyncio.run_coroutine_threadsafe(self._check_queues(), self.loop)
            time.sleep(1)

    async def _check_queues(self):
        async with self.lock:
            if len(self.queued_players) > 1:
                await self._start_random_game(self.queued_players[:2])
                self.queued_players = self.queued_players[2:]