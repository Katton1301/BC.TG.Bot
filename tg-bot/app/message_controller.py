from aiogram import types
from aiogram.fsm.context import FSMContext
import keyboards as kb
from phrases import phrases
from event_handler import EventHandler
from player_state import PlayerStates
from aiogram.types import ReplyKeyboardRemove


import logging
logger = logging.getLogger(__name__)

class MessageController:
    def __init__(self, eh: EventHandler):
        self.eh : EventHandler = eh

    async def command_start(self, message: types.Message, state: FSMContext):
        await self.eh.insert_player(message, state)
        lang = message.from_user.language_code
        await message.answer(phrases.dict("greeting", lang), reply_markup=kb.main[lang])


    async def command_help(self, message: types.Message, state: FSMContext):
        await self.eh.change_player(message, state, PlayerStates.main_menu_state)
        lang = message.from_user.language_code
        await message.answer(
            phrases.dict("help",lang),
            reply_markup=kb.main[lang])
        
    async def state_main_menu(self, message: types.Message, state: FSMContext):
        lang = message.from_user.language_code
        if phrases.checkPhrase("game", str(message.text)):
            await self.eh.change_player(message, state, PlayerStates.choose_game)
            await message.answer(
                phrases.dict("chooseGameMode", lang),
                reply_markup=kb.game[lang])
        elif phrases.checkPhrase("rules", str(message.text)):
            await message.answer(
                phrases.dict("fullRules",lang),
                reply_markup=kb.main[lang])
        elif phrases.checkPhrase("lang", str(message.text)):
            answer = f"{phrases.dict("function",lang)} '{phrases.dict("lang",lang)}' {phrases.dict("underway",lang)}"
            await message.answer(
                answer,
                reply_markup=kb.main[lang])
        elif phrases.checkPhrase("feedback", str(message.text)):
            await self.eh.change_player(message, state, PlayerStates.feedback_state)
            await message.answer(
                phrases.dict("writeFeedback",lang),
                reply_markup=ReplyKeyboardRemove())
        elif message.text.isdigit():
            await message.answer(phrases.dict("warningDigit", lang), reply_markup=kb.main[lang])
        else:
            await message.answer(phrases.dict("chooseMenuItem", lang), reply_markup=kb.main[lang])

    async def state_choose_game(self, message: types.Message, state: FSMContext):
        lang = message.from_user.language_code
        if phrases.checkPhrase("singlePlay", str(message.text)):
            await self.eh.start_single_game(message, state)
        elif phrases.checkPhrase("botPlay", str(message.text)):
            await self.eh.change_player(message, state, PlayerStates.choose_bot_difficulty)
            await message.answer(
                phrases.dict("chooseBotDifficulty",lang),
                reply_markup=kb.bot[lang])
        elif phrases.checkPhrase("randomPlay", str(message.text)):
            await self.eh.start_random_game(message, state)
        elif phrases.checkPhrase("createLobby", str(message.text)):
            answer = f"{phrases.dict("function",lang)} '{phrases.dict("createLobby",lang)}' {phrases.dict("underway",lang)}"
            await message.answer(
                answer,
                reply_markup=kb.game[lang])
        elif phrases.checkPhrase("enterLobby", str(message.text)):
            answer = f"{phrases.dict("function",lang)} '{phrases.dict("enterLobby",lang)}' {phrases.dict("underway",lang)}"
            await message.answer(
                answer,
                reply_markup=kb.game[lang])
        elif phrases.checkPhrase("back", str(message.text)):
            await self.eh.change_player(message, state, PlayerStates.main_menu_state)
            await message.answer(phrases.dict("menu", lang), reply_markup=kb.main[lang])
        elif message.text.isdigit():
            await message.answer(phrases.dict("warningDigit", lang), reply_markup=kb.game[lang])
        else:
            await message.answer(phrases.dict("chooseMenuItem", lang), reply_markup=kb.game[lang])

    async def state_choose_bot_difficulty(self, message: types.Message, state: FSMContext):
        lang = message.from_user.language_code
        if phrases.checkPhrase("back", str(message.text)):
            await self.eh.change_player(message, state, PlayerStates.choose_game)
            await message.answer(
                phrases.dict("chooseGameMode", lang),
                reply_markup=kb.game[lang])
        elif message.text.isdigit():
            await message.answer(phrases.dict("warningDigit", lang), reply_markup=kb.bot[lang])
        elif phrases.checkPhrase("easy", str(message.text)) or \
            phrases.checkPhrase("medium", str(message.text)) or \
            phrases.checkPhrase("hard", str(message.text)):
            await self.eh.start_bot_play(message, state)
        else:
            await message.answer(phrases.dict("chooseMenuItem", lang), reply_markup=kb.game[lang])
            
    async def state_waiting_for_number(self, message: types.Message, state: FSMContext):
        lang = message.from_user.language_code
        if not message.text.isdigit():
            await message.answer(phrases.dict("warningNotDigit", lang))
        else:
            await self.eh.do_step(message, state)

    async def state_feedback(self, message: types.Message, state: FSMContext):
        await self.eh.send_feedback(message, state)

    async def state_waiting_a_rival(self, message: types.Message, state: FSMContext):
        lang = message.from_user.language_code
        await message.answer(phrases.dict("rivalStillWaiting", lang))