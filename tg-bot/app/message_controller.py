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
        await self.eh.new_player_start(message, state)


    async def command_help(self, message: types.Message, state: FSMContext):
        await self.eh.help_player(message, state)


    async def state_main_menu(self, message: types.Message, state: FSMContext):
        lang = self.eh.langs[message.from_user.id]
        if phrases.checkPhrase("game", str(message.text)):
            ok = await self.eh.change_player(message, state, PlayerStates.choose_game)
            if not ok:
                return
            await message.answer(
                phrases.dict("chooseGameMode", lang),
                reply_markup=kb.game[lang])
        elif phrases.checkPhrase("rules", str(message.text)):
            await message.answer(
                phrases.dict("fullRules",lang),
                reply_markup=kb.main[lang])
        elif phrases.checkPhrase("lang", str(message.text)):
            ok = await self.eh.change_player(message, state, PlayerStates.lang_state)
            if not ok:
                return
            await message.answer(
                phrases.dict("lang",lang),
                reply_markup=kb.lang[lang])
        elif phrases.checkPhrase("feedback", str(message.text)):
            ok = await self.eh.change_player(message, state, PlayerStates.feedback_state)
            if not ok:
                return
            await message.answer(
                phrases.dict("writeFeedback",lang),
                reply_markup=ReplyKeyboardRemove())
        elif message.text.isdigit():
            await message.answer(phrases.dict("warningDigit", lang), reply_markup=kb.main[lang])
        else:
            await message.answer(phrases.dict("chooseMenuItem", lang), reply_markup=kb.main[lang])

    async def state_choose_game(self, message: types.Message, state: FSMContext):
        lang = self.eh.langs[message.from_user.id]
        if phrases.checkPhrase("singlePlay", str(message.text)):
            await self.eh.start_single_game(message, state)
        elif phrases.checkPhrase("botPlay", str(message.text)):
            ok = await self.eh.change_player(message, state, PlayerStates.choose_bot_difficulty)
            if not ok:
                return
            await message.answer(
                phrases.dict("chooseBotDifficulty",lang),
                reply_markup=kb.bot[lang])
        elif phrases.checkPhrase("randomPlay", str(message.text)):
            await self.eh.start_random_game(message, state)
        elif phrases.checkPhrase("createLobby", str(message.text)):
            await self.eh.create_lobby(message, state)
        elif phrases.checkPhrase("enterLobby", str(message.text)):
            await self.eh.enter_lobby(message, state)
        elif phrases.checkPhrase("back", str(message.text)):
            ok = await self.eh.change_player(message, state, PlayerStates.main_menu_state)
            if not ok:
                return
            await message.answer(phrases.dict("menu", lang), reply_markup=kb.main[lang])
        elif message.text.isdigit():
            await message.answer(phrases.dict("warningDigit", lang), reply_markup=kb.game[lang])
        else:
            await message.answer(phrases.dict("chooseMenuItem", lang), reply_markup=kb.game[lang])

    async def state_choose_bot_difficulty(self, message: types.Message, state: FSMContext):
        lang = self.eh.langs[message.from_user.id]
        if phrases.checkPhrase("back", str(message.text)):
            ok = await self.eh.change_player(message, state, PlayerStates.choose_game)
            if not ok:
                return
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

    async def state_game_step(self, message: types.Message, state: FSMContext):
        lang = self.eh.langs[message.from_user.id]
        if phrases.checkPhrase("giveUpCommand", str(message.text).lower()):
            await self.eh.give_up(message, state)
        elif not message.text.isdigit():
            await message.answer(phrases.dict("warningNotDigit", lang))
        else:
            await self.eh.do_step(message, state)


    async def choose_exit_game_after_give_up(self, message: types.Message, state: FSMContext):
        lang = self.eh.langs[message.from_user.id]
        if phrases.checkPhrase("stay", str(message.text).lower()):
            ok = await self.eh.change_player(message, state, PlayerStates.waiting_game_end)
            if not ok:
                return
            await message.answer(phrases.dict("waitingEndGame", lang), reply_markup=ReplyKeyboardRemove())
        elif phrases.checkPhrase("exit", str(message.text).lower()):
            await self.eh.exit_to_menu(message, state)
        else:
            await message.answer(phrases.dict("chooseMenuItem", lang), reply_markup=kb.exit_or_not[lang])


    async def waiting_game_end(self, message: types.Message, state: FSMContext):
        lang = self.eh.langs[message.from_user.id]
        await message.answer(phrases.dict("pleaseWaitGameEnd", lang), reply_markup=ReplyKeyboardRemove())


    async def state_lang(self, message: types.Message, state: FSMContext):
        lang = self.eh.langs[message.from_user.id]
        if phrases.checkPhrase("name", str(message.text)):
            await self.eh.change_lang(message, state)
        elif phrases.checkPhrase("back", str(message.text)):
            ok = await self.eh.change_player(message, state, PlayerStates.main_menu_state)
            if not ok:
                return
            await message.answer(phrases.dict("menu", lang), reply_markup=kb.main[lang])
        else:
            await message.answer(phrases.dict("chooseMenuItem", lang), reply_markup=kb.main[lang])

    async def state_feedback(self, message: types.Message, state: FSMContext):
        await self.eh.send_feedback(message, state)

    async def callback_full_game(self, callback: types.CallbackQuery, state: FSMContext):
        if await state.get_state() is None:
            await self.eh.new_player_start(callback, state)
        else:
            await self.eh.game_report(callback, state)
        await callback.answer()

    async def callback_to_menu(self, callback: types.CallbackQuery, state: FSMContext):
        if await state.get_state() is None:
            await self.eh.new_player_start(callback, state)
        else:
            await self.eh.exit_to_menu(callback, state)
        await callback.answer()

    async def callback_stay_in_lobby(self, callback: types.CallbackQuery, state: FSMContext):
        if await state.get_state() is None:
            await self.eh.new_player_start(callback, state)
        else:
            await self.eh.stay_in_lobby(callback, state)
        await callback.answer()

    async def state_waiting_a_rival(self, message: types.Message, state: FSMContext):
        lang = self.eh.langs[message.from_user.id]
        await message.answer(phrases.dict("rivalStillWaiting", lang))

    async def state_wait_password_for_lobby(self, message: types.Message, state: FSMContext):
        await self.eh.create_lobby_with_password(message, state)

    async def state_in_lobby(self, message: types.Message, state: FSMContext):
        if phrases.checkPhrase("ready", str(message.text)):
            await self.eh.set_player_ready_state(message, state, True)
        elif phrases.checkPhrase("notReady", str(message.text)):
            await self.eh.set_player_ready_state(message, state, False)
        elif phrases.checkPhrase("leaveLobby", str(message.text)):
            await self.eh.leave_lobby(message, state)
        elif phrases.checkPhrase("startGame", str(message.text)):
            await self.eh.start_lobby_game(message, state)
        elif phrases.checkPhrase("banPlayer", str(message.text)):
            await self.eh.delete_player(message, state)
        else:
            lang = self.eh.langs[message.from_user.id]
            await message.answer(phrases.dict("chooseMenuItem", lang))

    async def state_ban_player_choose(self, message: types.Message, state: FSMContext):
        await self.eh.choose_ban_player(message, state)

    async def state_choose_lobby_type(self, message: types.Message, state: FSMContext):
        lang = self.eh.langs[message.from_user.id]
        if phrases.checkPhrase("enterByLobbyId", str(message.text)):
            ok = await self.eh.change_player(message, state, PlayerStates.enter_lobby_id)
            if not ok:
                return
            await message.answer(
                phrases.dict("enterLobbyId", lang),
                reply_markup=ReplyKeyboardRemove()
            )
        elif phrases.checkPhrase("enterRandomPublicLobby", str(message.text)):
            await self.eh.enter_to_random_lobby(message, state)
        elif phrases.checkPhrase("back", str(message.text)):
            ok = await self.eh.change_player(message, state, PlayerStates.choose_game)
            if not ok:
                return
            await message.answer(phrases.dict("chooseGameMode", lang), reply_markup=kb.game[lang])
        else:
            await message.answer(phrases.dict("chooseMenuItem", lang), reply_markup=kb.lobby_types[lang])

    async def state_choose_lobby_creation_type(self, message: types.Message, state: FSMContext):
        lang = self.eh.langs[message.from_user.id]
        if phrases.checkPhrase("createPrivateLobby", str(message.text)):
            await self.eh.create_private_lobby(message, state)
        elif phrases.checkPhrase("createPublicLobby", str(message.text)):
            await self.eh.create_public_lobby(message, state)
        elif phrases.checkPhrase("back", str(message.text)):
            ok = await self.eh.change_player(message, state, PlayerStates.choose_game)
            if not ok:
                return
            await message.answer(phrases.dict("chooseGameMode", lang), reply_markup=kb.game[lang])
        else:
            await message.answer(phrases.dict("chooseMenuItem", lang), reply_markup=kb.lobby_creation_types[lang])

    async def state_enter_lobby_id(self, message: types.Message, state: FSMContext):
        lang = self.eh.langs[message.from_user.id]
        if message.text.isdigit():
            await self.eh.enter_by_lobby_id(message, state)
        else:
            await message.answer(phrases.dict("lobbyIdMustBeNumber", lang))

    async def state_enter_password(self, message: types.Message, state: FSMContext):
        await self.eh.enter_by_lobby_id_and_password(message, state)