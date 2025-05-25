from aiogram import Dispatcher
from aiogram import types, F, Router
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import keyboards as kb
from phrases import phrases
import logging

router = Router()

class PlayerStates(StatesGroup):
    free_state = State()
    choose_game = State()
    choose_bot_difficulty = State()
    waiting_a_rival = State()
    waiting_for_number = State()

def setup_event_handler(eh):
    router.message.register(play_command, StateFilter(PlayerStates.choose_game))
    router.message.register(handle_bot_difficulty, StateFilter(PlayerStates.choose_bot_difficulty))
    router.message.register(handle_number_input, StateFilter(PlayerStates.waiting_for_number))
    router.message.register(start_command, CommandStart())
    router.message.register(help_command, Command('help'))
    router.message.register(rules_command, F.text.contains('правила') | F.text.contains('rules'))
    router.message.register(game_command, lambda msg: phrases.checkPhrase("game", str(msg.text)))
    router.message.register(back_command, lambda msg: phrases.checkPhrase("back", str(msg.text)))
    router.message.register(handle_warning_digit, lambda msg: msg.text.isdigit())

    router.ee = eh

@router.message(CommandStart())
async def start_command(message: types.Message, state: FSMContext):
    eh = router.ee
    await eh.change_player(message, state, PlayerStates.free_state)
    lang = message.from_user.language_code
    await message.answer(phrases.dict("greeting", lang), reply_markup=kb.main[lang])

@router.message(Command('help'))
async def help_command(message: types.Message, state: FSMContext):
    eh = router.ee
    await eh.change_player(message, state, PlayerStates.free_state)
    lang = message.from_user.language_code
    await message.answer(
        phrases.dict("help",lang),
        reply_markup=kb.main[lang])


@router.message(F.text.contains('правила') or F.text.contains('rules'))
async def rules_command(message: types.Message, state: FSMContext):
    eh = router.ee
    await eh.change_player(message, state, PlayerStates.free_state)
    lang = message.from_user.language_code
    await message.answer(
        phrases.dict("fullRules",lang),
        reply_markup=kb.main[lang])

@router.message(lambda msg: phrases.checkPhrase("game", str(msg.text)))
async def game_command(message: types.Message, state: FSMContext):
    eh = router.ee
    await eh.change_player(message, state, PlayerStates.choose_game)
    lang = message.from_user.language_code
    await message.answer(
        phrases.dict("chooseGameMode", lang),
        reply_markup=kb.game[lang])

@router.message(StateFilter(PlayerStates.choose_game))
async def play_command(message: types.Message, state: FSMContext):
    eh = router.ee
    if phrases.checkPhrase("singlePlay", str(message.text)):
        ok = await eh.start_single_game(message)
        if ok:
            await eh.change_player(message, state, PlayerStates.waiting_for_number)
    elif phrases.checkPhrase("botPlay", str(message.text)):
        await eh.change_player(message, state, PlayerStates.choose_bot_difficulty)
        lang = message.from_user.language_code
        await message.answer(
            phrases.dict("chooseBotDifficulty",lang),
            reply_markup=kb.bot[lang])
    elif phrases.checkPhrase("randomPlay", str(message.text)):
        await eh.start_random_game(message, state)

@router.message(StateFilter(PlayerStates.choose_bot_difficulty))
async def handle_bot_difficulty(message: types.Message, state: FSMContext):
    eh = router.ee
    ok = await eh.start_bot_play(message)
    if ok:
        await eh.change_player(message, state, PlayerStates.waiting_for_number)

@router.message(StateFilter(PlayerStates.waiting_for_number))
async def handle_number_input(message: types.Message, state: FSMContext):
    eh = router.ee
    finish = await eh.do_step(message)
    if finish:
        await eh.change_player(message, state, PlayerStates.free_state)

@router.message(lambda msg: phrases.checkPhrase("back", str(msg.text)))
async def back_command(message: types.Message, state: FSMContext):
    eh = router.ee
    lang = message.from_user.language_code
    if await state.get_state() == PlayerStates.choose_bot_difficulty:
        await eh.change_player(message, state, PlayerStates.choose_game)
        await message.answer(
            phrases.dict("chooseGameMode", lang),
            reply_markup=kb.game[lang])
    elif await state.get_state() == PlayerStates.choose_game:
        await eh.change_player(message, state, PlayerStates.free_state)
        await message.answer(phrases.dict("menu", lang), reply_markup=kb.main[lang])

@router.message(F.text.isdigit(), PlayerStates.free_state)
async def handle_warning_digit(message: types.Message, state: FSMContext):
    lang = message.from_user.language_code
    logging.info(f"wrong state {await state.get_state()}")
    await message.answer(phrases.dict("warningDigit", lang), reply_markup=kb.main[lang])
