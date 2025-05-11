from aiogram import types, F, Router
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import keyboards as kb
from phrases import phrases

router = Router()

class GameStates(StatesGroup):
    waiting_for_number = State()

def setup_event_handler(eh):
    router.message.register(start_command, CommandStart())
    router.message.register(help_command, Command('help'))
    router.message.register(rules_command, F.text.contains('правила') | F.text.contains('rules'))
    router.message.register(game_command, lambda msg: phrases.checkPhrase("game", str(msg.text)))
    router.message.register(single_play_command, lambda msg: phrases.checkPhrase("singlePlay", str(msg.text)))
    router.message.register(handle_number_input, GameStates.waiting_for_number)
    
    router.ee = eh

@router.message(CommandStart())
async def start_command(message: types.Message):
    eh = router.ee
    lang = message.from_user.language_code
    await eh.change_player(message)
    await message.answer(phrases.dict("greeting", lang), reply_markup=kb.main[lang])

@router.message(Command('help'))
async def help_command(message: types.Message):
    lang = message.from_user.language_code
    await message.answer(
        phrases.dict("help",lang),
        reply_markup=kb.main[lang])


@router.message(F.text.contains('правила') or F.text.contains('rules'))
async def rules_command(message: types.Message):
    lang = message.from_user.language_code
    await message.answer(
        phrases.dict("fullRules",lang),
        reply_markup=kb.main[lang])

@router.message(lambda msg: phrases.checkPhrase("game", str(msg.text)))
async def game_command(message: types.Message):
    lang = message.from_user.language_code
    await message.answer(
        phrases.dict("chooseGameMode", lang),
        reply_markup=kb.game[lang])
    
@router.message(lambda msg: phrases.checkPhrase("singlePlay", str(msg.text)))
async def single_play_command(message: types.Message, state: FSMContext):
    eh = router.ee
    ok = await eh.start_single_game(message)
    if ok:
        await state.set_state(GameStates.waiting_for_number)

@router.message(GameStates.waiting_for_number)
async def handle_number_input(message: types.Message, state: FSMContext):
    eh = router.ee
    finish = await eh.do_step(message)
    if finish:
        await state.clear()