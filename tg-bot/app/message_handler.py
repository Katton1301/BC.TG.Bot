from aiogram import types, F, Router
from aiogram.filters import CommandStart, Command
import keyboards as kb
from phrases import phrases
from event_handler import EventHandler

router = Router()
eh = EventHandler()

@router.message(CommandStart())
async def start_command(message: types.Message):
    lang = message.from_user.language_code
    await eh.change_player(message)
    await message.answer(phrases.dict("greeting",lang), reply_markup=kb.main[lang])

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
async def game_command(message: types.Message):
    await eh.create_game(message)