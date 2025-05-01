from aiogram import types, F, Router
from aiogram.filters import CommandStart, Command
import json
import os
import keyboards as kb
from phrases import phrases
from consumer import waiting_messages, get_uniq_message_id
from kafka_commands import send_to_kafka


# Settings
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_POSTGRES_SEND = os.environ['TOPIC_POSTGRES_SEND']
TOPIC_POSTGRES_REQUEST = os.environ['TOPIC_POSTGRES_REQUEST']
SERVER_ID = 1

router = Router()

@router.message(CommandStart())
async def start_command(message: types.Message):
    lang = message.from_user.language_code
    data = {
        'id': message.from_user.id,
        'firstname': message.from_user.first_name,
        'lastname': message.from_user.last_name,
        'fullname': message.from_user.full_name,
        'lang': lang,
    }
    kafka_message = {
        'command': "change_player",
        'data': data,
    }
    await send_to_kafka(TOPIC_POSTGRES_SEND, kafka_message)
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
    data = {
        'id': message.from_user.id,
        'server_id': SERVER_ID,
        'stage': 'UNKNOWN',
        'step': 0,
    }
    messag_id = get_uniq_message_id()
    waiting_messages.append(messag_id)
    kafka_message = {
        'command': "insert_game",
        'message_id': messag_id,
        'data': data,
    }
    await send_to_kafka(TOPIC_POSTGRES_SEND, kafka_message)