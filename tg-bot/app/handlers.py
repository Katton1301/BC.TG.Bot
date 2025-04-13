from aiogram import types, F, Router
from aiogram.filters import CommandStart, Command
from aiokafka import AIOKafkaProducer
import json

import keyboards as kb
from phrases import phrases


# Settings
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
TOPIC_NAME = 'notifications'

# Ititialize Kafka Producer

router = Router()


async def send_to_kafka(data):
    kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await kafka_producer.start()
    try:
        await kafka_producer.send_and_wait(TOPIC_NAME, json.dumps(data).encode('utf-8'))
    finally:
        await kafka_producer.stop()

@router.message(CommandStart())
async def start_command(message: types.Message):
    data = {
        'message': message.text
    }
    await send_to_kafka(data)
    await message.answer(phrases.dict("greeting",'ru'), reply_markup=kb.main)

@router.message(Command('help'))
async def help_command(message: types.Message):
    await message.answer(
        phrases.dict("help",'ru'),
        reply_markup=kb.main)


@router.message(F.text.contains('правила') or F.text.contains('rules'))
async def rules_command(message: types.Message):
    await message.answer(phrases.dict("fullRules",'ru'))

@router.message(F.text == phrases.dict("game",'ru'))
async def game_command(message: types.Message):
    await message.answer(
        phrases.dict("help",'ru'),
        reply_markup=kb.game)