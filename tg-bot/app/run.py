from aiogram import Bot, Dispatcher
import logging
import os
from message_handler import router
import asyncio
from event_handler import bot

# Initialize dispatcher and bot
dp = Dispatcher()

async def main():
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Bot Stopped!')