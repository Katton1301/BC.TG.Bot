from aiogram import Bot, Dispatcher
import logging
import os
from handlers import router
import asyncio

# Initialize dispatcher and bot
bot = Bot(token=os.environ["TG_API_BC_TOKEN"])
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