from aiogram import Bot, Dispatcher
import logging
import os
from handlers import router
import asyncio
from consumer import bot, run_consumer

# Initialize dispatcher and bot
dp = Dispatcher()

async def main():
    dp.include_router(router)
    bot_task = asyncio.create_task(dp.start_polling(bot))
    consumer_task = asyncio.create_task(run_consumer())
    
    await asyncio.gather(bot_task, consumer_task)
    #await dp.start_polling(bot)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Bot Stopped!')