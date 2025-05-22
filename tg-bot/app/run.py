from aiogram import Dispatcher
import logging
from message_handler import router, setup_event_handler
import asyncio
from event_handler import EventHandler
from aiogram import Bot
import os

async def main():

    bot = Bot(token=os.environ["TG_API_BC_TOKEN"])
    eh = EventHandler(bot)
    await eh.start()

    await eh.initGameServer()
    dp = Dispatcher()

    setup_event_handler(eh)
    dp.include_router(router)
    try:
        await dp.start_polling(bot)
    finally:
        await eh.stop()
        await bot.session.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info('Bot stopped gracefully')
    except Exception as e:
        logging.error(f'Bot crashed: {e}')