from aiogram import Dispatcher
import logging
from message_handler import router, setup_event_handler
import asyncio
from event_handler import EventHandler
from aiogram import Bot
import os

def getToken():
    token = os.environ.get("TG_API_BC_TOKEN", "")
    if token:
        logging.info("Telegram token loaded from environment variable")
        return token
    token_file_path = os.environ.get('TG_API_BC_TOKEN_FILE', "")
    if token_file_path:
        try:
            with open(token_file_path, 'r') as f:
                token = f.read().strip()
                logging.info("Telegram token loaded from file")
                return token
        except FileNotFoundError:
            logging.error(f"Token file not found: {token_file_path}")
        except Exception as e:
            logging.error(f"Error reading token file: {e}")
    
    logging.error("Telegram token not found in TG_API_BC_TOKEN environment variable or TG_API_BC_TOKEN_FILE file")

async def main():

    bot = Bot(token=getToken())
    dp = Dispatcher()
    eh = EventHandler(bot, dp)
    await eh.start()

    await setup_event_handler(eh)
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