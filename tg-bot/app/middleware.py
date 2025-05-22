from aiogram import BaseMiddleware
from aiogram.fsm.context import FSMContext
import logging

logger = logging.getLogger(__name__)

class StateRestoreMiddleware(BaseMiddleware):
    def __init__(self, event_handler):
        super().__init__()
        self.event_handler = event_handler

    async def __call__(self, handler, event, data):
        if not hasattr(event, 'from_user'):
            return await handler(event, data)

        user_id = event.from_user.id
        state: FSMContext = data['state']

        if user_id in self.event_handler.players_state:
            saved_state = self.event_handler.players_state[user_id]
            current_state = await state.get_state()
            
            if current_state != saved_state:
                await state.set_state(saved_state)
                logger.info(f"State restored for {user_id}: {saved_state}")


        return await handler(event, data)