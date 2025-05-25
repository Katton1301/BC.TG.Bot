from aiogram import BaseMiddleware
from aiogram.fsm.context import FSMContext
import logging

logger = logging.getLogger(__name__)

class StateRestoreMiddleware(BaseMiddleware):
    def __init__(self, event_handler):
        super().__init__()
        self.event_handler = event_handler

    async def __call__(self, handler, event, data):

        user_id = event.message.from_user.id
        state: FSMContext = data['state']
        
        if state is None:
            storage = data['fsm_storage']
            state = FSMContext(
                storage=storage,
                user_id=user_id,
                chat_id=data['event_chat'].id
            )
            data['state'] = state

        if user_id in self.event_handler.players_state:
            saved_state = self.event_handler.players_state[user_id]
            
            await state.set_state(saved_state)
            await state.update_data() 

            logger.info(f"Storage state after update: {await state.get_state()}")

        return await handler(event, data)