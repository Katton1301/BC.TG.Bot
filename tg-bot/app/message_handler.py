from aiogram import types, F, Router
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from player_state import PlayerStates
from message_controller import MessageController

router = Router()

async def setup_event_handler(eh):
    router.message.register(start_command, CommandStart())
    router.message.register(help_command, Command('help'))
    router.message.register(handle_main_menu, StateFilter(PlayerStates.main_menu_state))
    router.message.register(play_command, StateFilter(PlayerStates.choose_game))
    router.message.register(handle_bot_difficulty, StateFilter(PlayerStates.choose_bot_difficulty))
    router.message.register(handle_number_input, StateFilter(PlayerStates.waiting_for_number))
    router.message.register(handle_waiting_a_rival, StateFilter(PlayerStates.waiting_a_rival))
    router.message.register(handle_lang, StateFilter(PlayerStates.lang_state))
    router.message.register(handle_feedback, StateFilter(PlayerStates.feedback_state))
    router.message.register(handle_full_game, F.data == "full_game")
    router.message.register(handle_to_menu, F.data == "to_menu")
    router.message.register(unhandled_message)
    router.controller = MessageController(eh)

@router.message(CommandStart())
async def start_command(message: types.Message, state: FSMContext):
    await router.controller.command_start(message, state)

@router.message(Command('help'))
async def help_command(message: types.Message, state: FSMContext):
    await router.controller.command_help(message, state)

@router.message(StateFilter(PlayerStates.choose_game))
async def play_command(message: types.Message, state: FSMContext):
    await router.controller.state_choose_game(message, state)

@router.message(StateFilter(PlayerStates.choose_bot_difficulty))
async def handle_bot_difficulty(message: types.Message, state: FSMContext):
    await router.controller.state_choose_bot_difficulty(message, state)

@router.message(StateFilter(PlayerStates.main_menu_state))
async def handle_main_menu(message: types.Message, state: FSMContext):
    await router.controller.state_main_menu(message, state)

@router.message(StateFilter(PlayerStates.waiting_for_number))
async def handle_number_input(message: types.Message, state: FSMContext):
    await router.controller.state_game_step(message, state)

@router.message(StateFilter(PlayerStates.waiting_a_rival))
async def handle_waiting_a_rival(message: types.Message, state: FSMContext):
    await router.controller.state_waiting_a_rival(message, state)

@router.message(StateFilter(PlayerStates.lang_state))
async def handle_lang(message: types.Message, state: FSMContext):
    await router.controller.state_lang(message, state)

@router.message(StateFilter(PlayerStates.feedback_state))
async def handle_feedback(message: types.Message, state: FSMContext):
    await router.controller.state_feedback(message, state)

@router.callback_query(F.data == "full_game")
async def handle_full_game(callback: types.CallbackQuery, state: FSMContext):
    await router.controller.callback_full_game(callback, state)

@router.callback_query(F.data == "to_menu")
async def handle_to_menu(callback: types.CallbackQuery, state: FSMContext):
    await router.controller.callback_to_menu(callback, state)

@router.message()
async def unhandled_message(message: types.Message, state: FSMContext):
    await router.controller.command_start(message, state)
