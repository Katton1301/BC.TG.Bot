from aiogram.fsm.state import State, StatesGroup

class PlayerStates(StatesGroup):
    free_state = State()
    main_menu_state = State()
    lang_state = State()
    feedback_state = State()
    choose_game = State()
    choose_bot_difficulty = State()
    waiting_a_rival = State()
    waiting_for_number = State()
    wait_password = State()
    in_lobby = State()
    choose_lobby_type = State()
    enter_lobby_id = State()
    enter_password = State()