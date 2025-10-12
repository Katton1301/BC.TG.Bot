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
    choose_lobby_creation_type = State()
    enter_lobby_id = State()
    enter_password = State()

def isFreeState(state: State) -> bool:
    return state == PlayerStates.free_state or \
     state == PlayerStates.main_menu_state or \
        state == PlayerStates.lang_state or \
        state == PlayerStates.feedback_state or \
        state == PlayerStates.choose_game or \
        state == PlayerStates.choose_bot_difficulty or \
        state == PlayerStates.choose_lobby_type or \
        state == PlayerStates.choose_lobby_creation_type
