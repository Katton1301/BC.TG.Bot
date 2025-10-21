from aiogram.types import (ReplyKeyboardMarkup, KeyboardButton,
                           InlineKeyboardMarkup, InlineKeyboardButton)
from phrases import phrases

main = dict()
game = dict()
bot = dict()
lang = dict()
full_game_menu = dict()
full_game_menu_with_lobby = dict()
lobby = dict()
lobby_types = dict()
lobby_creation_types = dict()
exit_or_not = dict()

def create_keyboards():
    for _lang in phrases.langs():
        main[_lang] = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=phrases.dict('game', _lang))],
                [KeyboardButton(text=phrases.dict('rules', _lang))],
                [KeyboardButton(text=phrases.dict('lang', _lang))],
                [KeyboardButton(text=phrases.dict('feedback', _lang))]
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        game[_lang] =  ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=phrases.dict('singlePlay', _lang)), KeyboardButton(text=phrases.dict('botPlay', _lang))],
                [KeyboardButton(text=phrases.dict('randomPlay', _lang))],
                [KeyboardButton(text=phrases.dict('createLobby', _lang)), KeyboardButton(text=phrases.dict('enterLobby', _lang))],
                [KeyboardButton(text=phrases.dict('back', _lang))],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        bot[_lang] =  ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=phrases.dict('easy', _lang))],
                [KeyboardButton(text=phrases.dict('medium', _lang))],
                [KeyboardButton(text=phrases.dict('hard', _lang))],
                [KeyboardButton(text=phrases.dict('back', _lang))],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )

        lobby[_lang] = list()
        lobby[_lang].append(ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=phrases.dict('ready', _lang))],
                [KeyboardButton(text=phrases.dict('leaveLobby', _lang))],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        ))
        lobby[_lang].append(ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=phrases.dict('notReady', _lang))],
                [KeyboardButton(text=phrases.dict('leaveLobby', _lang))],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        ))
        lobby[_lang].append(ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=phrases.dict('startGame', _lang))],
                [KeyboardButton(text=phrases.dict('ready', _lang))],
                [KeyboardButton(text=phrases.dict('banPlayer', _lang))],
                [KeyboardButton(text=phrases.dict('leaveLobby', _lang))],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        ))
        lobby[_lang].append(ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=phrases.dict('startGame', _lang))],
                [KeyboardButton(text=phrases.dict('notReady', _lang))],
                [KeyboardButton(text=phrases.dict('banPlayer', _lang))],
                [KeyboardButton(text=phrases.dict('leaveLobby', _lang))],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        ))

        lobby_types[_lang] = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=phrases.dict('enterByLobbyId', _lang))],
                [KeyboardButton(text=phrases.dict('enterRandomPublicLobby', _lang))],
                [KeyboardButton(text=phrases.dict('back', _lang))],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )

        lobby_creation_types[_lang] = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=phrases.dict('createPrivateLobby', _lang))],
                [KeyboardButton(text=phrases.dict('createPublicLobby', _lang))],
                [KeyboardButton(text=phrases.dict('back', _lang))],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )

        full_game_menu[_lang] = InlineKeyboardMarkup(
            inline_keyboard=[
                [ InlineKeyboardButton(text=phrases.dict('fullGame', _lang), callback_data="full_game_menu"), ],
                [ InlineKeyboardButton(text=phrases.dict('toMenu', _lang), callback_data="to_menu"), ],
            ]
        )

        full_game_menu_with_lobby[_lang] = InlineKeyboardMarkup(
            inline_keyboard=[
                [ InlineKeyboardButton(text=phrases.dict('fullGame', _lang), callback_data="full_game_menu"), ],
                [ InlineKeyboardButton(text=phrases.dict('stayInLobby', _lang), callback_data="stay_in_lobby"), ],
                [ InlineKeyboardButton(text=phrases.dict('toMenu', _lang), callback_data="to_menu"), ],
            ]
        )

        exit_or_not[_lang] = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text=phrases.dict('stay', _lang))],
                [KeyboardButton(text=phrases.dict('exit', _lang))],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )

        keyboard_langs = []
        for lng in phrases.langs():
            keyboard_langs.append([KeyboardButton(text=phrases.dict('name', lng))])
        keyboard_langs.append([KeyboardButton(text=phrases.dict('back', _lang))])
        lang[_lang] = ReplyKeyboardMarkup(
            keyboard=keyboard_langs,
            resize_keyboard=True,
            one_time_keyboard=True
        )

def get_lobby_keyboard(isHost, isReady, _lang):
    return lobby[_lang][isHost * 2 + isReady]

def get_ban_player_keyboard(lobby_players, lobby_names, _lang):
    keyboard = []
    buttons_row = []

    for i, player in enumerate(lobby_players):
        player_name = next((name['name'] for name in lobby_names if name['id'] == player['player_id']), f"Player {i+1}")

        buttons_row.append(KeyboardButton(text=player_name))

        if len(buttons_row) == 2:
            keyboard.append(buttons_row)
            buttons_row = []

    if buttons_row:
        keyboard.append(buttons_row)

    keyboard.append([KeyboardButton(text=phrases.dict('back', _lang))])

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        one_time_keyboard=True
    )