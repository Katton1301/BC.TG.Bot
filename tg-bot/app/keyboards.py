from aiogram.types import (ReplyKeyboardMarkup, KeyboardButton,
                           InlineKeyboardMarkup, InlineKeyboardButton)
from phrases import phrases

main = dict()
game = dict()

for lang in phrases.langs():
    main[lang] = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=phrases.dict('game', lang))],
            [KeyboardButton(text=phrases.dict('rules', lang))],
            [KeyboardButton(text=phrases.dict('lang', lang))],
            [KeyboardButton(text=phrases.dict('feedback', lang))]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

    game[lang] =  ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=phrases.dict('singlePlay', lang)), KeyboardButton(text=phrases.dict('botPlay', lang))],
            [KeyboardButton(text=phrases.dict('randomPlay', lang))],
            [KeyboardButton(text=phrases.dict('createLobby', lang)), KeyboardButton(text=phrases.dict('enterLobby', lang))],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )