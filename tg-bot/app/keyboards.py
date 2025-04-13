from aiogram.types import (ReplyKeyboardMarkup, KeyboardButton,
                           InlineKeyboardMarkup, InlineKeyboardButton)
from phrases import phrases

main = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=phrases.dict('game', 'ru'))],
        [KeyboardButton(text=phrases.dict('rules', 'ru'))],
        [KeyboardButton(text=phrases.dict('lang', 'ru'))],
        [KeyboardButton(text=phrases.dict('feedback', 'ru'))]
    ],
    resize_keyboard=True,
    one_time_keyboard=True
)

game =  ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=phrases.dict('snglePlay', 'ru')), KeyboardButton(text=phrases.dict('botPlay', 'ru'))],
        [KeyboardButton(text=phrases.dict('randomPlay', 'ru'))],
        [KeyboardButton(text=phrases.dict('createLobby', 'ru')), KeyboardButton(text=phrases.dict('enterLobby', 'ru'))],
    ],
    resize_keyboard=True,
    one_time_keyboard=True
)