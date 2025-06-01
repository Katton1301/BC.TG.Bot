from aiogram.types import (ReplyKeyboardMarkup, KeyboardButton,
                           InlineKeyboardMarkup, InlineKeyboardButton)
from phrases import phrases

main = dict()
game = dict()
bot = dict()
lang = dict()

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
    
        keyboard_langs = []
        for lng in phrases.langs():
            keyboard_langs.append([KeyboardButton(text=phrases.dict('name', lng))])
        keyboard_langs.append([KeyboardButton(text=phrases.dict('back', _lang))])
        lang[_lang] = ReplyKeyboardMarkup(
            keyboard=keyboard_langs,
            resize_keyboard=True,
            one_time_keyboard=True
        )
