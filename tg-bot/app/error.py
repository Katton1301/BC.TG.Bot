from enum import Enum, auto
from dataclasses import dataclass
from typing import Optional

class ErrorLevel(Enum):
    INFO = auto()
    WARNING = auto()
    GAME_ERROR = auto()
    LOBBY_ERROR = auto()
    ERROR = auto()
    CRITICAL = auto()

@dataclass
class Error:
    level: ErrorLevel
    message: str
    game_id: Optional[int] = None
    lobby_id: Optional[int] = None

    def __init__(self, level: ErrorLevel, message: str):
        self.level = level
        self.message = message

    def __str__(self):
        return self.message

    def setLobbyId(self, _id):
        self.lobby_id = _id

    def setGameId(self, _id):
        self.game_id = _id

    def Level(self):
        return self.level

    def Message(self):
        return self.message
