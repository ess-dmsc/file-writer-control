from enum import Enum, auto


class CommandState(Enum):
    NO_COMMAND = auto()
    WAITING_RESPONSE = auto()
    TIMEOUT_RESPONSE = auto()
    ERROR = auto()
    SUCCESS = auto()


class CommandHandler:
    def get_state(self) -> CommandState:
        pass

    def get_error_string(self) -> str:
        pass

