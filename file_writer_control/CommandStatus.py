from enum import Enum, auto
from datetime import datetime


class CommandState(Enum):
    UNKNOWN = auto()
    NO_COMMAND = auto()
    WAITING_RESPONSE = auto()
    TIMEOUT_RESPONSE = auto()
    ERROR = auto()
    SUCCESS = auto()


class CommandStatus(object):
    def __init__(self, job_id: str, command_id: str):
        self._job_id = job_id
        self._command_id = command_id
        self._last_update = datetime.now()
        self._state = CommandState.NO_COMMAND
        self._message = ""

    def __eq__(self, other):
        if not isinstance(other, CommandStatus):
            raise NotImplementedError
        return (
            other.command_id == self.command_id
            and other.job_id == self.job_id
            and other.state == self.state
        )

    def update_status(self, new_status):
        if new_status.command_id != self.command_id:
            raise RuntimeError(
                "Command id of status update is not correct ({} vs {})".format(
                    self.command_id, new_status.command_id
                )
            )
        self._state = new_status.state
        if new_status.message:
            self._message = new_status.error_message
        self._last_update = new_status.last_update

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def command_id(self) -> str:
        return self._command_id

    @property
    def message(self) -> str:
        return self._message

    @message.setter
    def message(self, new_message: str):
        if new_message:
            self._message = new_message
            self._last_update = datetime.now()

    @property
    def state(self) -> CommandState:
        return self._state

    @state.setter
    def state(self, new_state: CommandState):
        self._state = new_state
        self._last_update = datetime.now()

    @property
    def last_update(self) -> datetime:
        return self._last_update
