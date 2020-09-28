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
        self.job_id = job_id
        self.command_id = command_id
        self.last_update = datetime.now()
        self.state = CommandState.NO_COMMAND
        self.error_message = ""

    def __eq__(self, other):
        if not isinstance(other, CommandStatus):
            raise NotImplementedError
        return other.command_id == self.command_id and other.job_id == self.job_id and other.state == self.state

    def update_status(self, new_status):
        if new_status.command_id != self.command_id:
            raise RuntimeError("Command id of status update is not correct ({} vs {})".format(self.command_id, new_status.command_id))
        self.state = new_status.state
        if len(new_status.error_message) != 0:
            self.error_message = new_status.error_message
        self.last_update = new_status.last_update

