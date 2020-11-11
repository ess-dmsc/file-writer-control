from enum import Enum, auto
from datetime import datetime


class CommandState(Enum):
    """
    The state of a command.
    """

    UNKNOWN = auto()
    NO_COMMAND = auto()
    WAITING_RESPONSE = auto()
    TIMEOUT_RESPONSE = auto()
    ERROR = auto()
    SUCCESS = auto()


class CommandStatus(object):
    """
    The status of a command.
    """

    def __init__(self, job_id: str, command_id: str):
        self._job_id = job_id
        self._command_id = command_id
        self._last_update = datetime.now()
        self._state = CommandState.NO_COMMAND
        self._message = ""

    def __eq__(self, other_status: "CommandStatus"):
        if not isinstance(other_status, CommandStatus):
            raise NotImplementedError
        return (
            other_status.command_id == self.command_id
            and other_status.job_id == self.job_id
            and other_status.state == self.state
        )

    def update_status(self, new_status: "CommandStatus"):
        """
        Updates the status/state of this instance of the CommandStatus class using another instance.
        .. note:: The command identifier of both this instance and the other one must be identical.
        :param new_status: The other instance of the CommandStatus class.
        """
        if new_status.command_id != self.command_id:
            raise RuntimeError(
                f"Command id of status update is not correct ({self.command_id} vs {new_status.command_id})"
            )
        self._state = new_status.state
        if new_status.message:
            self._message = new_status.message
        self._last_update = new_status.last_update

    @property
    def job_id(self) -> str:
        """
        The job identifier under which this command is executed.
        """
        return self._job_id

    @property
    def command_id(self) -> str:
        """
        The unique command identifier of this command.
        """
        return self._command_id

    @property
    def message(self) -> str:
        """
        A status/error message as returned by the file-writer that responded to the command.
        :return:
        """
        return self._message

    @message.setter
    def message(self, new_message: str):
        if new_message:
            self._message = new_message
            self._last_update = datetime.now()

    @property
    def state(self) -> CommandState:
        """
        The current state of the command.
        """
        return self._state

    @state.setter
    def state(self, new_state: CommandState):
        self._state = new_state
        self._last_update = datetime.now()

    @property
    def last_update(self) -> datetime:
        """
        The local time stamp of the last update of the status of the command.
        """
        return self._last_update
