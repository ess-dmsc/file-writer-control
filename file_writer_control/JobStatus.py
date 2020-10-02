from enum import Enum, auto
from datetime import datetime


class JobState(Enum):
    NO_JOB = auto()
    WAITING = auto()
    WRITING = auto()
    TIMEOUT = auto()
    ERROR = auto()
    DONE = auto()
    UNAVAILABLE = auto()


class JobStatus:
    def __init__(self, job_id: str):
        self._job_id = job_id
        self._service_id = ""
        self._last_update = datetime.now()
        self._state = JobState.WAITING
        self._message = ""

    def update_status(self, new_status):
        if new_status.job_id != self.job_id:
            raise RuntimeError(
                "Job id of status update is not correct ({} vs {})".format(
                    self.job_id, new_status.job_id
                )
            )
        self._state = new_status.state
        if new_status.message:
            self._message = new_status.message
        self._service_id = new_status.service_id
        self._last_update = new_status.last_update

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def service_id(self) -> str:
        return self._service_id

    @service_id.setter
    def service_id(self, new_service_id: str):
        if not self._service_id:
            self._service_id = new_service_id
            self._last_update = datetime.now()
        elif self._service_id == new_service_id:
            return
        else:
            raise RuntimeError("Can not set service_id. It has already been set.")

    @property
    def last_update(self) -> datetime:
        return self._last_update

    @property
    def state(self) -> JobState:
        return self._state

    @state.setter
    def state(self, new_state: JobState):
        self._state = new_state
        self._last_update = datetime.now()

    @property
    def message(self) -> str:
        return self._message

    @message.setter
    def message(self, new_message: str):
        if new_message:
            self._message = new_message
            self._last_update = datetime.now()
