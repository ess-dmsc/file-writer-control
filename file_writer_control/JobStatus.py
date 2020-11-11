from enum import Enum, auto
from datetime import datetime


class JobState(Enum):
    """
    The state of a job.
    """

    NO_JOB = auto()
    WAITING = auto()
    WRITING = auto()
    TIMEOUT = auto()
    ERROR = auto()
    DONE = auto()
    UNAVAILABLE = auto()


class JobStatus:
    """
    Contains general information about the (execution) of a job.
    """

    def __init__(self, job_id: str):
        self._job_id = job_id
        self._service_id = ""
        self._last_update = datetime.now()
        self._state = JobState.WAITING
        self._message = ""

    def update_status(self, new_status: "JobStatus") -> None:
        """
        Updates the status/state of a this instance of the JobStatus class, using another instance.
        .. note:: The job identifier of this instance and the other must be identical.
        :param new_status: The other instance of the JobStatus class.
        """
        if new_status.job_id != self.job_id:
            raise RuntimeError(
                f"Job id of status update is not correct ({self.job_id} vs {new_status.job_id})"
            )
        self._state = new_status.state
        if new_status.message:
            self._message = new_status.message
        self._service_id = new_status.service_id
        self._last_update = new_status.last_update

    @property
    def job_id(self) -> str:
        """
        The (unique) job identifier.
        """
        return self._job_id

    @property
    def service_id(self) -> str:
        """
        The (unique) service identifier of the instance of the file-writer that executes the current job.
        """
        return self._service_id

    @service_id.setter
    def service_id(self, new_service_id: str) -> None:
        if not self._service_id:
            self._service_id = new_service_id
            self._last_update = datetime.now()
        elif self._service_id == new_service_id:
            return
        else:
            raise RuntimeError("Can not set service_id. It has already been set.")

    @property
    def last_update(self) -> datetime:
        """
        The local time stamp of the last update of the status of the job.
        """
        return self._last_update

    @property
    def state(self) -> JobState:
        """
        The current state of the job.
        """
        return self._state

    @state.setter
    def state(self, new_state: JobState) -> None:
        self._state = new_state
        self._last_update = datetime.now()

    @property
    def message(self) -> str:
        """
        Status/state message of the job as received from the file-writer.
        """
        return self._message

    @message.setter
    def message(self, new_message: str) -> None:
        if new_message:
            self._message = new_message
            self._last_update = datetime.now()
