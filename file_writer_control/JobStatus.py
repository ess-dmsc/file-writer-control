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
        self.job_id = job_id
        self.service_id = ""
        self.last_update = datetime.now()
        self.state = JobState.WAITING
        self.error_message = ""

    def update_status(self, new_status):
        if new_status.job_id != self.job_id:
            raise RuntimeError(
                "Job id of status update is not correct ({} vs {})".format(
                    self.job_id, new_status.job_id
                )
            )
        self.last_update = new_status.last_update
        self.state = new_status.state
        if len(new_status.error_message) != 0:
            self.error_message = new_status.error_message
        self.service_id = new_status.service_id
