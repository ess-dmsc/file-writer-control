from enum import Enum, auto


class WorkerState(Enum):
    NO_JOB = auto()
    WRITING = auto()
    WRITING_TIMEOUT = auto()
    ERROR = auto()
    STOPPED = auto()
    UNAVAILABLE = auto()


class Worker:
    def __init__(self, service_id: str):
        pass

    def get_current_state(self) -> WorkerState:
        pass

    def get_current_job_id(self) -> str:
        pass

