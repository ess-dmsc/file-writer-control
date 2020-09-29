from datetime import datetime
from enum import Enum, auto


class WorkerState(Enum):
    IDLE = auto()
    WRITING = auto()
    UNKNOWN = auto()
    UNAVAILABLE = auto()


class WorkerStatus(object):
    def __init__(self, service_id: str):
        self.last_update = datetime.min
        self.service_id = service_id
        self.state = WorkerState.UNAVAILABLE

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, WorkerStatus):
            raise NotImplementedError
        return self.service_id == other.service_id and self.state == other.state

    def update_status(self, new_status):
        if new_status.service_id != self.service_id:
            raise RuntimeError(
                "Service id of status update is not correct ({} vs {})".format(
                    self.service_id, new_status.service_id
                )
            )
        self.last_update = new_status.last_update
        self.state = new_status.state
