from typing import List
from file_writer_control.CommandHandler import CommandHandler
from file_writer_control.WorkerStatus import WorkerStatus, WorkerState

class Worker:
    def __init__(self, service_id: str):
        self.status = WorkerStatus(service_id)

    def get_commands(self) -> List[CommandHandler]:
        pass

    def get_last_command(self) -> CommandHandler:
        pass

    def get_current_state(self) -> WorkerState:
        return self.status.state

    def get_service_id(self) -> str:
        return self.status.service_id

    def get_current_job_id(self) -> str:
        pass

