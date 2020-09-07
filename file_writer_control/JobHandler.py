from filewriter_control.WorkerFinder import WorkerFinder
from filewriter_control.WriteJob import WriteJob
from filewriter_control.CommandHandler import CommandHandler
from filewriter_control.Worker import WorkerState
from datetime import datetime


class JobHandler:
    def __init__(self, worker_finder: WorkerFinder):
        self.worker_finder = worker_finder

    def start_job(job: WriteJob) -> CommandHandler:
        pass

    def get_state(self) -> WorkerState:
        pass

    def error_string(self) -> str:
        pass

    def set_stop_time(self, stop_time: datetime) -> CommandHandler:
        pass

    def stop_now(self) -> CommandHandler:
        pass
