from file_writer_control.WorkerFinder import WorkerFinder
from file_writer_control.WriteJob import WriteJob
from file_writer_control.CommandHandler import CommandHandler
from datetime import datetime
from file_writer_control.JobStatus import JobState


class JobHandler:
    def __init__(self, worker_finder: WorkerFinder):
        self.worker_finder = worker_finder
        self.job_id = ""

    def start_job(self, job: WriteJob) -> CommandHandler:
        self.job_id = job.job_id
        return self.worker_finder.try_start_job(job)

    def get_state(self) -> JobState:
        return self.worker_finder.get_job_state(self.job_id)

    def error_string(self) -> str:
        current_status = self.worker_finder.get_job_status(self.job_id)
        if current_status is None:
            return ""
        return current_status.error_message

    def set_stop_time(self, stop_time: datetime) -> CommandHandler:
        current_status = self.worker_finder.get_job_status(self.job_id)
        if current_status is None:
            return None
        return self.worker_finder.try_send_stop_time(
            current_status.service_id, self.job_id, stop_time
        )

    def stop_now(self) -> CommandHandler:
        current_status = self.worker_finder.get_job_status(self.job_id)
        if current_status is None:
            return None
        return self.worker_finder.try_send_stop_now(
            current_status.service_id, self.job_id
        )
