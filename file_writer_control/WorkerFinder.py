from filewriter_control.Worker import Worker
from filewriter_control.WriteJob import WriteJob


class WorkerFinder:
    def __init__(self):
        pass

    def find_worker_for_job(self, write_job: WriteJob) -> Worker:
        raise NotImplementedError("Not implemented in base class.")


class JobPool(WorkerFinder):
    def __init__(self, job_topic_url: str, command_topic_url: str):
        super(WorkerFinder, self).__init__()
        self.job_topic = job_topic_url
        self.command_topic = command_topic_url

    def find_worker_for_job(self, write_job: WriteJob) -> Worker:
        pass


class CommandChannel(WorkerFinder):
    def __init__(self, command_topic_url: str):
        super(WorkerFinder, self).__init__()
        self.command_topic = command_topic_url
        
    def find_worker_for_job(self, write_job: WriteJob) -> Worker:
        pass
    
    def list_known_workers(self) -> list[Worker]:
        pass


class WorkerInstance(WorkerFinder):
    def __init__(self, worker: Worker):
        self.worker = worker

    def find_worker_for_job(self, write_job: WriteJob) -> Worker:
        return self.worker
