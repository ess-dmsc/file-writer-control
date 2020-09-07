from datetime import datetime


def generate_job_id() -> str:
    return "some_random_id"


class WriteJob:
    def __init__(self, nexus_structure: str, file_name: str, start_time: datetime, stop_time=datetime.max):
        self.structure = nexus_structure
        self.file = file_name
        self.job_id = generate_job_id()
        self.start = start_time
        self.stop = stop_time
        self.service_id = ""

    @property
    def service_id(self) -> str:
        return self.service_id

    @service_id.setter
    def service_id(self, new_service_id: str):
        self.service_id = new_service_id

