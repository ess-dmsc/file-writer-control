from datetime import datetime
from streaming_data_types import serialise_pl72
import platform
import os


def fletcher32(string) -> int:
    a = list(map(ord, string))
    b = [sum(a[:i]) % 65535 for i in range(len(a)+1)]
    return ((sum(b) << 16) | max(b)) & 0xffffffff


def generate_job_id() -> str:
    timestamp = int(datetime.now().timestamp())
    timestamp_str = "%0.2X" % timestamp
    partial_id = "{}-{}-{}-".format(platform.node(), os.getpid(), timestamp_str)
    return partial_id + "%0.8X" % fletcher32(partial_id)


class WriteJob:
    def __init__(self, nexus_structure: str, file_name: str, broker: str, start_time: datetime, stop_time=datetime.max):
        self.structure = nexus_structure
        self.file = file_name
        self.job_id = generate_job_id()
        self.start = start_time
        self.stop = stop_time
        self._service_id = ""
        self.broker = broker

    def generate_new_job_id(self):
        self.job_id = generate_job_id()

    @property
    def service_id(self) -> str:
        return self._service_id

    @service_id.setter
    def service_id(self, new_service_id: str):
        self._service_id = new_service_id

    def get_start_message(self) -> bytes:
        return serialise_pl72(self.job_id, self.file, int(self.start.timestamp() * 1000), int(self.stop.timestamp() * 1000), nexus_structure=self.structure, service_id=self.service_id, broker=self.broker)

