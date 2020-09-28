from datetime import datetime, timedelta
from streaming_data_types import serialise_pl72
import platform
import os
from zlib import adler32
from random import randint


def generate_job_id() -> str:
    timestamp = int(datetime.now().timestamp())
    timestamp_str = "{:08X}".format(timestamp ^ 0xFFFFFFFF)
    partial_id = "{}-{}-{}-{}-".format(platform.node(), os.getpid(), timestamp_str, "{:04X}".format(randint(0, 65535)))
    return partial_id + "{:08X}".format(adler32(partial_id.encode()))


class WriteJob:
    def __init__(self, nexus_structure: str, file_name: str, broker: str, start_time: datetime, stop_time=datetime.max - timedelta(days=365)):
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

