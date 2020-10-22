from datetime import datetime, timedelta
from streaming_data_types import serialise_pl72
import platform
import os
from zlib import adler32
from random import randint


def generate_job_id() -> str:
    timestamp = int(datetime.now().timestamp())
    partial_id = "{:s}-{:d}-{:08X}-{:04X}-".format(
        platform.node(), os.getpid(), timestamp ^ 0xFFFFFFFF, randint(0, 65535)
    )
    return partial_id + "{:08X}".format(adler32(partial_id.encode()))


class WriteJob:
    """
    Represents a file-writer write job (before it has been started).
    """

    def __init__(
        self,
        nexus_structure: str,
        file_name: str,
        broker: str,
        start_time: datetime,
        stop_time: datetime = None,
        instrument_name: str = "",
        run_name: str = "",
        metadata: str = "",
    ):
        self.structure = nexus_structure
        self.file = file_name
        self.job_id = generate_job_id()
        self.start = start_time
        if stop_time is None:
            self.stop = self.start + timedelta(days=365.25 * 10)
        else:
            self.stop = stop_time
        self._service_id = ""
        self.broker = broker
        self.instrument_name = instrument_name
        self.run_name = run_name
        self.metadata = metadata

    def generate_new_job_id(self):
        """
        Generate a new job id. Should be called if an attempt at starting this write job fails and another attempt is made.
        """
        self.job_id = generate_job_id()

    @property
    def service_id(self) -> str:
        """
        The service identifier that should process this job. Defaults to an empty string.
        .. note:: Must be set if job is to be processed by a specific file-writer instance.
        """
        return self._service_id

    @service_id.setter
    def service_id(self, new_service_id: str):
        self._service_id = new_service_id

    def get_start_message(self) -> bytes:
        """
        Generate the (flatbuffer) start message that will start this job. If you are sending the message to a specific
        file-writer instance, you have to set the service_id property first or the message will fail to start the job.
        :return: A flatbuffer message that holds the necessary information for starting a write job.
        """
        return serialise_pl72(
            self.job_id,
            self.file,
            self.start,
            self.stop,
            nexus_structure=self.structure,
            service_id=self.service_id,
            broker=self.broker,
            instrument_name=self.instrument_name,
            run_name=self.run_name,
            metadata=self.metadata,
        )
