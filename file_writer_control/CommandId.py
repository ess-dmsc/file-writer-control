from zlib import adler32
from datetime import datetime
import os
import platform
from random import randint


def generate_command_id(command_name: str) -> str:
    """
    Generate a (unique) command identifier.
    :param command_name: The "name" of the command. Will be encoded as part of the command identifier.
    :return: The generated command identifier.
    """
    timestamp = int(datetime.now().timestamp())
    partial_id = "{host:s}-{pid:d}-{command:s}-{time:08X}-{random:04X}-".format(
        host=platform.node(),
        pid=os.getpid(),
        command=command_name,
        time=timestamp ^ 0xFFFFFFFF,
        random=randint(0, 65535),
    )
    return partial_id + "{:08X}".format(adler32(partial_id.encode()))
