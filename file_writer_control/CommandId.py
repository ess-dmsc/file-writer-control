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
    timestamp_str = "{:08X}".format(timestamp ^ 0xFFFFFFFF)
    partial_id = "{host:s}-{pid:d}-{command:s}-{time:s}-{random:s}-".format(
        host=platform.node(),
        pid=os.getpid(),
        time=timestamp_str,
        random="{:04X}".format(randint(0, 65535)),
        command=command_name,
    )
    return partial_id + "{:08X}".format(adler32(partial_id.encode()))
