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
    partial_id = f"{platform.node():s}-{os.getpid():d}-{command_name:s}-{timestamp ^ 0xFFFFFFFF:08X}-{randint(0, 65535):04X}-"
    return partial_id + f"{adler32(partial_id.encode()):08X}"
