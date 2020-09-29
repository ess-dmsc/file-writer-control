from file_writer_control.CommandId import generate_command_id
import re
from math import fabs
import platform
import os
from zlib import adler32
import time


def test_command_id():
    cmd_name = "STOP"
    under_test = generate_command_id(cmd_name)
    used_re = re.compile("((.*)-(\\d+)-(.*)-([A-F0-9]{8})-([A-F0-9]{4})-)([A-F0-9]{8})")
    match_res = re.match(used_re, under_test)
    assert match_res.group(2) == platform.node()
    assert match_res.group(4) == cmd_name
    assert int(match_res.group(3)) == os.getpid()
    assert fabs(time.time() - (int("0x" + match_res.group(5), 0) ^ 0xFFFFFFFF)) <= 1.0
    assert "{:8X}".format(adler32(match_res.group(1).encode())) == match_res.group(7)
