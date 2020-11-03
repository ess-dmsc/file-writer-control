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
    used_re = re.compile(r"(?P<coding>(?P<host>.*)-(?P<pid>\d+)-(?P<cmd>.*)-(?P<time>[A-F0-9]{8})-(?:[A-F0-9]{4})-)(?P<checksum>[A-F0-9]{8})")
    match_res = re.match(used_re, under_test)
    assert match_res.group("host") == platform.node()
    assert match_res.group("cmd") == cmd_name
    assert int(match_res.group("pid")) == os.getpid()
    assert fabs(time.time() - (int("0x" + match_res.group("time"), 0) ^ 0xFFFFFFFF)) <= 1.0
    assert "{:8X}".format(adler32(match_res.group("coding").encode())) == match_res.group("checksum")
