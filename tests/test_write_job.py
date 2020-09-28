from file_writer_control.WriteJob import generate_job_id, WriteJob
import re
import platform
import os
from math import fabs
import time
from zlib import adler32
from datetime import datetime
from copy import copy


def test_job_id():
    under_test = generate_job_id()
    used_re = re.compile("((.*)-(\\d+)-([A-F0-9]{8})-([A-F0-9]{4})-)([A-F0-9]{8})")
    match_res = re.match(used_re, under_test)
    assert match_res.group(2) == platform.node()
    assert int(match_res.group(3)) == os.getpid()
    assert fabs(time.time() - (int("0x" + match_res.group(4), 0) ^ 0xFFFFFFFF)) <= 1.0
    assert "{:8X}".format(adler32(match_res.group(1).encode())) == match_res.group(6)


def test_write_generate_job_id():
    under_test = WriteJob("", "", "", datetime.now())
    old_job_id = copy(under_test.job_id)
    under_test.generate_new_job_id()
    assert old_job_id != under_test.job_id


def test_get_start_message():
    under_test = WriteJob("", "", "", datetime.now())
    assert type(under_test.get_start_message()) is bytes
