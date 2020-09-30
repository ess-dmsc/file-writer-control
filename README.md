# File-writer-control
This is a small library for controlling the [ESS HDF5/NeXus file-writer application](https://github.com/ess-dmsc/kafka-to-nexus). The file-writer is controlled by sending commands to it via an Apache Kafka broker. This library implements the encoding/decoding of commands as well as an abstraction of this interface in order to simplify the commands and control.

## Getting started

Documentation on the usage of this library can be found in the _examples_ directory in the root of this repository. For getting started, look no further than the following code snippet:

```python
from file_writer_control.WorkerCommandChannel import WorkerCommandChannel
from file_writer_control.WriteJob import WriteJob
from file_writer_control.JobHandler import JobHandler
from datetime import datetime, timedelta
import time

command_channel = WorkerCommandChannel("dmsc-kafka01:9092/command_topic")
job_handler = JobHandler(worker_finder=command_channel)
write_job = WriteJob(nexus_structure="{...}", "file.nxs", "dmsc-kafka01:9092", datetime.now())
start_handler = job_handler.start_job(write_job)
while not start_handler.is_done():
    time.sleep(1)
stop_handler = job_handler.set_stop_time(datetime.now() + timedelta(seconds=60))
while not stop_handler.is_done():
    time.sleep(1)
while not write_job.is_done():
    time.sleep(1)
print("Write job is done")
```

## Installing dependencies

This library uses the [_kafka-python_](https://kafka-python.readthedocs.io/en/master/index.html) library for the communication with the Kafka broker and the [_python-streaming-data-types_](https://github.com/ess-dmsc/python-streaming-data-types) for serialising and de-serialising messages to and from the filewriter. These dependencies can be installed by executing the following command in the root of the repository:

```bash
pip install -r requirements.txt
```

Alternatively, to install the dependencies such that they are only available to the current user, execute the following command:

```bash
pip install --user -r requirements.txt
```

**Note:** At the time of this writing, the _python-streaming-data-types__ library version required by this library has not been released. If you want to use this library, you will have to manually (and locally) install the *filewriter_ctrl* branch of that library.

**Also note:** This library was developed using Python 3.8 but it is likely that it will work with Python 3.6 and up.

## Running the unit tests
For running the unit tests, execute the following command in the root of this repository:

```bash
python -m pytest -s .
```

## Installing the development version locally

First, uninstall any existing versions of this library:

```bash
pip uninstall file-writer-control
```

Then, from the *file-writer-control* root directory, run the following command:

```bash
pip install --user -e ./
```

