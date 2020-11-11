# Usage examples

## Principles of operation

The library allows for more or less direct communication with the file-writer using the various helper classes. However, this is not how the library was designed to be used. Instead, communication is abstracted into the following hierarchy:

1. There is a pool of file-writers.
2. The classes `WorkerCommandChannel` and `WorkerJobPool` are the interfaces for finding and communicating with the available file-writers.
3. For interfacing with individual write jobs, the class `JobHandler` is used.
4. An instance of the class `WriteJob` contains all the requried information for starting a write job.
5. When sending a command to a file-writer (through a `JobHandler`), an instance of `CommandHandler` is used to check for the failure or success of that specific command.

## Writing a file using direct communication

There are two ways for sending a write job to a file-writer. The first one is by sending the job to a specific file-writer instance (with a unique service id). The library can be used to pick a file-writer from a list of available ones automatically. This is done with the use of the `WorkerCommandChannel` class. The following piece of code illustrates this (also in the file [*write_job_1.py*](write_job_1.py)).

```python
from file_writer_control.WorkerCommandChannel import WorkerCommandChannel
from file_writer_control.WriteJob import WriteJob
from file_writer_control.JobHandler import JobHandler
from datetime import datetime
import time

command_channel = WorkerCommandChannel("dmsc-kafka01:9092/command_topic")
job_handler = JobHandler(worker_finder=command_channel)
job_handler.start_job(WriteJob(nexus_structure="{...}", "file.nxs", "dmsc-kafka01:9092", datetime.now()))
```

## Write a file using indirect communication

Alternatively, we can start a job by having a file-writer instance that is currently idle pick up the job. This is done by sending the file-writing job to a "job pool" topic. This can be done with the use of the `WorkerJobPool` class as follows (also in [*write_job_2.py*](write_job_2.py)):

```python
from file_writer_control.WorkerJobPool import WorkerJobPool
from file_writer_control.WriteJob import WriteJob
from file_writer_control.JobHandler import JobHandler
from datetime import datetime

worker_pool = WorkerJobPool("dmsc-kafka01:9092/job_pool_topic", "dmsc-kafka01:9092/command_topic")
job_handler = JobHandler(worker_finder=worker_pool)
job_handler.start_job(WriteJob(nexus_structure="{...}", "file.nxs", "dmsc-kafka01:9092", datetime.now()))
```

## Find which workers are available

It is possible to list currently known workers using the `WorkerCommandChannel` and `WorkerJobPool` classes. This is done as follows (also in [*list_status.py*](list_workers.py)):

```python
from file_writer_control.WorkerCommandChannel import WorkerCommandChannel
import time

command_channel = WorkerCommandChannel("dmsc-kafka01:9092/command_topic")
time.sleep(5) # Sleep for a while so that we can listen for workers on the topic
known_workers = command_channel.list_known_workers()
for worker in known_workers:
    print(f"Worker id: {worker.service_id:15s} Current state: {worker.state}")
```

**Note:** Due to the use of Kafka for communicating with the file-writers, the `sleep()` statement is required such that we can listen for the file-writers to announce their presence. With out it, the library will likely list 0 file-writers.
