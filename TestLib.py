from file_writer_control.WorkerFinder import WorkerCommandChannel
from file_writer_control.JobHandler import JobHandler
from file_writer_control.WriteJob import WriteJob
from file_writer_control.CommandHandler import CommandState
from datetime import datetime, timedelta


if __name__ == "__main__":
    cmd_topic = "dmsc-kafka01:9092/test_command_topic"
    cmdChannel = WorkerCommandChannel(cmd_topic)
    jobHandler = JobHandler(cmdChannel)
    nexus_structure = open("filewriter_config.json", "r").read()
    job = WriteJob(nexus_structure, file_name="some_file8.nxs", start_time=datetime.now(), stop_time=datetime.now() + timedelta(seconds=60), broker="dmsc-kafka01:9092")
    commandHandler = jobHandler.start_job(job)
    import time
    while True:
        time.sleep(2)
        known_workers = cmdChannel.list_known_workers()
        print("-"*20)
        print("-- Start job status: {}".format(commandHandler.get_state()))
        if commandHandler.get_state() == CommandState.ERROR:
            print("---- Error message: {}".format(commandHandler.get_error_string()))
        print("-- Known workers")
        for worker in known_workers:
            print("   {} : {}".format(worker.service_id, worker.state))
        print("-- Known jobs")
        for job in cmdChannel.list_known_jobs():
            print("   {} : {} : {}".format(job.service_id, job.job_id, job.state))

        # print("Start job status: {}".format(commandHandler.get_state()))
        # if commandHandler.get_state() == CommandState.ERROR:
        #     print("Error string was: {}".format(commandHandler.get_error_string()))

