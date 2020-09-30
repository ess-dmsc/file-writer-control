from file_writer_control.WorkerCommandChannel import WorkerCommandChannel
import time


def print_current_state(channel: WorkerCommandChannel):
    print("Known workers")
    print("{:30s}{:30s}".format("Service id", "Current state"))
    print("-" * 80)
    for worker in channel.list_known_workers():
        print("{:30s}{:30s}".format(worker.service_id, worker.state))

    print("\nKnown jobs")
    print("{:30s}{:30s}{:30s}".format("Service id", "Job id", "Current state"))
    print("-" * 80)
    for job in channel.list_known_jobs():
        print("{:30s}{:30s}{:30s}".format(job.service_id, job.job_id, job.state))
        if len(job.error_message) > 0:
            print("    Message: {}".format(job.error_message))

    print("\nKnown commands")
    print("{:30s}{:30s}{:30s}".format("Job id", "Command id", "Current state"))
    print("-" * 80)
    for command in channel.list_known_commands():
        print("{:26}{:26}{:26}".format(command.job_id, command.command_id, command.state))
        if len(command.error_message) > 0:
            print("    Message: {}".format(command.error_message))


if __name__ == "__main__":
    kafka_host = "dmsc-kafka01:9092"
    command_channel = WorkerCommandChannel("{}/command_topic".format(kafka_host))
    time.sleep(5)
    print_current_state(command_channel)
