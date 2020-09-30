from file_writer_control.WorkerCommandChannel import WorkerCommandChannel
import time

def print_current_state(command_channel):
    print("Known workers")
    print("{:30s}{:30s}".format("Service id", "Current state"))
    print("-" * 80)
    for worker in command_channel.list_known_workers():
        print("{:30s}{:30s}".format(worker.service_id, worker.state))

    print("Known jobs")
    print("{:30s}{:30s}{:30s}".format("Service id", "Job id", "Current state"))
    print("-" * 80)
    for job in command_channel.list_known_jobs():
        print("{:30s}{:30s}{:30s}".format(job.service_id, job.job_id, job.state))
        if len(job.error_message) > 0:
            print("    Message: {}".format(job.error_message))

    print("Known commands")
    print("{:30s}{:30s}{:30s}{:30s}".format("Service id", "Job id", "Command id", "Current state"))
    print("-" * 80)
    print("{:30s}{:30s}{:30s}".format(job.service_id, job.job_id, job.state))


for job in command_channel.list():
    if len(job.error_message) > 0:
        print("    Message: {}".format(job.error_message))

if __name__ == "__main__":
    kafka_host = "dmsc-kafka01:9092"
    command_channel = WorkerCommandChannel("{}/command_topic".format(kafka_host))
    time.sleep(5)
    print_current_state(command_channel)
