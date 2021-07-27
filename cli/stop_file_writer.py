import argparse
import sys
import time
from datetime import datetime, timedelta
from time import time as current_time

from cli.start_file_writer import is_empty
from file_writer_control import JobHandler, JobState, WorkerCommandChannel


def cli_parser() -> argparse.Namespace:
    """
    Parser for the command line interface.
    """

    fw_parser = argparse.ArgumentParser(
        fromfile_prefix_chars="@", description="FileWriter Stopper"
    )

    fw_parser.add_argument(
        "-s",
        "--stop",
        metavar="stop",
        type=str,
        help="Stop FileWriter immediately.",
    )
    fw_parser.add_argument(
        "-sa",
        "--stop_after",
        metavar="stop_after",
        nargs=2,
        type=str,
        help="Stop FileWriter after a given time in seconds.",
    )
    fw_parser.add_argument(
        "-b",
        "--broker",
        metavar="kafka_broker",
        type=str,
        default="localhost:9092",
        help="Kafka broker port.",
    )
    fw_parser.add_argument(
        "-t",
        "--topic",
        metavar="consume_topic",
        type=str,
        required=True,
        help="Name of the Kafka topic to be consumed.",
    )
    fw_parser.add_argument(
        "--timeout",
        metavar="ack_timeout",
        type=float,
        default=5,
        help="How long to wait for timeout on acknowledgement.",
    )

    args = fw_parser.parse_args()

    return args


def create_job_handler(args, job_id):
    host = args.broker
    topic = args.topic
    command_channel = WorkerCommandChannel(f"{host}/{topic}")
    job_handler = JobHandler(worker_finder=command_channel, job_id=job_id)
    # Required for formation of the handler.
    time.sleep(3)
    return job_handler


def stop_write_job_now(job_handler) -> None:
    while job_handler.get_state() == JobState.WRITING:
        job_handler.stop_now()
        time.sleep(1)
        if job_handler.get_state() == JobState.DONE:
            print("FileWriter successfully stopped.")


def stop_write_job(args, job_handler) -> None:
    stop_time = float(args.stop_after[1])
    timeout = int(current_time()) + args.timeout
    stop_time = datetime.now() + timedelta(seconds=stop_time)
    stop_handler = job_handler.set_stop_time(stop_time)
    while not stop_handler.is_done() and not job_handler.is_done():
        if int(current_time()) > timeout:
            raise ValueError("Timeout.")


def verify_write_job(job_handler):
    if job_handler.get_state() == JobState.WRITING:
        print("The write process is confirmed. Stopping...")
    else:
        raise ValueError(
            "There are no write jobs associated with the "
            "given job id. Please check broker, topic and "
            "id information and try again."
        )


def validate_namespace():
    if cli_args.stop and cli_args.stop_after:
        print(
            "Positional arguments [-s --stop] and [-sa --stop_after] cannot "
            "be used simultaneously."
        )
        sys.exit()
    argument_list = [
        cli_args.stop,
        cli_args.stop_after,
        cli_args.broker,
        cli_args.topic,
    ]
    for arg in argument_list:
        if arg:
            is_empty(arg)


if __name__ == "__main__":
    cli_args = cli_parser()
    validate_namespace()

    _id = cli_args.stop if cli_args.stop else cli_args.stop_after[0]
    handler = create_job_handler(cli_args, _id)
    verify_write_job(handler)

    if cli_args.stop_after:
        stop_write_job(cli_args, handler)
    else:
        stop_write_job_now(handler)
