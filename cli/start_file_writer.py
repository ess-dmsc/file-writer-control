import argparse
import os
from datetime import datetime
from time import time as current_time

from file_writer_control import JobHandler, WorkerCommandChannel, WriteJob


def cli_parser() -> argparse.Namespace:
    """
    Parser for the command line interface.
    """

    fw_parser = argparse.ArgumentParser(
        fromfile_prefix_chars="@", description="FileWriter Starter"
    )

    fw_parser.add_argument(
        "-f",
        "--filename",
        metavar="filename",
        type=str,
        required=True,
        help="Name of the output file, e.g., `<filename>.nxs`.",
    )
    fw_parser.add_argument(
        "-c",
        "--config",
        metavar="json_config",
        type=str,
        required=True,
        help="Path to JSON config file.",
    )
    fw_parser.add_argument(
        "-b",
        "--broker",
        metavar="kafka_broker",
        type=str,
        required=True,
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
        type=int,
        default=5,
        help="How long to wait for timeout on acknowledgement.",
    )

    args = fw_parser.parse_args()

    return args


def start_file_writer(args: argparse.Namespace) -> None:
    file_name = args.filename
    host = args.broker
    topic = args.topic
    config = args.config
    ack_timeout = args.timeout

    command_channel = WorkerCommandChannel(f"{host}/{topic}")
    job_handler = JobHandler(worker_finder=command_channel)

    with open(config, "r") as f:
        nexus_structure = f.read()

    write_job = WriteJob(
        nexus_structure,
        file_name,
        host,
        datetime.now(),
    )

    start_handler = job_handler.start_job(write_job)
    timeout = int(current_time()) + ack_timeout

    while not start_handler.is_done():
        if int(current_time()) > timeout:
            raise ValueError("Timeout.")


def validate_namespace(args: argparse.Namespace) -> None:
    argument_list = [args.filename, args.config, args.broker, args.topic]
    for arg in argument_list:
        is_empty(arg)

    # Validate extensions for filename and config
    check_file_extension(args.filename, "nxs")
    check_file_extension(args.config, "json")

    # Validate JSON config path
    config_file = args.config
    if not os.path.isfile(config_file):
        raise ValueError(
            "The configuration file " f"`{config_file}` " "does not exist."
        )


def check_file_extension(arg: str, extension: str) -> None:
    if len(arg.split(".")) < 2 or arg.split(".")[1] != extension:
        raise ValueError(
            f"The argument, {arg}, has incorrect extension. "
            "Please use `.nxs` for output file and `.json` for "
            "the configuration."
        )


def is_empty(arg: str) -> None:
    if not arg or arg.isspace():
        raise ValueError("A positional argument cannot be an empty string.")


if __name__ == "__main__":
    cli_args = cli_parser()
    validate_namespace(cli_args)
    start_file_writer(cli_args)
