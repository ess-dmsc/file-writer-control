import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from time import time as current_time

from kafka_config import get_kafka_config

from cli.start_file_writer import is_empty
from file_writer_control import JobHandler, JobState, WorkerJobPool


def cli_parser() -> argparse.Namespace:
    """
    Parser for the command line interface.
    """

    fw_parser = argparse.ArgumentParser(
        fromfile_prefix_chars="@", description="FileWriter Stopper"
    )
    kafka_args = fw_parser.add_argument_group("Kafka broker options")

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
        "-t",
        "--command-status-topic",
        metavar="consume_topic",
        type=str,
        required=True,
        help="Name of the Kafka topic to listen to commands and send status to.",
    )
    fw_parser.add_argument(
        "-p",
        "--job-pool-topic",
        metavar="job_pool_topic",
        type=str,
        required=True,
        help="The Kafka topic that the available file-writers"
        " are listening to for write jobs.",
    )
    fw_parser.add_argument(
        "--timeout",
        metavar="ack_timeout",
        type=float,
        default=5,
        help="How long to wait for timeout on acknowledgement.",
    )

    kafka_args.add_argument(
        "-b",
        "--broker",
        metavar="kafka_broker",
        type=str,
        required=True,
        help="Broker host and port (e.g. localhost:9092)",
    )
    kafka_args.add_argument(
        "--security-protocol",
        type=str,
        default="SASL_SSL",
        help="Kafka security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)",
    )
    kafka_args.add_argument(
        "--sasl-mechanism",
        type=str,
        default="SCRAM-SHA-256",
        help="Kafka SASL mechanism (GSSAPI, PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER)",
    )
    kafka_args.add_argument(
        "-U",
        "--sasl-username",
        type=str,
        help="Kafka SASL username",
    )
    kafka_args.add_argument(
        "--ssl-ca-location",
        type=str,
        help="Kafka SSL CA certificate path",
    )

    args = fw_parser.parse_args()

    return args


def create_job_handler(args: argparse.Namespace, job_id: str) -> JobHandler:
    host = args.broker
    command_topic = args.command_status_topic
    pool_topic = args.job_pool_topic
    kafka_config = get_kafka_config(
        security_protocol=args.security_protocol,
        sasl_mechanism=args.sasl_mechanism,
        sasl_username=args.sasl_username,
        sasl_password=args.sasl_password,
        ssl_ca_location=args.ssl_ca_location,
    )
    command_channel = WorkerJobPool(
        f"{host}/{pool_topic}", f"{host}/{command_topic}", kafka_config=kafka_config
    )
    job_handler = JobHandler(worker_finder=command_channel, job_id=job_id)
    # Required for formation of the handler.
    time.sleep(10)
    return job_handler


def stop_write_job_now(job_handler: JobHandler) -> None:
    if job_handler.get_state() == JobState.WRITING:
        job_handler.set_stop_time(datetime.now())
        while not job_handler.is_done():
            time.sleep(1)
        print("FileWriter successfully stopped.")
    sys.exit()


def stop_write_job(args: argparse.Namespace, job_handler: JobHandler) -> None:
    stop_time = float(args.stop_after[1])
    timeout = int(current_time()) + args.timeout
    stop_time = datetime.now() + timedelta(seconds=stop_time)
    stop_handler = job_handler.set_stop_time(stop_time)
    while not stop_handler.is_done() and not job_handler.is_done():
        if int(current_time()) > timeout:
            raise ValueError("Timeout.")


def verify_write_job(job_handler: JobHandler) -> None:
    if job_handler.get_state() == JobState.WRITING:
        print("The write process is confirmed. Stopping...")
    else:
        raise ValueError(
            "There are no write jobs associated with the "
            "given job id. Please check broker, topic and "
            "id information and try again."
        )


def validate_namespace(args: argparse.Namespace) -> None:
    if args.stop and args.stop_after:
        print(
            "Positional arguments [-s --stop] and [-sa --stop_after] cannot "
            "be used simultaneously."
        )
        sys.exit()
    mandatory_argument_list = [
        args.stop,
        args.broker,
        args.command_status_topic,
        args.job_pool_topic,
        args.stop_after,
    ]
    for arg in mandatory_argument_list:
        if arg:
            is_empty(arg)

    sasl_password = os.environ.get("SASL_PASSWORD")
    setattr(args, "sasl_password", sasl_password)
    if "SASL_" in args.security_protocol and not sasl_password:
        raise ValueError(
            f"Security protocol {args.security_protocol} requires password. Set username with --sasl-username and export the environment variable 'SASL_PASSWORD' with the password"
        )


def stop_writer():
    cli_args = cli_parser()
    validate_namespace(cli_args)

    _id = cli_args.stop if cli_args.stop else cli_args.stop_after[0]
    handler = create_job_handler(cli_args, _id)
    verify_write_job(handler)

    if cli_args.stop_after:
        stop_write_job(cli_args, handler)
    else:
        stop_write_job_now(handler)


if __name__ == "__main__":
    stop_writer()
