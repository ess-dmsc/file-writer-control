import argparse
import os


def cli_parser() -> argparse.Namespace:
    """
    Parser for the command line interface.
    """

    fw_parser = argparse.ArgumentParser(description="FileWriter Starter")

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
        help="Path to JSON config file.",
    )
    fw_parser.add_argument(
        "-b", "--broker", metavar="kafka_broker", type=str, help="Kafka broker port."
    )
    fw_parser.add_argument(
        "-t",
        "--topic",
        metavar="consume_topic",
        type=str,
        help="Name of the Kafka topic to be consumed.",
    )

    args = fw_parser.parse_args()

    return args


def validate_namespace(args: argparse.Namespace) -> None:
    # Validate the filename
    file_name = args.filename
    if not file_name or file_name.isspace():
        raise ValueError("File name cannot be an empty string.")
    if len(file_name.split(".")) < 2 or file_name.split(".")[1] != "nxs":
        raise ValueError(
            "Output should be a NeXus file. Try again with "
            f'{file_name.split(".")[0]}.nxs'
        )

    # Validate JSON config
    config_file = args.config
    if not os.path.isfile(config_file):
        raise ValueError(
            "The configuration file " f"`{config_file}` " "does not exist."
        )


def is_empty(arg: str) -> None:
    if not arg or arg.isspace():
        raise ValueError("A positional argument cannot be an empty string.")


if __name__ == "__main__":
    cli_args = cli_parser()
    validate_namespace(cli_args)
