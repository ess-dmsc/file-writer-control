import argparse
import os


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

    args = fw_parser.parse_args()

    return args


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
