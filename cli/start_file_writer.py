import argparse


def cli_parser() -> argparse.Namespace:
    """
    Parser for the command line interface.
    """

    fw_parser = argparse.ArgumentParser(description="FileWriter Starter")
    fw_parser.add_argument(
        "Name",
        metavar="filename",
        type=str,
        help="Name of the output file, e.g., `<filename>.nxs`.",
    )
    fw_parser.add_argument(
        "Config", metavar="json_config", type=str, help="Path to JSON config file."
    )
    fw_parser.add_argument(
        "Broker", metavar="kafka_broker", type=str, help="Kafka broker port."
    )
    fw_parser.add_argument(
        "Topic",
        metavar="consume_topic",
        type=str,
        help="Name of the Kafka topic to be consumed.",
    )

    args = fw_parser.parse_args()

    return args


# def validate_namespace(args: argparse.Namespace) -> None:
#     # Validate the filename
#     file_name = args.Name


if __name__ == "__main__":
    cli_args = cli_parser()
    # validate_namespace(cli_args)
