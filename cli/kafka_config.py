# Convert command-line args to Kafka library options.
import logging


def get_kafka_config(**kwargs):
    config = {}
    security_settings = get_kafka_security_config(**kwargs)
    config.update(security_settings)

    # Map configuration settings from librdkafka format to kafka_python's
    key_mapping = {
        "security.protocol": "security_protocol",
        "sasl.mechanism": "sasl_mechanism",
        "sasl.username": "sasl_plain_username",
        "sasl.password": "sasl_plain_password",
        "ssl.ca.location": "ssl_cafile",
    }
    return {key_mapping.get(k, k): v for k, v in config.items()}


def get_kafka_security_config(
    security_protocol=None,
    sasl_mechanism=None,
    sasl_username=None,
    sasl_password=None,
    ssl_ca_location=None,
):
    """
    Create security configuration for kafka-python from just-bin-it options.
    If no protocol is passed, PLAINTEXT is returned in the configuration.

    :param protocol: Protocol used to communicate with brokers.
    :param mechanism: SASL mechanism.
    :param username: SASL username.
    :param password: SASL password.
    :param cafile: Path to SSL CA file.
    :return: Configuration dict.
    """
    supported_security_protocols = ["PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL"]
    supported_sasl_mechanisms = ["PLAIN", "SCRAM-SHA-512", "SCRAM-SHA-256"]

    config = {}

    if security_protocol is None:
        security_protocol = "PLAINTEXT"
    elif security_protocol not in supported_security_protocols:
        raise Exception(
            f"Kafka security protocol {security_protocol} not supported, use {supported_security_protocols}"
        )

    logging.info(f"Using Kafka security protocol {security_protocol}")
    config["security.protocol"] = security_protocol

    if "SASL_" in security_protocol:
        if sasl_mechanism not in supported_sasl_mechanisms:
            raise Exception(
                f"SASL mechanism {sasl_mechanism} not supported, use {supported_sasl_mechanisms}"
            )

        logging.info(f"Using SASL mechanism {sasl_mechanism}")
        config["sasl.mechanism"] = sasl_mechanism

        if not sasl_username or not sasl_password:
            raise Exception(
                f"Username and password are required with {security_protocol}"
            )

        config["sasl.username"] = sasl_username
        config["sasl.password"] = sasl_password

    if "_SSL" in security_protocol:
        logging.info(f"Using CA certificate file {ssl_ca_location}")
        config["ssl.ca.location"] = ssl_ca_location

    return config
