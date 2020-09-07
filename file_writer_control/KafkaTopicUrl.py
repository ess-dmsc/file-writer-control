import re


class KafkaTopicUrl:
    test_regexp = re.compile("(\s*(kafka://)?((([^/?#:]+)+)(:(\d+))?)/?([a-zA-Z0-9._-]+)?\s*)")

    def __init__(self, url: str):
        result = re.match(KafkaTopicUrl.test_regexp, url)
        if result is None:
            raise RuntimeError("Unable to match kafka url.")
        self.port = 9092
        if result.group(7) is not "":
            self.port = int(result.group(7))
        self.host = result.group(4)
        self.host_port = "{}:{}".format(self.host, self.port)
        self.topic = result.group(8)
