from filewriter_control.Worker import Worker
import threading
from filewriter_control.KafkaTopicUrl import KafkaTopicUrl
from kafka import KafkaConsumer
from copy import copy


class CommandChannel:
    def __init__(self, command_topic_url: str):
        kafka_address = KafkaTopicUrl(command_topic_url)
        thread_kwargs = {"hostport": kafka_address.host_port, "topic": kafka_address.topic}
        self.map_of_workers = []
        self.worker_lock = threading.Lock()
        self.run_thread = True
        self.thread = threading.Thread(target=self.thread_function, kwargs=thread_kwargs)

    def __del__(self):
        """Destructor. Tells the thread to exit and then waits for it to do so."""
        self.run_thread = False
        self.thread.join()

    def list_workers(self) -> list(Worker):
        # Mutex?
        return copy(self.list_of_workers)

    def thread_function(self, host: str, topic: str):
        consumer = KafkaConsumer(bootstrap_servers=host, topics=topic, fetch_max_bytes=52428800*6)
        while self.run_thread:
            import time
            time.sleep(0.1)
            messages = consumer.poll(timeout_ms=100)
            for message in messages:
                pass



