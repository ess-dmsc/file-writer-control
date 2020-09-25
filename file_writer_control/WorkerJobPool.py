from file_writer_control.WorkerFinder import WorkerFinder
from file_writer_control.WriteJob import WriteJob
from kafka import KafkaProducer
from file_writer_control.KafkaTopicUrl import KafkaTopicUrl


class WorkerJobPool(WorkerFinder):
    def __init__(self, job_topic_url: str, command_topic_url: str):
        super(WorkerFinder, self).__init__(command_topic_url)
        self.job_pool = KafkaTopicUrl(job_topic_url)
        self.message_producer = KafkaProducer(bootstrap_servers=[self.job_pool.host_port])

    def try_start_job(self, job: WriteJob):
        pass
