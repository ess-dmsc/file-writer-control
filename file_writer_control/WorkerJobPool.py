from file_writer_control.WorkerFinder import WorkerFinder
from file_writer_control.WriteJob import WriteJob
from kafka import KafkaProducer
from file_writer_control.KafkaTopicUrl import KafkaTopicUrl
from file_writer_control.CommandHandler import CommandHandler


class WorkerJobPool(WorkerFinder):
    def __init__(self, job_topic_url: str, command_topic_url: str):
        super().__init__(command_topic_url)
        self.job_pool = KafkaTopicUrl(job_topic_url)
        self.pool_producer = KafkaProducer(bootstrap_servers=[self.job_pool.host_port])

    def send_pool_message(self, message: bytes):
        self.pool_producer.send(self.job_pool.topic, message)

    def try_start_job(self, job: WriteJob) -> CommandHandler:
        self.command_channel.add_command_id(job.job_id, job.job_id)
        self.send_pool_message(job.get_start_message())
        return CommandHandler(self.command_channel, job.job_id)
