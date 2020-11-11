from file_writer_control.WorkerFinder import WorkerFinder
from file_writer_control.WriteJob import WriteJob
from kafka import KafkaProducer
from file_writer_control.KafkaTopicUrl import KafkaTopicUrl
from file_writer_control.CommandHandler import CommandHandler
from file_writer_control.CommandStatus import CommandState


class WorkerJobPool(WorkerFinder):
    """
    A child of WorkerFinder intended for use with "worker pool" style of starting a file-writing job.
    """

    def __init__(self, job_topic_url: str, command_topic_url: str):
        """
        :param job_topic_url: The Kafka topic that the available file-writers are listening to for write jobs.
        :param command_topic_url:  The Kafka topic that a file-writer uses to send status updates to and receive direct
        commands from.
        """
        super().__init__(command_topic_url)
        self._job_pool = KafkaTopicUrl(job_topic_url)
        self._pool_producer = KafkaProducer(
            bootstrap_servers=[self._job_pool.host_port]
        )

    def _send_pool_message(self, message: bytes):
        """
        Send a message to the Kafka topic that is configured as the job-pool topic.
        .. note:: If the file-writer has been configured properly, it will only accept start-job messages to this topic.
        :param message: The binary data of the message.
        """
        self._pool_producer.send(self._job_pool.topic, message)

    def try_start_job(self, job: WriteJob) -> CommandHandler:
        """
        See base class for documentation.
        """
        self.command_channel.add_command_id(job.job_id, job.job_id)
        self.command_channel.get_command(
            job.job_id
        ).state = CommandState.WAITING_RESPONSE
        self._send_pool_message(job.get_start_message())
        return CommandHandler(self.command_channel, job.job_id)
