from file_writer_control.CommandChannel import CommandChannel
from file_writer_control.CommandStatus import CommandState


class CommandHandler:
    """
    A stand in for (more easily) checking the stat of a command sent to a file-writer.
    """
    def __init__(self, command_channel: CommandChannel, command_id: str):
        """
        Constructor.
        :param command_channel: The instance of a CommandChannel that this class uses for getting the command status from.
        :param command_id: The (unique) command identifier.
        """
        self.command_id = command_id
        self.command_channel = command_channel

    def get_state(self) -> CommandState:
        """
        Get the command state.
        :return: The stat of the command if possible. CommandState.UNKNOWN if the the state can not be determined.
        """
        command = self.command_channel.get_command(self.command_id)
        if command is None:
            return CommandState.UNKNOWN
        return command.state

    def is_done(self) -> bool:
        """
        :return: True of the command completed successfully. False otherwise.
        """
        return self.command_channel.get_command(self.command_id).state == CommandState.SUCCESS

    def get_error_string(self) -> str:
        """
        :return: If there was an error executing the command, this member function will return the error string as
        sent by the file-writer. Will return an empty string otherwise.
        """
        command = self.command_channel.get_command(self.command_id)
        if command is None:
            return ""
        return command.error_message
