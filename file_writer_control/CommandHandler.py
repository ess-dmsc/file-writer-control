from file_writer_control.CommandChannel import CommandChannel
from file_writer_control.CommandStatus import CommandState, CommandStatus


class CommandHandler:
    def __init__(self, command_channel: CommandChannel, command_id: str):
        self.command_id = command_id
        self.command_channel = command_channel

    def get_state(self) -> CommandState:
        command = self.command_channel.get_command(self.command_id)
        if command is None:
            return CommandState.UNKNOWN
        return command.state

    def get_error_string(self) -> str:
        command = self.command_channel.get_command(self.command_id)
        if command is None:
            return ""
        return command.error_message
