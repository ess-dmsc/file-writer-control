from file_writer_control.CommandChannel import CommandChannel
from file_writer_control.CommandStatus import CommandState, CommandStatus


class CommandHandler:
    def __init__(self, command_channel: CommandChannel, command_id: str):
        self.command_id = command_id
        self.command_channel = command_channel

    def get_state(self) -> CommandState:
        for command in self.command_channel.list_commands():
            if command.command_id == self.command_id:
                return command.state
        return CommandState.NO_COMMAND

    def get_error_string(self) -> str:
        for command in self.command_channel.list_commands():
            if command.command_id == self.command_id:
                return command.error_message
        return ""

