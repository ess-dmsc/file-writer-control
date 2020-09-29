from streaming_data_types.action_response_answ import (
    ActionResponse,
    ActionOutcome,
    ActionType,
)
from json import loads
from file_writer_control.CommandStatus import CommandState
from file_writer_control.JobStatus import JobState
from streaming_data_types.status_x5f2 import StatusMessage
from file_writer_control.WorkerStatus import WorkerState


def extract_worker_state_from_status(status: StatusMessage) -> WorkerState:
    json_struct = loads(status.status_json)
    status_map = {"writing": WorkerState.WRITING, "idle": WorkerState.IDLE}
    try:
        status_string = json_struct["state"]
        return status_map[status_string]
    except KeyError:
        return WorkerState.UNKNOWN


def extract_state_from_command_answer(answer: ActionResponse) -> CommandState:
    status_map = {
        ActionOutcome.Failure: CommandState.ERROR,
        ActionOutcome.Success: CommandState.SUCCESS,
    }
    try:
        return status_map[answer.outcome]
    except KeyError:
        return CommandState.ERROR


def extract_job_state_from_answer(answer: ActionResponse) -> JobState:
    if answer.action == ActionType.HasStopped:
        if answer.outcome == ActionOutcome.Success:
            return JobState.DONE
        else:
            return JobState.ERROR
    if answer.action == ActionType.StartJob:
        if answer.outcome == ActionOutcome.Success:
            return JobState.WRITING
        else:
            return JobState.ERROR
