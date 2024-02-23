import time

from file_writer_control import WorkerJobPool


def print_columns(titles: list | tuple, values: list[list | tuple] | tuple[list | tuple, ...]):
    if not len(values) or not len(titles):
        return
    widths = [len(str(x)) for x in titles]
    for row in values:
        for i, v in enumerate(row):
            n = len(str(v))
            if n > widths[i]:
                widths[i] = n
    w_format = ''.join([f'{{:{n+1:d}s}}' for n in widths])
    print(w_format.format(*[str(x) for x in titles]))
    print(w_format.format(*['-'*n for n in widths]))
    for row in values:
        print(w_format.format(*[str(x) for x in row]))
    print()


def print_current_state(channel: WorkerJobPool):
    workers = channel.list_known_workers()
    if len(workers):
        print("Known workers")
        print_columns(("Service id", "Current state"), [(w.service_id, w.state) for w in workers])
    else:
        print("No workers")

    jobs = channel.list_known_jobs()
    if len(jobs):
        print("Known jobs")
        job_info = [(j.service_id, j.job_id, j.state, j.file_name if j.file_name else j.message) for j in jobs]
        print_columns(("Service id", "Job id", "Current state", "File name or message"), job_info)
    else:
        print("No jobs")

    commands = channel.list_known_commands()
    if len(commands):
        print("Known commands")
        print_columns(("Job id", "Command id", "Current state", "Message"),
                      [(c.job_id, c.command_id, c.state, c.message) for c in commands])
    else:
        print("No commands")


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-b', '--broker', help="Kafka broker", default='localhost:9092', type=str)
    parser.add_argument('-c', '--command', help="Writer command topic", default="WriterCommand", type=str)
    parser.add_argument('-t', '--topic', help='Writer job topic', default='WriterJobs', type=str)
    parser.add_argument('-s', '--sleep', type=int, help='Post pool creation sleep time', default=1)
    args = parser.parse_args()
    pool = WorkerJobPool(f'{args.broker}/{args.topic}', f'{args.broker}/{args.command}')
    time.sleep(args.sleep)
    print_current_state(pool)


if __name__ == "__main__":
    main()
