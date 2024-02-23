import time

from file_writer_control import WorkerJobPool


def kill_job(pool: WorkerJobPool, service_id: str, job_id: str):
    result = pool.try_send_stop_now(service_id, job_id)


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-b', '--broker', help="Kafka broker", default='localhost:9092', type=str)
    parser.add_argument('-c', '--command', help="Writer command topic", default="WriterCommand", type=str)
    parser.add_argument('-t', '--topic', help='Writer job topic', default='WriterJobs', type=str)
    parser.add_argument('-s', '--sleep', help='Post pool creation sleep time (s)', default=1, type=int)
    parser.add_argument('service_id', type=str, help='Writer service id to stop')
    parser.add_argument('job_id', type=str, help='Writer job id to stop')

    args = parser.parse_args()
    pool = WorkerJobPool(f'{args.broker}/{args.topic}', f'{args.broker}/{args.command}')
    time.sleep(args.sleep)
    kill_job(pool, args.service_id, args.job_id)


if __name__ == "__main__":
    main()
