import argparse
import logging
import datetime
from queue import Queue
import subprocess as sp

def computedates(args):

    output = []

    if args.s:
        if args.e:
            start_date = datetime.datetime.strptime(args.s, '%Y-%m-%d')
            end_date = datetime.datetime.strptime(args.e, '%Y-%m-%d')
            if end_date < start_date:
                logging.info("Invalid End Date, End Date should be equal or greater than Start Date")
                exit(1)
            while start_date <= end_date:
                output.append(start_date.strftime('%Y-%m-%d'))
                start_date += datetime.timedelta(days=1)
        else:
            return [args.s]
    elif args.d:
        return args.d

    return output

def buildconfig(args):

    output_string = ''

    if args.c and args.E == 'spark':
        for config in args.c:
            output_string = output_string + ' --conf ' + config
    elif args.c and args.E == 'hive':
        for config in args.extra_vars:
            output_string = output_string + ' --hiveconf ' + config

    return output_string

def create_run_script(args, configs, run_date):

    if args.E == 'spark':
        return '/usr/bin/spark -f ' + args.f + configs + ' --hiveconf run_date=' + run_date
    elif args.E == 'hive':
        return 'hive -f ' + args.f + configs + ' --hiveconf run_date=' + run_date

class QueueEntry():

    def __init__(self, run_date):
        self.run_date = run_date
        self.attempt = 1

    def retry(self):
        self.attempt += 1

    def __str__(self):
        return f"Date is {self.run_date} and attempt is {self.attempt}"

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '-filename', required=True)
    parser.add_argument('-d', '-dates', nargs='*')
    parser.add_argument('-s', '-start_date')
    parser.add_argument('-e', '-end_date')
    parser.add_argument('-c', '-config', nargs='*')
    parser.add_argument('-E', '-Engine', default='spark')
    parser.add_argument('-r', '-retry', default=3)

    args, extra_vars = parser.parse_known_args()
    args.extra_vars = extra_vars

    dates = computedates(args)
    configs = buildconfig(args)
    backfill_queue = Queue()

    for d in dates:
        backfill_queue.put(QueueEntry(d))

    print(backfill_queue.qsize())

    while not backfill_queue.empty():
        curr_item = backfill_queue.get()
        print(curr_item.run_date)
        print(curr_item.attempt)
        if curr_item.attempt < args.r:
            exec = sp.run(create_run_script(args, configs, curr_item.run_date))
            if exec.returncode != 0:
                QueueEntry.retry(curr_item.run_date)
                backfill_queue.put(curr_item)

    for run_date in dates:
        run_script = create_run_script(args, configs, run_date)
        print(run_script)
