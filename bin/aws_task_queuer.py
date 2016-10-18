#!/usr/bin/python3.5

"""
Usage:
    aws_task_date_queuer.py --cluster=<cluster> --task=<task> \
        --feed=<feed> --eventdate=<eventdate>... \
        --max_tasks=<max_tasks> [--force_write] [--config_file=<config_file>]
    aws_task_date_queuer.py --cluster=<cluster> --task=<task> \
        --feed=<feed> --fileglob=<fileglob>  \
        --max_tasks=<max_tasks> [--force_write] [--config_file=<config_file>]

Options:
    -f, --feed=<s>         Feed type to process
    -d, --eventdate=<s>    The date to read the file for
    -n, --cluster=<s>      AWS cluster name to use
    -t, --task=<s>         AWS task name
    -g, --fileglob=<s>     File glob with files to process
    -c, --config_file=<s>  The config file to run with
                           [default: configs/config.json]
    -m, --max_tasks=<d>    The number of tasks to run in parallel
    --force_write          Write to the output file, even if it already exists


Examples:
    aws_task_date_queuer.py --cluster=cybergreen-etl2 \
     --task='arn:aws:ecs:[region]:[acc ID]:task-definition/etl2:2'\
     --feed=openntp -d 20160527 -d 20160610
    aws_task_date_queuer.py --cluster=cybergreen-etl2 \
     --task='arn:aws:ecs:[region]:[acc ID]:task-definition/etl2:2'\
     --feed=openntp -f data/*.csv.gz  --max_tasks=10
"""
import time
import glob
import re
import logging
from pprint import pformat

import boto3
from collections import deque
from etl2.utils import load_source_config, load_env_var, load_env_var_or_none

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(pathname)s:%(lineno)d (%(funcName)s) - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(name=__name__)

client = boto3.client('ecs')
ARGS = {}
CONFIG = {}


def get_task_list():
    resp = client.list_tasks(cluster=ARGS["--cluster"])

    return resp["taskArns"]


# TODO: we currently don't account for source type when checking running tasks,
# need to make this a combo of EVENTDATE and SOURCE
def update_running_tasks():
    logger.info("DESCRIBING TASKS")
    known_task_arns = get_task_list()
    running_queue = []

    if known_task_arns:
        resp = client.describe_tasks(
            cluster=ARGS["--cluster"],
            tasks=known_task_arns
        )
        tasks = resp["tasks"]
        logger.info("Task arns: {}".format(pformat(tasks)))

        for task in tasks:
            try:
                date = ([
                    k["value"]
                    for k in task["overrides"]["containerOverrides"][0]["environment"]
                    if k["name"] == "EVENTDATE"
                ][0])
            except (IndexError, KeyError):
                date = None

            logging.info("DATE: {}".format(date))

            if date:
                if task["lastStatus"] in ["RUNNING", "PENDING"]:
                    logging.info("task {} is running".format(date))
                    running_queue.append(date)
                elif task["lastStatus"] == "STOPPED":
                    logging.info("task {} has stopped, removing {} from queue"
                                 .format(task, date))

    return running_queue


def dispatch(pending_queue):
    running_queue = update_running_tasks()
    pending = sorted(pending_queue.copy())
    print("Running queue: {}".format(running_queue))

    new_task_count = int(ARGS["--max_tasks"]) - len(running_queue)

    logging.info("Running jobs deficit (need {}), launching {} more"
                 .format(ARGS["--max_tasks"], new_task_count))

    task_arns = []

    # add the number of jobs to make up the deficit
    for file_date in pending[:new_task_count]:
        # TODO: work out how to change Cloudwatch log name
        # logname = "{}-{}-{}".format(
        #    datetime.utcnow().isoformat(), ARGS["--source"], date)
        response = client.run_task(
            cluster=ARGS['--cluster'],
            taskDefinition=ARGS['--task'],
            overrides={
                'containerOverrides': [
                    {
                        'name': 'etl',
                        'environment': [
                            {
                                'name': "FEED",
                                'value': ARGS['--feed']
                            },
                            {
                                'name': "EVENTDATE",
                                'value': file_date
                            },
                            {
                                'name': "CYBERGREEN_SOURCE_ROOT",
                                'value': load_env_var("CYBERGREEN_SOURCE_ROOT")
                            },
                            {
                                'name': "CYBERGREEN_DEST_ROOT",
                                'value': load_env_var("CYBERGREEN_DEST_ROOT")
                            },
                            {
                                'name': "DD_API_KEY",
                                'value': load_env_var_or_none("DD_API_KEY")
                            },
                            {
                                'name': "ECS_AVAILABLE_LOGGING_DRIVERS",
                                'value': "json-file,awslogs"
                            },
                        ]
                    },
                ],
            },
        )

        if response.get("failures"):
            pass
            # print("{} not running yet".format(date))
        else:
            # print(response)
            logger.info("{} running, taskArn (log name): {}"
                        .format(date, response["tasks"][0]["taskArn"]))
            logger.info("Resp: {}".format(pformat(response)))
            task_arns.append(response["tasks"][0]["taskArn"])
            pending_queue.remove(date)

    return pending_queue


if __name__ == "__main__":
    from docopt import docopt
    ARGS = docopt(__doc__)
    print(ARGS)
    date_queue = deque()

    CONFIG = load_source_config(ARGS["--config_file"], ARGS["--feed"])

    if ARGS.get("--eventdate"):
        for date in ARGS.get("--eventdate"):
            date_queue.append(date)
    elif ARGS.get("--fileglob"):
        for f in glob.glob(ARGS.get("--fileglob")):
            m = re.search(CONFIG["source_file_regex"], f)
            date_queue.append("{}{}{}".format(
                m.group("year"), m.group("month"), m.group("day")))

    last_remain_count = None

    while date_queue:
        date_queue = dispatch(sorted(date_queue))

        if last_remain_count != len(date_queue):
            logger.info("Still pending: {}".format(len(date_queue)))
            last_remain_count = len(date_queue)
        if date_queue:
            time.sleep(20)
