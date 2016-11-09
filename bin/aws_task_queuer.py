#!/usr/bin/python3.5

"""
Usage:
    aws_task_queuer.py --cluster=<cluster> --task=<task> --max_tasks=<max_tasks> [--force_write] [--config_file=<config_file>] <filepattern>...

Options:
    -n, --cluster=<s>      AWS cluster name to use
    -t, --task=<s>         AWS task name
    -c, --config_file=<s>  The config file to run with
                           [default: configs/config.json]
    -m, --max_tasks=<d>    The number of tasks to run in parallel
    --force_write          Write to the output file, even if it already exists
    filepattern            source/datemask for the files to load

Examples:
    aws_task_queuer.py --cluster=cybergreen-etl2 \
     --task='arn:aws:ecs:[region]:[acc ID]:task-definition/etl2:2'\
     openntp/20160527 openntp/20160610
    aws_task_queuer.py --cluster=cybergreen-etl2 \
     --task='arn:aws:ecs:[region]:[acc ID]:task-definition/etl2:2'\
     --max_tasks=2 opensnmp/201605*
"""
import time
import glob
import re
import logging
from pprint import pformat
import sys

import boto3
from collections import deque
from etl2.utils import load_config, load_env_var, load_env_var_or_none, list_s3_files
import base64

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(pathname)s:%(lineno)d (%(funcName)s) - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(name=__name__)

client = boto3.client('ecs')
s3 = boto3.resource('s3')
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
    pending = sorted(pending_queue.copy(), key=lambda x: x['event_date'])
    print("Running queue: {}".format(running_queue))

    new_task_count = int(ARGS["--max_tasks"]) - len(running_queue)

    logging.info("Running jobs deficit (need {}), launching {} more"
                 .format(ARGS["--max_tasks"], new_task_count))

    task_arns = []

    # add the number of jobs to make up the deficit
    for task in pending[:new_task_count]:
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
                                'value': task['feed']
                            },
                            {
                                'name': "EVENTDATE",
                                'value': task['event_date']
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
                                'value': load_env_var_or_none("DD_API_KEY") or ""
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
                        .format(task['event_date'], response["tasks"][0]["taskArn"]))
            logger.info("Resp: {}".format(pformat(response)))
            task_arns.append(response["tasks"][0]["taskArn"])
            pending_queue.remove(task)

    return pending_queue


def enqueue_files(patterns):
    file_set = set()
    for pattern in patterns:
        logger.info("Processing {}".format(pattern))
        feed, date_pattern = pattern.split('/')
        for s3_file in list_s3_files(s3, CONFIG, feed, date_pattern=date_pattern):
            if s3_file not in list_s3_files(s3, CONFIG, feed, srcordest="destination_path", date_pattern=date_pattern):
                logger.info("Adding file {}/{}".format(s3_file['feed'], s3_file['event_date']))
                file_set.add(s3_file)
    for s3_file in file_set:
        task_queue.append(s3_file)


def start_ec2_instances():
    user_data = base64.b64encode(b"#!/bin/bash\nyum install -y aws-cli\necho ECS_CLUSTER=cybergreen-etl2 >>/etc/ecs/ecs.config\n")
    ec2 = boto3.resource('ec2', region_name='eu-west-1', api_version='2016-04-01')
    instances = ec2.create_instances(ImageId='ami-078df974', MinCount=1, MaxCount=1, KeyName='cybergreen-ec2',
                                    SecurityGroups=['launch-wizard-1'], InstanceType="m4.large", UserData=user_data,
                                    BlockDeviceMappings=[{"DeviceName": "/dev/xvdcz",
                                                          "Ebs": {"VolumeSize": 200, "DeleteOnTermination": True}}],
                                    IamInstanceProfile={"Name": "cybergreenECSRole"})
    all_ready = False
    while not all_ready:
        all_ready = True
        for i in instances:
            print(i, i.state.Name)
            state = ec2.Instance(id=i.id).state.Name
            if state != 'running':
                all_ready = False
            if state not in ['running', 'pending']:
                logging.error("Instance failed to start")
                exit()
        time.sleep(10)
    return

def stop_ec2_instances(instances):
    ec2 = boto3.resource('ec2', region_name='eu-west-1', api_version='2016-04-01')
    for i in instances:
        i.stop()

if __name__ == "__main__":
    # TODO: move this into a main()?
    from docopt import docopt
    ARGS = docopt(__doc__)
    task_queue = deque()
    instances=[]
    CONFIG = load_config(ARGS["--config_file"])
    last_remain_count = None
    enqueue_files(ARGS.get("<filepattern>"))
    if task_queue:
        instances = start_ec2_instances()
    while task_queue or len(update_running_tasks()) > 0:
        task_queue = dispatch(sorted(task_queue, key=lambda x: x['event_date']))
        if last_remain_count != len(task_queue):
            logger.info("Still pending: {}".format(len(task_queue)))
            last_remain_count = len(task_queue)
        time.sleep(20)
    if instances:
        stop_ec2_instances(instances)
