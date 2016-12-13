#!/usr/bin/env bash 

set -e

SUBMODULE_DIR="submodules"
SCRIPTS_DIR="scripts"
ETL_DIR="${SUBMODULE_DIR}/etl2"

bash ${SCRIPTS_DIR}/aws_init.sh
bash "${ETL_DIR}/dockerup.sh"
# returns the latest task revision we just created in ecs register-task-definition
ecs_task_name=$(aws ecs list-task-definitions --family CYBERGREEN_AWS_ECS_TASK_FAMILY | jq --raw-output ".taskDefinitionArns[-1]")

: "${ecs_task_name?Couldn\'t get ECS task name in $CYBERGREEN_AWS_TASK_FAMILY family from AWS, aborting}" 

cd $ETL_DIR
python3.5 -mbin.aws_task_queuer --cluster=$CYBERGREEN_AWS_ECS_CLUSTER --task='${ecs_task_name}' --max_tasks=2 openntp/201605*
cd -
