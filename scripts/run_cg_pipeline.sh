#!/usr/bin/env bash 

set -e

SUBTREE_DIR="subtrees"
SCRIPTS_DIR="scripts"
ETL_DIR="${SUBTREE_DIR}/etl2"
ETL_VENV_DIR="venv"

# ETL

## ETL setup / pull latest

if [[ ! -d "$ETL_DIR" ]]; then
    git subtree add --prefix $ETL_DIR git@github.com:cybergreen-net/etl2.git devel-kayne-s3-orchestration-and-tests
else
    git subtree pull --prefix $ETL_DIR git@github.com:cybergreen-net/etl2.git devel-kayne-s3-orchestration-and-tests
fi

if [ ! -d "$ETL_VENV_DIR" ]; then
    virtualenv --python=python3 $ETL_VENV_DIR
    echo "Virtualenv created at $ETL_VENV_DIR"
fi

## Infra setup / confirm

bash ${SCRIPTS_DIR}/aws_init.sh
bash "${ETL_DIR}/dockerup.sh"
# returns the latest task revision we just created in ecs register-task-definition
ecs_task_name=$(aws ecs list-task-definitions --family CYBERGREEN_AWS_ECS_TASK_FAMILY | jq --raw-output ".taskDefinitionArns[-1]")

: "${ecs_task_name?Couldn\'t get ECS task name in $CYBERGREEN_AWS_TASK_FAMILY family from AWS, aborting}" 

## ETL run

source $ETL_VENV_DIR/bin/activate
pip install --upgrade pip
pip install $ETL_VENV_DIR/requirements.txt

cd $ETL_DIR
python3.5 -mbin.aws_task_queuer --cluster=$CYBERGREEN_AWS_ECS_CLUSTER --task='${ecs_task_name}' --max_tasks=2 openntp/201605*
cd -
deactivate
