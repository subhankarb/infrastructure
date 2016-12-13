set -e

bash aws_init.sh
bash dockerup.sh
# returns the latest task revision we just created in ecs register-task-definition
ecs_task_name=$(aws ecs list-task-definitions --family CYBERGREEN_AWS_ECS_TASK_FAMILY | jq --raw-output ".taskDefinitionArns[-1]")

: "${ecs_task_name?Couldn\'t get ECS task name in $CYBERGREEN_AWS_TASK_FAMILY family from AWS, aborting}" 

python3.5 -mbin.aws_task_queuer --cluster=$CYBERGREEN_AWS_ECS_CLUSTER --task='${ecs_task_name}' --max_tasks=2 openntp/201605*
