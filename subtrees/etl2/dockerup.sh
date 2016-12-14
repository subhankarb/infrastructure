#!/usr/bin/env bash

set -e

die () {
        echo >&2 "$@"
        exit 1
}

: "${CYBERGREEN_AWS_CONTAINER_REPO?Need to set CYBERGREEN_AWS_CONTAINER_REPO}"
: "${CYBERGREEN_AWS_CONTAINER_IMAGE?Need to set CYBERGREEN_AWS_CONTAINER_IMAGE}"
: "${CYBERGREEN_AWS_ECS_TASK_FAMILY?Need to set CYBERGREEN_AWS_ECS_TASK_FAMILY}"

tmpfile=$(mktemp) && $(cat container_def.json | tr '\n' ' ' | envsubst > $tmpfile) && cat $tmpfile && aws ecs register-task-definition --family $CYBERGREEN_AWS_ECS_TASK_FAMILY --cli-input-json file://$tmpfile

login_cmd="$(aws ecr get-login --region eu-west-1)"
echo "Logging in"
# AWS "best practice" - execute arbitrary remote code.
# TODO: much more validation
(echo $login_cmd| grep -e '^docker login') || die "Not the right docker command"
eval $login_cmd
echo "Logged in"

pip install --upgrade pip
pip3.5 wheel -r requirements.txt -w wheels/

# lxml is special. Don't need for ETL just yet, but may need later on orchest.
# working="$(pwd)"
# lxml_build="$(mktemp -d)"
# lxml_version="lxml-3.6.4"
# cd $lxml_build
# wget http://lxml.de/files/$lxml_version.tgz -O $lxml_build/lxml.tgz
# tar -xf lxml.tgz
# cd $lxml_version
# python setup.py bdist_wheel --static-deps
# cd $working
# cp $lxml_build/lxml-$lxml_version/dist/*.wml wheels/

docker build --no-cache -t $CYBERGREEN_AWS_CONTAINER_IMAGE .
echo "Finished build"
aws ecr create-repository --repository-name $CYBERGREEN_AWS_CONTAINER_IMAGE || true # don't exit if it exists
docker tag -f $CYBERGREEN_AWS_CONTAINER_IMAGE:latest $CYBERGREEN_AWS_CONTAINER_REPO/$CYBERGREEN_AWS_CONTAINER_IMAGE:latest
docker push $CYBERGREEN_AWS_CONTAINER_REPO/$CYBERGREEN_AWS_CONTAINER_IMAGE:latest
