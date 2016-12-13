#!/bin/bash

set -e

: "${CYBERGREEN_S3_BUCKET_NAME?Need to set CYBERGREEN_S3_BUCKET_NAME}" 
: "${CYBERGREEN_AWS_CLUSTER_NAME?Need to set CYBERGREEN_AWS_CLUSTER_NAME}" 
: "${CYBERGREEN_AWS_REGION?Need to set CYBERGREEN_AWS_REGION}" 

aws ecs create-cluster --cluster-name ${CYBERGREEN_AWS_CLUSTER_NAME}
# bucket we use for private data
aws s3api create-bucket --bucket ${CYBERGREEN_S3_BUCKET_NAME} --acl private --create-bucket-configuration LocationConstraint=${CYBERGREEN_AWS_REGION} --region=${CYBERGREEN_AWS_REGION}

