#!/bin/bash

set -e

: "${CYBERGREEN_S3_BUCKET_NAME?Need to set CYBERGREEN_S3_BUCKET_NAME}" 
: "${CYBERGREEN_AWS_CLUSTER_NAME?Need to set CYBERGREEN_AWS_CLUSTER_NAME}" 
: "${CYBERGREEN_AWS_REGION?Need to set CYBERGREEN_AWS_REGION}" 

echo "Creating ECS cluster ${CYBERGREEN_AWS_CLUSTER_NAME}"
aws ecs create-cluster --cluster-name ${CYBERGREEN_AWS_CLUSTER_NAME}

echo "Creating bucket ${CYBERGREEN_S3_BUCKET_NAME}"
# bucket we use for private data - may already exist, usually errors so || true
(aws s3api create-bucket --bucket ${CYBERGREEN_S3_BUCKET_NAME} --acl private --create-bucket-configuration LocationConstraint=${CYBERGREEN_AWS_REGION} --region=${CYBERGREEN_AWS_REGION} || true)
echo "Bucket create: $?"
aws s3api get-bucket-acl --bucket ${CYBERGREEN_S3_BUCKET_NAME}
echo "AWS setup complete"
