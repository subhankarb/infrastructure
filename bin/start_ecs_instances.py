#!/usr/bin/python27
import boto3
import base64
import __future__
"""
Lambda's can't use environments so we hard-code things or we put them in an s3 bucket and load it from there which requires
another configuration in here for the bucket etc etc etc. Hard coding it is.
No Python 3 on Lambda so going old school.
"""

user_data = base64.b64encode(
    "#!/bin/bash\nyum install -y aws-cli\necho ECS_CLUSTER={} >>/etc/ecs/ecs.config\n".format('cybergreen-etl2'))


def lambda_handler(event, context):
    ec2 = boto3.resource('ec2', region_name='eu-west-1')
    instance = ec2.create_instances(ImageId='ami-078df974', MinCount=1, MaxCount=1, KeyName='cybergreen-ec2',
                                    SecurityGroups=['launch-wizard-1'], InstanceType="m4.large", UserData=user_data,
                                    BlockDeviceMappings=[{"DeviceName": "/dev/xvdcz",
                                                          "Ebs": {"VolumeSize": 200, "DeleteOnTermination": True}}])
    print("started instance {}".format(instance))

lambda_handler(None,None)