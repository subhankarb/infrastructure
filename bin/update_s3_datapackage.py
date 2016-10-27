#!/usr/local/env python3

"""
Usage:
    update_s3_datapackage.py [--config_file=<config_file>]

    -f, --config_file=<s>  The config file to run with
                           [default: configs/config.json]
"""

import boto3
import tempfile
import os
from docopt import docopt
import json
import datapackage
from etl2.utils import split_s3_path, load_config


def get_file_listing(s3=None):
    source_files = {}

    for feed_name, feed in CONFIG["source"].items():
        try:
            s3_bucket, s3_path = split_s3_path(feed["destination_path"])
            bucket = s3.Bucket(s3_bucket)
            source_files[feed_name] = []
            for obj in bucket.objects.filter(Prefix=s3_path):
                # dir names seem to inconsistently slip in here...
                if not obj.key.endswith("/"):
                    source_files[feed_name].append(obj.key)
        except (KeyError, ValueError) as e:
            print(e)
            print("You need a destination S3 path")

    return source_files


def set_relative_datapackage_path(file_path):
    """
    Convert a full path name to a relative path name from the datapackage.json
    we upload to S3.
    """

    dp_uri = CONFIG["datapackage_path"]
    _, dp_path = split_s3_path(dp_uri)
    dp_dir, _ = os.path.split(dp_path)
    file_dir, file_name = os.path.split(file_path)

    return os.path.join(file_dir.replace(dp_dir, ""), file_name).strip("/")


def generate_datapackage(outfile, file_list):
    schema = {
        "fields": [
            {
                "name": "timestamp",
                "type": "datetime",
                "description": "Date and time of observation",
            },
            {
                "name": "risk_id",
                "type": "integer",
                "description": "ID from Postgres DB representing risk type",
            },
            {
                "name": "ip",
                "type": "string",
                "description": "IP address related to observation",
            },
            {
                "name": "asn",
                "type": "integer",
                "description": "Autonomous Network Number",
            },
            {
                "name": "cc",
                "type": "string",
                "description": "ISO ALPHA-2 country code",
            },
        ]
    }

    dp = datapackage.DataPackage()
    dp.descriptor['name'] = 'cybergreen_enriched_data'
    dp.descriptor['title'] = 'CyberGreen Enriched Data'
    dp.descriptor['resources'] = []
    for source in file_list:
        dp.descriptor['resources'].append(
            {
                "name": source,
                "format": "csv",
                "compression": "gz",  # TODO: determine this dynamically
                "schema": schema,
                "path": [set_relative_datapackage_path(f) for f in file_list[source]]
            }
        )

    print(dp.to_json())


def upload_datapackage(local_file, remote_target):
    pass


if __name__ == "__main__":
    ARGS = docopt(__doc__)
    CONFIG = load_config(ARGS["--config_file"])

    s3 = boto3.resource('s3')

    local_file_obj = tempfile.NamedTemporaryFile()
    s3_path = CONFIG["datapackage_path"]

    #print("Uploading to {}".format(s3_path))

    file_list = get_file_listing(s3=s3)
    generate_datapackage(local_file_obj.name, file_list)
    upload_datapackage(local_file_obj.name, s3_path)
