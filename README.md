# CyberGreen ETLv2

Master: [![Build Status](https://travis-ci.org/cybergreen-net/etl2.svg?branch=master)](https://travis-ci.org/cybergreen-net/etl2) Devel: [![Build Status](https://travis-ci.org/cybergreen-net/etl2.svg?branch=devel)](https://travis-ci.org/cybergreen-net/etl2)

ETLv2 performs an Extract-Transform-Load cycle for data files relevant to the
CyberGreen project. It:

* Takes an input file for a particular date
* Parses the particular input format
* Transforms to a standard output format
* Filters out invalid, irrelevant or duplicate events
* Enriches with further information (currently ASN and country code)
* Saves resulting enriched data to an output file in a standard format for
  further analysis

## Installation

Python 3.5 or higher is required.

* Clone this repository
* Install requirements (ideally in virtualenv): ```pip3 install -r requirements.txt```

## Usage

### Running ETL for a single file:

* Set env var ```CYBERGREEN_SOURCE_ROOT``` to your input data files, e.g. data/raw
* Set env var ```CYBERGREEN_DEST_ROOT``` to your output data files e.g. data/clean
* Your input files needs to live under ```CYBERGREEN_SOURCE_ROOT``` and match a
  path in ```configs/config.json```, e.g. data/raw/ntp-data/parsed.20200101.out.gz
* To process a file:
  ```python3.5 ETL.py --source=openntp --eventdate=20200101```

### Processing multiple files in parallel on AWS:

* Upload your files to S3
* Set ```CYBERGREEN_SOURCE_ROOT``` to your S3 bucket and path, e.g. ```s3://mybucket/dev/raw```
* Set up an AWS ECS cluster
* Start a number of EC2 instances in the cluster
* Execute jobs on available systems until complete:
  ```python3.5 -mbin.aws_task_queuer --cluster=[ECS cluster name] --task='arn:aws:ecs:[region]:[account]:[task]' --source=[source name] --fileglob=data/files/* --max_tasks=[2 * number of EC2 hosts available]```

### Logging

Metrics on ETL processing times may be sent to Datadog by setting the ```DD_API_KEY``` environment variable.

## Contributing

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request 

## History

### [1.0.0] - 2016-09-30

#### Added

* Added initial ETL scripts plus tests

## Credits

Developed by Cosive (https://www.cosive.com) for The CyberGreen Institute (http://www.cybergreen.net)

## License

Released under the AGPL license.
