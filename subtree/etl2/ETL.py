#!/usr/bin/python3.5
"""
Usage:
    ETL.py --feed=<feed> --eventdate=<eventdate>
           [--config_file=<config_file>] [--force_write]
           [--sampling_rate=<sampling_rate>]

Options:
    -f, --feed=<s>         Feed type to process
    -d, --eventdate=<s>    The date to read the file for
    -c, --config_file=<s>  The config file to run with
                           [default: configs/config.json]
    --force_write          Write to the output file, even if it already exists
    --sampling_rate=<d>    Rate to sample raw logs [default: 1]

Examples:
    ETL.py --feed=openntp --eventdate=20160527
    ETL.py --feed=openntp --eventdate=20160527 \
        --config_file=configs/my_config.json
"""
import sys
from datetime import datetime
import logging
import os
import etl2.parsers
from etl2.utils import load_feed_config


class OutputExistsException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


if os.environ.get('DD_API_KEY'):
    USE_DATADOG = True
else:
    USE_DATADOG = False


# @profile
def etl_process(event_date=None, feed=None, config_path=None,
                force_write=False, sampling_rate=1, use_datadog=True):
    config = load_feed_config(config_path, feed)

    try:
        ETL = getattr(etl2.parsers, config["etl_class"])
        etl = ETL(eventdate=event_date, feed=feed, config=config,
                  force_write=force_write)
    except (AttributeError, TypeError):
        raise RuntimeError(
            "Couldn't find an ETL class or parser for {}".format(feed))

    before = datetime.now()

    logging.info("Input file: {}".format(etl.source_path))
    logging.info("Output file: {}".format(etl.outfile_full_path))

    try:
        etl.input(
            etl.filter_and_parse(
                etl.enrich(
                    etl.output()
                )
            ),
            sampling_rate=sampling_rate
        )
        etl.finalise()
    except RuntimeError as e:
        logging.exception(e)
    except OutputExistsException as e:
        sys.exit(e)

    runtime = datetime.now() - before
    s = etl.stats
    logging.info("{} took {} seconds".format(
        feed + "/" + event_date, runtime))
    logging.info("Processed {} recs / sec".format(
        etl.stats["total"] / runtime.total_seconds()))
    logging.info(etl.stats)

    if use_datadog:
        etl.log_stat(
            "processed_per_second", s["total"] / runtime.total_seconds())
        etl.log_stat(
            "enriched_per_second", s["enriched"] / runtime.total_seconds())
        for stat in s:
            etl.log_stat(stat, s[stat])
    return etl


if __name__ == "__main__":
    from docopt import docopt

    ARGS = docopt(__doc__)
    ARGS["--sampling_rate"] = int(ARGS["--sampling_rate"])

    etl_process(
        event_date=ARGS.get("--eventdate"),
        feed=ARGS.get("--feed"),
        config_path=ARGS.get("--config_file"),
        force_write=ARGS.get("--force_write"),
        sampling_rate=ARGS.get("--sampling_rate"),
        use_datadog=USE_DATADOG
    )
    # cProfile.run('etl_process(eventdate="20160805", feed="openntp")',
    #              "etl-slowness")
