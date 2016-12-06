import csv
import logging
import radix
import IP2Location
import pickle
import gzip
import os.path
import shutil
from pytz import utc

from etl2.utils import is_private_ipv4, is_s3_path, load_feed_config, check_path
from datetime import datetime

# import cProfile
import os

try:
    import boto3
    from botocore.exceptions import ClientError
except:
    # we test for successful import later if required
    pass

ARGS = {}
LOG_OUTPUT_INTERVAL = 1000000

# TODO: take this from config.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(pathname)s:%(lineno)d (%(funcName)s) - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')


class IPValidationException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class TimestampValidationException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def coroutine(func):
    """
    This is a little helper to kick off the iterator for coroutines
    """
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        cr.__next__()
        return cr
    return start


# The Particia tree for the subnet to ASN mapping lives in this, the destructor on the C code doesn't always trigger so we need
# to make sure we don't keep loading it for the sake of the test runners.
asn_tree = None


class csv_etl(object):
    def __init__(self, eventdate=None, feed=None, config_path=None, force_write=False):
        """
        Initialiser, main thing we bring in is the date we're working from and
        the source feed.
        """
        # Which day are we working on in YYYYMMDD format.
        self.stats = {
            "unknown_asn": 0,
            "total": 0,
            "badip": 0,
            "badts": 0,
            "custom_filter": 0,
            "repeats": 0,
            "no_country": 0,
            "parsed": 0,
            "enriched": 0
        }
        self.country_count = {}
        self.asn_count = {}
        self.eventdate = str(eventdate)
        # The feed name.
        self.feed = feed
        # This is used to cache seen IPs if we're stripping repeat IPs.
        self.ips_seen = set()
        self.config = load_feed_config(config_path, self.feed)
        self.risk_id = self.config['risk_id']
        self.source_path = None
        self.source_bucket = None
        self.source_s3_path = None
        self.dest_bucket = None
        self.dest_s3_path = None
        self.s3_output = False

        if not is_s3_path(self.config['source_path']):
            if not check_path(self.config['source_path']):
                logging.warning("Source path is not found")
                exit()
        if not is_s3_path(self.config['destination_path']):
            if not check_path(self.config['destination_path']):
                logging.warning("Destination path is not found")
                exit()
        e = datetime.strptime(eventdate, "%Y%m%d")
        if is_s3_path(self.config['source_path'] or is_s3_path(self.config['destination_path'])):
            try:
                assert(boto3)
                self.s3 = boto3.resource('s3')
            except (AssertionError, NameError):
                self.s3 = None
                logging.warning("Install boto3 for AWS integration")
                raise

        self.in_filename = self.config['source_file_prefix'].format(
            year=e.year, month=e.month, day=e.day)
        self.out_filename = self.config['destination_file_prefix'].format(
            year=e.year, month=e.month, day=e.day)
        self.ip2l = IP2Location.IP2Location()
        self.ip2l.open(self.config['ip2l_db'])
        self.enrich_country = self.enrich_country_ip2l
        self.temp_dir = self.config.get("temp_dir", "/tmp")

        if not asn_tree:
            self.load_asn_tree()

        self.chose_outputs()

        if not force_write and self.output_file_exists():
            logging.info("Already found output file for this date, aborting. "
                         "Run with --force_write to replace.")
            raise OutputExistsException(
                "Already found output file for this date, aborting. Run with "
                "--force_write to replace.")

        self.choose_inputs()

    def log_stat(self, metric, count):
        api.Metric.send(metric=metric, points=count, tags=[
            'source:' + self.feed, 'eventdate:' + self.eventdate])

    def load_asn_tree(self):
        """
        We call this if the asn_tree isn't initialised.
        """
        global asn_tree
        asn_tree = radix.Radix()
        if self.config.get("pickled_prefix_table"):
            with open(self.config.get("pickled_prefix_table"), "rb") as f:
                asn_tree = pickle.load(f)
        else:
            with open(self.config.get("prefix_table"), "r") as f:
                i = 0
                for prefix in f.readlines():
                    prefix, asn = prefix.strip().split()
                    # if verbose: print ("prefix,asn={prefix},{asn}".format(
                    # prefix=prefix,asn=asn), file=sys.stderr)
                    rnode = asn_tree.add(prefix)
                    rnode.data['origin'] = int(asn)
                    i += 1
        logging.info("Loaded prefix tree")

    def choose_inputs(self):
        if is_s3_path(self.config['source_path']):
            self.s3_input = True
            logging.info("s3 source: " + self.config['source_path'])
            (self.source_bucket, self.source_s3_path) = (
                self.config['source_path'][5:].split('/', 1))
            self.source_path = os.path.join(self.temp_dir, self.in_filename)

            s3_path = self.source_s3_path + self.in_filename

            logging.info("fetching input from s3")
            try:
                # TODO: this should do a check if output exists before
                # downloading - doing unnecessary d/ls at the moment
                self.s3.Bucket(self.source_bucket).download_file(
                    s3_path, self.source_path)
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    raise RuntimeError(
                        "Input file {} does not exist".format(s3_path))

            logging.info("input fetch complete")
        else:
            self.source_path = os.path.join(self.config['source_path'],
                                            self.in_filename)
            self.s3_input = False

    # @coroutine
    # @profile
    def input(self, target, sampling_rate):
        """
        Reads from a filename, returns an iterator
        if it's GZ then deal with that.
        """
        if self.in_filename.endswith(".gz"):
            fh = gzip.open(self.source_path, 'rt')
        else:
            fh = open(self.source_path, "r")

        csvreader = csv.DictReader(
            fh, fieldnames=self.config['in_fields'],
            delimiter=self.config.get('in_sep'), quotechar="'")

        logging.info("Sampling raw data at 1:{}".format(sampling_rate))

        for line_num, line in enumerate(csvreader):
            if line_num % LOG_OUTPUT_INTERVAL == 0:
                logging.info("Input file {}: read {} lines".format(
                    self.in_filename, line_num))

            if line_num % sampling_rate != 0:
                continue

            target.send(line)
        else:
            logging.info("Input file {}: finished".format(self.in_filename))

    def custom_filter(self, line):
        """
        Takes in the record as an array of columns, return True if it's good,
        False if it's bad.
        """
        return True

    def chose_outputs(self):
        if is_s3_path(self.config['destination_path']):
            self.s3_output = True
            logging.info("s3 destination: " + self.config['destination_path'])
            self.destpath = "/tmp/"
            # take the s3:// (5 chars) off the front,
            (self.dest_bucket, self.dest_s3path) = (
                self.config['destination_path'][5:].split('/', 1))
        else:
            self.s3_output = False
            self.destpath = self.config['destination_path']
        #  if it's a s3 then we write it to /tmp first.

    def output_file_exists(self):
        # TODO: this breaks if we're saving with .gz extn. Need to have
        # careful tracking of state of names throughout.
        if self.s3_output:
            try:
                logging.info("Trying to load {}: {}".format(
                    self.dest_bucket, self.dest_s3_path + self.out_filename + '.gz'))
                self.s3.Object(
                    self.dest_bucket, self.dest_s3_path + self.out_filename + '.gz').load()
            except ClientError as e:
                # print(e.response)
                if e.response['Error']['Code'] == "404":
                    return False
                else:
                    raise
            else:
                return True
        else:
            logging.info("Already found dest file {}".format(
                self.outfile_full_path))
            return os.path.exists(self.outfile_full_path)

    @property
    def outfile_full_path(self):
        return os.path.join(self.destpath, self.out_filename)

    @coroutine
    # @profile
    def output(self):
        """
        reads from an iterator, writes a csv/tsv to "filename"
        TODO: test!
        """

        with open(self.outfile_full_path, "w") as fp:
            csv_writer = csv.DictWriter(
                fp, self.config["out_fields"],
                delimiter=self.config.get('out_sep'), quotechar="'",
                extrasaction="ignore")
            csv_writer.writeheader()

            while True:
                line = (yield)
                csv_writer.writerow(line)

    def finalise(self):
        """
        Upload to s3 and currently list dir to show it's there.
        This should delete files once we're done too.
        """
        print(self.country_count)
        print(self.asn_count)
        if self.s3_output:
            local_path = os.path.join(self.destpath, self.out_filename)
            local_zip_path = local_path + '.gz'
            s3_zip_path = (
                os.path.join(self.dest_s3_path, self.out_filename) + '.gz')

            logging.info("Uploading to S3: " + self.dest_bucket + "/" +
                         s3_zip_path)

            with open(local_path, 'rb') as f_in, gzip.open(local_zip_path, 'wb') as f_out:  # noqa
                shutil.copyfileobj(f_in, f_out)

            self.s3.Bucket(self.dest_bucket).upload_file(
                local_zip_path, s3_zip_path)
        else:
            # TODO: compress local files too
            pass

    # @profile
    def parse_ip(self, ip_str):
        """
        Either return a valid IP or raise an exception/log a warninging
        """
        try:
            if is_private_ipv4(ip_str):
                logging.debug("{}: private IP".format(ip_str))
                raise IPValidationException("{}: {}".format(
                    ip_str, "private IP"))
        # TODO: note we throw away v6 for now! MUST fix post milestone 2.
        except ValueError:
            logging.debug("{}: invalid IP".format(ip_str))
            raise IPValidationException("{}: {}".format(ip_str, "invalid IP"))
        except:
            raise
        # return str(ip_obj)
        return ip_str

    # @profile
    def parse_ts(self, ts_str):
        """
        Either return a valid TS or raise an exception/log a warninging
        """
        try:
            ts_float = float(ts_str)
            ts_datetime = datetime.fromtimestamp(ts_float, tz=utc).replace(
                microsecond=0)
            if ts_datetime > datetime.now(tz=utc):
                logging.debug("{}: future timestamp".format(ts_str))
                raise TimestampValidationException("{}: {}".format(
                    ts_str, "time is in the future"))
            return ts_datetime.isoformat()
        except TimestampValidationException:
            logging.warning("{}: future timestamp".format(ts_str))
            raise
        except (ValueError, TypeError):
            raise TimestampValidationException("{}: {}".format(
                ts_str, "invalid timestamp"))
        except:
            raise

    # @profile
    def strip_repeat(self, ip):
        if self.config.get('remove_repeats'):
            if ip in self.ips_seen:
                logging.debug("{}: Repeat Record".format(ip))
                return True
            self.ips_seen.add(ip)
        return False

    # @profile
    def enrich_country_ip2l(self, ip):
        try:
            response = self.ip2l.get_country_short(ip).decode('utf-8')
            if response != "-":
                return response
            else:
                self.stats['no_country'] += 1
                return "XY"
        except OSError:
            logging.debug("{}: IP was malformed?".format(ip))
            self.stats['no_country'] += 1
            return "XY"

    # @profile
    def enrich_asn(self, ip):
        r_node = asn_tree.search_best(ip.strip())
        if r_node and r_node.data.get('origin'):
            return r_node.data['origin']
        else:
            logging.debug("{}: ASN not found".format(ip))
            self.stats['unknown_asn'] += 1
            return ''

    @coroutine
    # @profile
    def enrich(self, target):
        """
        TODO: long ASNs may be a DB issue? add a sanity check function in for
        ASNs to check for dotted.
        """
        while True:
            line = (yield)
            ip = line["ip"]
            line["asn"] = self.enrich_asn(ip)
            line["cc"] = self.enrich_country(ip)
            self.country_count[line["cc"]]=self.country_count.get(line["cc"],0)+1
            self.asn_count[line["asn"]]=self.country_count.get(line["cc"],0)+1
            self.stats["enriched"] += 1
            target.send(line)

    @coroutine
    # @profile
    def filter_and_parse(self, target):
        """
        TODO: make sure the config file is used vs the self. parameters
        It yields a tuple of the formatted date, risk ID and ip.
        """
        while True:
            line = (yield)

            self.stats['total'] += 1

            if self.stats['total'] % LOG_OUTPUT_INTERVAL == 0:
                logging.debug("File {}: filter / parsed {}".format(
                    self.in_filename, self.stats['total']))

            if self.strip_repeat(line["ip"]):
                self.stats['repeats'] += 1
                continue
            self.stats['parsed'] += 1
            try:
                line["ts"] = self.parse_ts(line["ts"])
                # if ts is going to be invalid it's already had an exception
                # from ts_formatted
                line["ip"] = self.parse_ip(line["ip"])
                line["risk_id"] = self.risk_id
            except IPValidationException:
                self.stats['badip'] += 1
                continue
            except TimestampValidationException:
                self.stats['badts'] += 1
                continue

            # filters are usually True for a pass, even if it does feel weird
            # syntactically
            if not self.custom_filter(line):
                self.stats['customfilter'] += 1
                continue

            target.send(line)
