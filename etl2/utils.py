from struct import unpack
from socket import AF_INET, inet_pton
import json
from string import Template
import os
import re

def load_env_var(env_name):
    try:
        return os.environ[env_name]
    except KeyError:
        raise ValueError("{} is required as an environment variable")


def load_env_var_or_none(env_name):
    return os.environ.get(env_name)


def is_private_ipv4(ip_str):
    """
        TODO: handle ipv6 in a single function that's performant.
    """

    try:
        f = unpack('!I', inet_pton(AF_INET, ip_str))[0]
    except OSError:  # invalid IP
        raise ValueError("{} is not a valid IPv4 address")

    private = (
        [2130706432, 4278190080],  # 127.0.0.0,   255.0.0.0
        [3232235520, 4294901760],  # 192.168.0.0, 255.255.0.0
        [2886729728, 4293918720],  # 172.16.0.0,  255.240.0.0
        [167772160,  4278190080],  # 10.0.0.0,    255.0.0.0
    )
    for net in private:
        if (f & net[1]) == net[0]:
            return True
    return False


def is_private_ip(ip_obj):
    """
    ip_obj is ipaddress.IP*
    """
    return ip_obj.is_private


def is_private_ip_ipy(ip_str):
    # on m, gives 10k rec/sec, not quite as good as above int range check
    from IPy import IP
    ip = IP(ip_str)
    ip_type = ip.iptype()
    return ip_type in (['LOOPBACK', 'PRIVATE'])


def is_s3_path(str):
    return str.startswith("s3://")


def split_s3_path(s3_address):
    if not is_s3_path(s3_address):
        raise ValueError("{} is not an S3 address".format(s3_address))
    else:
        (s3_bucket, s3_path) = s3_address[5:].split('/', 1)
        return (s3_bucket, s3_path)


def list_s3_files_for_source(s3, config, source):
    """

    """
    s3_bucket, s3_path = split_s3_path(config['source'][source]['source_path'])
    remote_files = s3.Bucket(s3_bucket).objects.filter(
        Prefix=s3_path)
    pattern_re = re.compile(config['source'][source]['source_file_regex'])
    matching_files = []
    for f in remote_files:
        t = f.key[len(s3_path):]
        if len(t) > 1 and pattern_re.match(t):
            matching_files.append(f.key)
    return matching_files

def all_sources(config):
    """
    Return a list of the sources we know about.
    """
    return [i for i in config['source']]


def load_config(config_path):
    """
    Load the regular config file
    """
    with open(config_path) as f:
        template = f.read()

    try:
        config_str = Template(template).substitute(os.environ)
    except KeyError as e:
        raise ValueError(
            "An environment variable is missing: {}".format(e))
    config = json.loads(config_str)

    return config


def load_source_config(config_path, source):
    config = {}

    c = load_config(config_path)
    # TODO is this a bad idea? flatten the tree a bit.
    config = {}
    config.update(c)
    config.update(c['source'][source])
    del config['source']
    return config
