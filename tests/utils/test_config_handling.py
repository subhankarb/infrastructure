import etl2.utils
import pytest
import os
import tempfile


@pytest.fixture(scope="module")
def config():
    root_dir = tempfile.mkdtemp()
    source_root = os.path.join(root_dir, "raw")
    dest_root = os.path.join(root_dir, "clean")
    # *shudder* TODO: fix this swapping mess
    old_source_root = os.environ["CYBERGREEN_SOURCE_ROOT"]
    old_dest_root = os.environ["CYBERGREEN_DEST_ROOT"]
    os.environ["CYBERGREEN_SOURCE_ROOT"] = source_root
    os.environ["CYBERGREEN_DEST_ROOT"] = dest_root
    os.environ["DD_API_KEY"] = ""
    c = etl2.utils.load_config('./tests/utils/test_data/config.json')
    r = {}
    r['c'] = c
    r['source_root']=source_root
    r['dest_root']=dest_root
    os.environ["CYBERGREEN_SOURCE_ROOT"] = old_source_root
    os.environ["CYBERGREEN_DEST_ROOT"] = old_dest_root
    return r


@pytest.fixture(scope="module")
def source_config():
    root_dir = tempfile.mkdtemp()
    source_root = os.path.join(root_dir, "raw")
    dest_root = os.path.join(root_dir, "clean")
    # *shudder* TODO: fix this swapping mess
    old_source_root = os.environ["CYBERGREEN_SOURCE_ROOT"]
    old_dest_root = os.environ["CYBERGREEN_DEST_ROOT"]
    os.environ["CYBERGREEN_SOURCE_ROOT"] = source_root
    os.environ["CYBERGREEN_DEST_ROOT"] = dest_root
    os.environ["DD_API_KEY"] = ""
    c = etl2.utils.load_feed_config('./tests/utils/test_data/config.json','opensnmp')
    r = {}
    r['c']=c
    r['source_root']=source_root
    r['dest_root']=dest_root
    os.environ["CYBERGREEN_SOURCE_ROOT"] = old_source_root
    os.environ["CYBERGREEN_DEST_ROOT"] = old_dest_root
    return r

def test_if_config_loaded(config):
    assert config['c']['ip2l_db'] == './IP2LOCATION-LITE-DB1.BIN'
    assert config['c'].get('nothing') == None

def test_if_source_config_loaded(source_config):
    assert source_config['c']['ip2l_db'] == './IP2LOCATION-LITE-DB1.BIN'
    assert source_config['c'].get('nothing') == None
    assert source_config['c'].get('feed') == None
    assert source_config['c'].get('risk_id') == 4
    assert source_config['c'].get("in_fields") == ["ts", "ip", "ip_secondary", "extras"]
    assert source_config['c']['source_path'] == source_config['source_root']+"/snmp-data/"


def test_if_source_path_template_filled(config):
    assert config['c']['feed']['openntp']['source_path'] == config['source_root']+"/ntp-scan/"

def test_if_dest_path_template_filled(config):
    assert config['c']['feed']['openntp']['destination_path'] == config['dest_root']+"/ntp-scan/"

def test_all_feeds_list(config):
    all_feeds = sorted(etl2.utils.all_feeds(config['c']))
    assert all_feeds == ['opendns', 'openntp', 'opensnmp', 'openssdp', 'spam']
    assert type(all_feeds) == list
