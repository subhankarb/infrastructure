import etl2.utils
import pytest
import os
import tempfile
import boto3

@pytest.fixture(scope="module")
def fixture():
    root_dir = tempfile.mkdtemp()
    #source_root = os.path.join(root_dir, "raw")
    #dest_root = os.path.join(root_dir, "clean")
    # doesn't effect shell env
    #os.environ["CYBERGREEN_SOURCE_ROOT"] = source_root
    #os.environ["CYBERGREEN_DEST_ROOT"] = dest_root
    os.environ["DD_API_KEY"] = ""
    c = etl2.utils.load_config('./tests/utils/test_data/config.json')
    r = {}
    r['s3']=boto3.resource('s3')
    r['c']=c
    return r

def test_list_s3_files_for_source(fixture):
	assert len(etl2.utils.list_s3_files_for_source(fixture['s3'],fixture['c'],'openntp')) == 138