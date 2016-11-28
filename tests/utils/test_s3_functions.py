import etl2.utils
import pytest
import os
import boto3
from moto import mock_s3

bucket_name = "mybucket"
feed_name = "ntp-scan"
src_root = "s3://{}/raw".format(bucket_name)
dst_root = "s3://{}/clean".format(bucket_name)


@pytest.fixture()
@mock_s3
def fixture(monkeypatch):
    monkeypatch.setenv("CYBERGREEN_DEST_ROOT", src_root)
    monkeypatch.setenv("CYBERGREEN_SOURCE_ROOT", dst_root)

    mock_s3().start()
    os.environ["DD_API_KEY"] = ""
    c = etl2.utils.load_config('./tests/utils/test_data/config.json')
    r = {}
    r['s3'] = boto3.resource('s3')
    r['s3_bucket'] = r['s3'].create_bucket(Bucket=bucket_name)

    r['c'] = c
    return r
    mock_s3().stop()


def test_list_s3_files_for_feed(fixture):
    file_days = 30

    for i in range(1, file_days + 1):
        filename = 'clean/{0}/parsed.201001{1:02d}.out.gz'.format(feed_name, i)
        o = fixture['s3'].Object(bucket_name=bucket_name,
                                 key=filename)
        o.upload_file('./tests/etl2/test_data/ntp/parsed.20000101.out')
    print ([a for a in fixture['s3_bucket'].objects.all()])

    assert (
        len(etl2.utils.list_s3_files(fixture['s3'], fixture['c'], 'openntp')) ==
        file_days)


def test_list_s3_files_for_feed_with_glob(fixture):
    file_days = 30

    for i in range(1, file_days + 1):
        filename = 'clean/{0}/parsed.201502{1:02d}.out.gz'.format(feed_name, i)
        o = fixture['s3'].Object(bucket_name=bucket_name,
                                 key=filename)
        o.upload_file('./tests/etl2/test_data/ntp/parsed.20000101.out')
    print ([a for a in fixture['s3_bucket'].objects.all()])

    assert (
        len(etl2.utils.list_s3_files(fixture['s3'], fixture['c'], 'openntp', date_pattern="2015021*")) ==
        10)
