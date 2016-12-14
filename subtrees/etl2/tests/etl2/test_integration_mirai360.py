import csv
import pytest
from .etlharness import EtlHarness


@pytest.fixture
def testmirai360():
    e = EtlHarness("mirai360", "mirai-360")
    return e


def test_file_has_header(testmirai360):
    data = "2016-10-27 00:01:23	sip=1.2.3.4	dport=23"
    lines, etl = testmirai360._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    assert csvr[0] == ["ts", "ip", "risk_id", "asn", "cc"]
    assert csvr[0] != ["ip", "risk_id", "asn", "cc", "ts"]
    assert csvr[0] != ["ip", "risk_id"]


def test_single_line_valid(testmirai360):
    data = "2016-10-27 00:01:23	sip=1.1.1.1	dport=23"
    lines, etl = testmirai360._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    assert csvr[1] == ["2016-10-27T00:01:23+00:00", "1.1.1.1", "6", "27947", "AU"]


def test_double_line_valid(testmirai360):
    data = (
        "2016-10-27 00:01:23	sip=1.1.1.1	dport=23\n"
        "2016-10-27 00:03:26	sip=2.2.2.2	dport=2323"
    )
    lines, etl = testmirai360._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]

    assert csvr[1] == ["2016-10-27T00:01:23+00:00", "1.1.1.1", "6", "27947", "AU"]
    assert csvr[2] == ["2016-10-27T00:03:26+00:00", "2.2.2.2", "6", "3215", "FR"]


def test_duplicate_lines_removed(testmirai360):
    data = (
        "2016-10-27 00:01:23	sip=1.1.1.1	dport=23\n"
        "2016-10-27 00:03:26	sip=1.1.1.1	dport=2323"
    )
    lines, etl = testmirai360._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    print(csvr)

    assert len(csvr[1:]) == 1
    assert etl.stats["repeats"] == 1
    assert etl.stats["total"] == 2


def test_bad_timestamp(testmirai360):
    # test invalid data first to make sure we keep processing
    data = (
        "2016-10-55 00:01:23	sip=6.6.6.6	dport=23\n"
        "2016-10-27 00:03:26	sip=1.1.1.1	dport=2323"
    )
    lines, etl = testmirai360._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    print(csvr)

    assert len(csvr[1:]) == 1
    assert etl.stats["total"] == 2
    assert csvr[1][1] == "1.1.1.1"
    assert etl.stats["enriched"] == 1
    assert etl.stats["parsed"] == 2
    assert etl.stats["badts"] == 1


def test_bad_ip_v4(testmirai360):
    # test invalid data first to make sure we keep processing
    data = (
        "2016-10-27 00:01:23	sip=hello	dport=23\n"
        "2016-10-27 00:02:25	sip=455.455.455.1	dport=23\n"
        "2016-10-27 00:03:26	sip=2.2.2.2	dport=2323"
    )
    lines, etl = testmirai360._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    print(csvr)

    assert len(csvr[1:]) == 1
    assert csvr[1][1] == "2.2.2.2"
    assert etl.stats["total"] == 3
    assert etl.stats["enriched"] == 1
    assert etl.stats["parsed"] == 3
    assert etl.stats["badip"] == 2
