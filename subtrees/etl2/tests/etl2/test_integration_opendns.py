import csv
import pytest
from .etlharness import EtlHarness


@pytest.fixture
def testopendns():
    e = EtlHarness("opendns", "dns-scan")
    return e


def test_file_has_header(testopendns):
    data = "IP1:53:1.1.1.1:NULL:1470096003.687349:2:1:0:0"
    lines, etl = testopendns._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    assert csvr[0] == ["ts", "ip", "risk_id", "asn", "cc"]
    assert csvr[0] != ["ip", "risk_id", "asn", "cc", "ts"]
    assert csvr[0] != ["ip", "risk_id"]


def test_single_line_valid(testopendns):
    data = "IP1:53:1.1.1.1:NULL:1470096003.687349:2:1:0:0"
    lines, etl = testopendns._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    # TODO: test fails - is +2 GMT correct?
    assert csvr[1] == ["2016-08-02T00:00:03+00:00", "1.1.1.1", "1", "27947", "AU"]


def test_double_line_valid(testopendns):
    data = (
        "IP1:53:1.1.1.1:NULL:1470096003.687349:2:1:0:0\n"
        "IP1:53:2.2.2.2:NULL:1470096004.847619:2:1:0:0"
    )
    lines, etl = testopendns._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]

    assert csvr[1] == ["2016-08-02T00:00:03+00:00", "1.1.1.1", "1", "27947", "AU"]
    assert csvr[2] == ["2016-08-02T00:00:04+00:00", "2.2.2.2", "1", "3215", "FR"]


def test_duplicate_lines_removed(testopendns):
    data = (
        "IP1:53:1.1.1.1:NULL:1470096003.687349:2:1:0:0\n"
        "IP1:53:1.1.1.1:NULL:1470096004.847619:2:1:0:0"
    )
    lines, etl = testopendns._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    print(csvr)

    assert len(csvr[1:]) == 1
    assert etl.stats["repeats"] == 1
    assert etl.stats["total"] == 2


def test_bad_timestamp(testopendns):
    # test invalid data first to make sure we keep processing
    data = (
        "IP1:53:2.2.2.2:NULL:notatimestamp:2:1:0:0\n"
        "IP1:53:1.1.1.1:NULL:1470096004.847619:2:1:0:0"
    )
    lines, etl = testopendns._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    print(csvr)

    assert len(csvr[1:]) == 1
    assert etl.stats["total"] == 2
    assert csvr[1][1] == "1.1.1.1"
    assert etl.stats["enriched"] == 1
    assert etl.stats["parsed"] == 2
    assert etl.stats["badts"] == 1


def test_bad_ip_v4(testopendns):
    # test invalid data first to make sure we keep processing
    data = (
        "IP1:53:500.23.256.453:NULL:1470096003.687349:2:1:0:0\n"
        "IP1:53:localhost:NULL:1470096003.687349:2:1:0:0\n"
        "IP1:53:1.1.1.1:NULL:1470096004.847619:2:1:0:0"
    )
    lines, etl = testopendns._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    print(csvr)

    assert len(csvr[1:]) == 1
    assert csvr[1][1] == "1.1.1.1"
    assert etl.stats["total"] == 3
    assert etl.stats["enriched"] == 1
    assert etl.stats["parsed"] == 3
    assert etl.stats["badip"] == 2
