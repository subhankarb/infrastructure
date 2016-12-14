import csv
import pytest
from .etlharness import EtlHarness


@pytest.fixture
def testopensnmp():
    e = EtlHarness("opensnmp", "snmp-data")
    return e


def test_file_has_header(testopensnmp):
    data = "1470096003:1.1.1.1:NULL:'0$     public              0 0   +    '"
    lines, etl = testopensnmp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    assert csvr[0] == ["ts", "ip", "risk_id", "asn", "cc"]


def test_single_line_valid(testopensnmp):
    data = "1470096003:1.1.1.1:NULL:'0$     public              0 0   +    '"
    lines, etl = testopensnmp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    assert csvr[1] == ["2016-08-02T00:00:03+00:00", "1.1.1.1", "4", "27947", "AU"]


def test_double_line_valid(testopensnmp):
    data = (
        "1470096003:1.1.1.1:NULL:'0$     public              0 0   +    '\n"
        "1470096004:2.2.2.2:2.2.2.2:'0$     public              0 0   +    '"
    )
    lines, etl = testopensnmp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]

    assert csvr[1] == ["2016-08-02T00:00:03+00:00", "1.1.1.1", "4", "27947", "AU"]

    assert csvr[2] == ["2016-08-02T00:00:04+00:00", "2.2.2.2", "4", "3215", "FR"]


def test_extras_data_handled(testopensnmp):
    pass


def test_duplicate_lines_removed(testopensnmp):
    data = (
        "1470096003:1.1.1.1:NULL:'0$     public              0 0   +    '\n"
        "1470096013:1.1.1.1:NULL:'0$     public              0 0   +    '"
    )
    lines, etl = testopensnmp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]

    assert len(csvr[1:]) == 1
    assert etl.stats["repeats"] == 1
    assert etl.stats["total"] == 2


def test_bad_timestamp(testopensnmp):
    # test invalid data first to make sure we keep processing
    data = (
        "notvalid:2.2.2.2:NULL:'0$     public              0 0   +    '\n"
        "1470096003:1.1.1.1:NULL:'0$     public              0 0   +    '"
    )
    lines, etl = testopensnmp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    print(csvr)

    assert len(csvr[1:]) == 1
    assert etl.stats["total"] == 2
    assert csvr[1][1] == "1.1.1.1"
    assert etl.stats["enriched"] == 1
    assert etl.stats["parsed"] == 2
    assert etl.stats["badts"] == 1


def test_bad_ip_v4(testopensnmp):
    # test invalid data first to make sure we keep processing
    data = (
        "1470096003:400.3.2.255:NULL:'0$     public              0 0   +    '\n"
        "1470096003:1.1.1.1:NULL:'0$     public              0 0   +    '"
    )
    lines, etl = testopensnmp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    print(csvr)

    assert len(csvr[1:]) == 1
    assert csvr[1][1] == "1.1.1.1"
    assert etl.stats["total"] == 2
    assert etl.stats["enriched"] == 1
    assert etl.stats["parsed"] == 2
    assert etl.stats["badip"] == 1
