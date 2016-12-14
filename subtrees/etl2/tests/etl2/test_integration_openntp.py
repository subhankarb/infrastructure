import csv
import pytest
from .etlharness import EtlHarness


@pytest.fixture
def testopenntp():
    e = EtlHarness("openntp", "ntp-scan")
    return e


def test_file_has_header(testopenntp):
    data = "1463702401.678097|1.1.1.1|123|1|3|7|8|"
    lines, etl = testopenntp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    assert csvr[0] == ["ts", "ip", "risk_id", "asn", "cc"]


def test_single_line_valid(testopenntp):
    data = "1463702401.678097|1.1.1.1|123|1|3|7|8|"
    lines, etl = testopenntp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    # TODO: test fails - is +2 GMT correct?
    assert csvr[1] == ["2016-05-20T00:00:01+00:00", "1.1.1.1", "2", "27947", "AU"]


def test_double_line_valid(testopenntp):
    data = (
        "1463702401.678097|1.1.1.1|123|1|3|7|8|\n"
        "1463702402.678097|2.2.2.2|123|1|3|7|8|"
    )
    lines, etl = testopenntp._get_etl_output(data)
    csvr = [l for l in csv.reader(lines)]
    assert csvr[1] == ["2016-05-20T00:00:01+00:00", "1.1.1.1", "2", "27947", "AU"]
    assert csvr[2] == ["2016-05-20T00:00:02+00:00", "2.2.2.2", "2", "3215", "FR"]


def test_extras_data_handled(testopenntp):
    pass


def test_duplicate_lines_removed(testopenntp):
    data = (
        "1463702401.678097|1.1.1.1|123|1|3|7|8|\n"
        "1463802402.678097|1.1.1.1|123|1|3|7|8|"
    )
    lines, etl = testopenntp._get_etl_output(data)
    csvr = [l for l in csv.reader(lines)]
    assert len(csvr[1:]) == 1
    assert etl.stats["repeats"] == 1
    assert etl.stats["total"] == 2


def test_bad_timestamp(testopenntp):
    # test invalid data first to make sure we keep processing
    data = (
        "notatimestamp|2.2.2.2|123|1|3|7|8|\n"
        "1463702401.678097|1.1.1.1|123|1|3|7|8|"
    )
    lines, etl = testopenntp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    print(csvr)

    assert len(csvr[1:]) == 1
    assert etl.stats["total"] == 2
    assert csvr[1][1] == "1.1.1.1"
    assert etl.stats["enriched"] == 1
    assert etl.stats["parsed"] == 2
    assert etl.stats["badts"] == 1


def test_bad_ip_v4(testopenntp):
    # test invalid data first to make sure we keep processing
    data = (
        "1463702400.678097|x.2.2.2|123|1|3|7|8|\n"
        "1463702401.678097|1.1.1.1|123|1|3|7|8|"
    )
    lines, etl = testopenntp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    print(csvr)

    assert len(csvr[1:]) == 1
    assert csvr[1][1] == "1.1.1.1"
    assert etl.stats["total"] == 2
    assert etl.stats["enriched"] == 1
    assert etl.stats["parsed"] == 2
    assert etl.stats["badip"] == 1