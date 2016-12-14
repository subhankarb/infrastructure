import csv
import pytest
from .etlharness import EtlHarness


@pytest.fixture
def testopenssdp():
    e = EtlHarness("openssdp", "ssdp-data")
    return e


def test_file_has_header(testopenssdp):
    data = (
        "1470096003:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '"
    )
    lines, etl = testopenssdp._get_etl_output(data)
    csvr = [l for l in csv.reader(lines)]
    assert csvr[0] == ["ts", "ip", "risk_id", "asn", "cc"]


def test_single_line_valid(testopenssdp):
    data = (
        "1470096003:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '"
    )

    lines, etl = testopenssdp._get_etl_output(data)
    csvr = [l for l in csv.reader(lines)]
    assert csvr[1] == ["2016-08-02T00:00:03+00:00", "1.1.1.1", "5", "27947", "AU"]


def test_double_line_valid(testopenssdp):
    data = (
        "1470096003:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '\n"
        "1470096004:2.2.2.2:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '"
    )
    lines, etl = testopenssdp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    assert csvr[1] == ["2016-08-02T00:00:03+00:00", "1.1.1.1", "5", "27947", "AU"]
    assert csvr[2] == ["2016-08-02T00:00:04+00:00", "2.2.2.2", "5", "3215", "FR"]


def test_duplicate_lines_removed(testopenssdp):
    data = (
        "1470096003:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '\n"
        "1470096034:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '"
    )
    lines, etl = testopenssdp._get_etl_output(data)
    csvr = [l for l in csv.reader(lines)]
    assert len(csvr[1:]) == 1
    assert etl.stats["repeats"] == 1
    assert etl.stats["total"] == 2


def test_bad_timestamp(testopenssdp):
    # test invalid data first to make sure we keep processing
    data = (
        "notatimestamp:2.2.2.2:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '\n"
        "1470096004:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '"
    )
    lines, etl = testopenssdp._get_etl_output(data)
    csvr = [l for l in csv.reader(lines)]
    assert len(csvr[1:]) == 1
    assert etl.stats["total"] == 2
    assert csvr[1][1] == "1.1.1.1"
    assert etl.stats["enriched"] == 1
    assert etl.stats["parsed"] == 2
    assert etl.stats["badts"] == 1


def test_bad_ip_v4(testopenssdp):
    # test invalid data first to make sure we keep processing
    data = (
        "1470096003:400.34.234.454:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '\n"
        "1470096004:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '"
    )
    lines, etl = testopenssdp._get_etl_output(data)

    csvr = [l for l in csv.reader(lines)]
    print(csvr)

    assert len(csvr[1:]) == 1
    assert csvr[1][1] == "1.1.1.1"
    assert etl.stats["total"] == 2
    assert etl.stats["enriched"] == 1
    assert etl.stats["parsed"] == 2
    assert etl.stats["badip"] == 1
