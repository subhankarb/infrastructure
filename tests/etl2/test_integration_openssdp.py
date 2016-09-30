from unittest import skip
import csv
import nose
from .base_etl_class import AbstractEtlTest


@nose.tools.istest
class TestOpenssdpEtl(AbstractEtlTest):
    def setUp(self):
        self.source_name = "openssdp"
        self.out_prefix = "ssdp-data"
        self.ts1 = "1470096003:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '"
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_file_has_header(self):
        data = self.ts1
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]
        self.assertListEqual(csvr[0], ["ts", "ip", "risk_id", "asn", "cc"])

    def test_single_line_valid(self):
        data = self.ts1
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]
        # TODO: test fails - is +2 GMT correct?
        self.assertListEqual(
            csvr[1],
            ["2016-08-02T00:00:03+00:00", "1.1.1.1", "5", "27947", "AU"])

    def test_double_line_valid(self):
        data = (
            "1470096003:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '\n"
            "1470096004:2.2.2.2:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '"
        )
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]

        self.assertListEqual(
            csvr[1],
            ["2016-08-02T00:00:03+00:00", "1.1.1.1", "5", "27947", "AU"])

        self.assertListEqual(
            csvr[2],
            ["2016-08-02T00:00:04+00:00", "2.2.2.2", "5", "3215", "FR"])

    def test_extras_data_handled(self):
        pass

    def test_duplicate_lines_removed(self):
        data = (
            "1470096003:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '\n"
            "1470096034:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '"
        )
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]
        print(csvr)

        self.assertEqual(len(csvr[1:]), 1)  # chop CSV headers
        self.assertEqual(etl.stats["repeats"], 1)
        self.assertEqual(etl.stats["total"], 2)

    def test_bad_timestamp(self):
        # test invalid data first to make sure we keep processing
        data = (
            "notatimestamp:2.2.2.2:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '\n"
            "1470096004:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '"
        )
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]
        print(csvr)

        self.assertEqual(len(csvr[1:]), 1)  # chop CSV headers
        self.assertEqual(etl.stats["total"], 2)
        self.assertEqual(csvr[1][1], "1.1.1.1")
        self.assertEqual(etl.stats["enriched"], 1)
        self.assertEqual(etl.stats["parsed"], 2)
        self.assertEqual(etl.stats["badts"], 1)

    def test_bad_ip_v4(self):
        # test invalid data first to make sure we keep processing
        data = (
            "1470096003:400.34.234.454:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '\n"
            "1470096004:1.1.1.1:'HTTP/1.1 200 OK  CACHE-CONTROL\: max-age=120  ST\: urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  USN\: uuid\:4283f832-dce8-4e9c-9876-d99ac2ea3a7a\:\:urn\:schemas-upnp-org\:device\:InternetGatewayDevice\:1  EXT\:  SERVER\: TBS/R2 UPnP/1.0 MiniUPnPd/1.4  LOCATION\: http\://192.168.0.1\:36568/rootDesc.xml    '"
        )
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]
        print(csvr)

        self.assertEqual(len(csvr[1:]), 1)  # chop CSV headers
        self.assertEqual(csvr[1][1], "1.1.1.1")
        self.assertEqual(etl.stats["total"], 2)
        self.assertEqual(etl.stats["enriched"], 1)
        self.assertEqual(etl.stats["parsed"], 2)
        self.assertEqual(etl.stats["badip"], 1)

    @skip("Not yet implemented")
    def test_not_enough_raw_cols(self):
        pass
