from unittest import skip
import csv
import nose
from .base_etl_class import AbstractEtlTest

"""
IP1:53:1.1.1.1:NULL:1470096003.687349:2:1:0:0
IP1:53:2.2.2.2:NULL:1470096004.847619:2:1:0:0

"""
@nose.tools.istest
class TestOpendnsEtl(AbstractEtlTest):
    def setUp(self):
        self.source_name = "opendns"
        self.out_prefix = "dns-scan"

        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_file_has_header(self):
        data = "IP1:53:1.1.1.1:NULL:1470096003.687349:2:1:0:0"
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]
        self.assertListEqual(csvr[0], ["ts", "ip", "risk_id", "asn", "cc"])

    def test_single_line_valid(self):
        data = "IP1:53:1.1.1.1:NULL:1470096003.687349:2:1:0:0"
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]
        # TODO: test fails - is +2 GMT correct?
        self.assertListEqual(
            csvr[1],
            ["2016-08-02T00:00:03+00:00", "1.1.1.1", "1", "27947", "AU"])

    def test_double_line_valid(self):
        data = (
            "IP1:53:1.1.1.1:NULL:1470096003.687349:2:1:0:0\n"
            "IP1:53:2.2.2.2:NULL:1470096004.847619:2:1:0:0"
        )
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]

        self.assertListEqual(
            csvr[1],
            ["2016-08-02T00:00:03+00:00", "1.1.1.1", "1", "27947", "AU"])

        self.assertListEqual(
            csvr[2],
            ["2016-08-02T00:00:04+00:00", "2.2.2.2", "1", "3215", "FR"])

    def test_extras_data_handled(self):
        pass

    def test_duplicate_lines_removed(self):
        data = (
            "IP1:53:1.1.1.1:NULL:1470096003.687349:2:1:0:0\n"
            "IP1:53:1.1.1.1:NULL:1470096004.847619:2:1:0:0"
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
            "IP1:53:2.2.2.2:NULL:notatimestamp:2:1:0:0\n"
            "IP1:53:1.1.1.1:NULL:1470096004.847619:2:1:0:0"
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
            "IP1:53:500.23.256.453:NULL:1470096003.687349:2:1:0:0\n"
            "IP1:53:localhost:NULL:1470096003.687349:2:1:0:0\n"
            "IP1:53:1.1.1.1:NULL:1470096004.847619:2:1:0:0"
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
