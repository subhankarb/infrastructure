from unittest import TestCase
# from ETL import etl_process
import ETL
import os
import tempfile
import csv
import nose


@nose.tools.nottest
class AbstractEtlTest(TestCase):
    def setUp(self):
        root_dir = tempfile.mkdtemp()

        self.source_root = os.path.join(root_dir, "raw")
        self.source_dir = os.path.join(self.source_root, "ntp-scan")
        self.dest_root = os.path.join(root_dir, "clean")
        self.dest_dir = os.path.join(self.dest_root, "ntp-scan")

        os.makedirs(self.source_dir)
        os.makedirs(self.dest_dir)
        print(self.source_dir, self.dest_dir)

        # doesn't effect shell env
        os.environ["CYBERGREEN_SOURCE_ROOT"] = self.source_root
        os.environ["CYBERGREEN_DEST_ROOT"] = self.dest_root
        os.environ["DD_API_KEY"] = ""

    def _write_source_file(self, file_name, data):
        file_path = os.path.join(self.source_dir, file_name)

        with open(file_path, "w") as f:
            f.write(data)

    def _read_dest_file(self, file_name):
        file_path = os.path.join(self.dest_dir, file_name)

        with open(file_path, "r") as f:
            return f.readlines()

    def _get_etl_output(self, data):
        self._write_source_file("parsed.20000101.out", data)

        etl = ETL.etl_process(eventdate="20000101", source=self.source_name,
                        config_path="configs/config.json", use_datadog=False)

        lines = self._read_dest_file("{}.20000101.csv".format(self.out_prefix))

        return lines, etl


@nose.tools.istest
class TestOpenntpEtl(AbstractEtlTest):
    def setUp(self):
        self.source_name = "openntp"
        self.out_prefix = "ntp-scan"

        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_file_has_header(self):
        data = "1463702401.678097|1.1.1.1|123|1|3|7|8|"
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]
        self.assertListEqual(csvr[0], ["ts", "ip", "risk_id", "asn", "cc"])

    def test_single_line_valid(self):
        data = "1463702401.678097|1.1.1.1|123|1|3|7|8|"
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]
        # TODO: test fails - is +2 GMT correct?
        self.assertListEqual(
            csvr[1],
            ["2016-05-20T00:00:01+00:00", "1.1.1.1", "2", "27947", "AU"])

    def test_double_line_valid(self):
        data = (
            "1463702401.678097|1.1.1.1|123|1|3|7|8|\n"
            "1463702402.678097|2.2.2.2|123|1|3|7|8|"
        )
        lines, etl = self._get_etl_output(data)

        csvr = [l for l in csv.reader(lines)]

        self.assertListEqual(
            csvr[1],
            ["2016-05-20T00:00:01+00:00", "1.1.1.1", "2", "27947", "AU"])

        self.assertListEqual(
            csvr[2],
            ["2016-05-20T00:00:02+00:00", "2.2.2.2", "2", "3215", "FR"])

    def test_extras_data_handled(self):
        pass

    def test_duplicate_lines_removed(self):
        data = (
            "1463702401.678097|1.1.1.1|123|1|3|7|8|\n"
            "1463802402.678097|1.1.1.1|123|1|3|7|8|"
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
            "notatimestamp|2.2.2.2|123|1|3|7|8|\n"
            "1463702401.678097|1.1.1.1|123|1|3|7|8|"
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
            "1463702400.678097|x.2.2.2|123|1|3|7|8|\n"
            "1463702401.678097|1.1.1.1|123|1|3|7|8|"
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

    def test_not_enough_raw_cols(self):
        pass
