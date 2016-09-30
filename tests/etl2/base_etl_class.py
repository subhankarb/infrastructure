from unittest import TestCase
# from ETL import etl_process
import ETL
import os
import tempfile
import nose
import gzip


@nose.tools.nottest
class AbstractEtlTest(TestCase):
    def setUp(self):
        root_dir = tempfile.mkdtemp()

        self.source_root = os.path.join(root_dir, "raw")
        self.source_dir = os.path.join(self.source_root, self.out_prefix)
        self.dest_root = os.path.join(root_dir, "clean")
        self.dest_dir = os.path.join(self.dest_root, self.out_prefix)

        os.makedirs(self.source_dir)
        os.makedirs(self.dest_dir)
        print(self.source_dir, self.dest_dir)

        # doesn't effect shell env
        os.environ["CYBERGREEN_SOURCE_ROOT"] = self.source_root
        os.environ["CYBERGREEN_DEST_ROOT"] = self.dest_root
        os.environ["DD_API_KEY"] = ""

    def _write_source_file(self, file_name, data):
        file_path = os.path.join(self.source_dir, file_name)

        with gzip.open(file_path, "w") as f:
            f.write(data.encode('ascii'))

    def _read_dest_file(self, file_name):
        file_path = os.path.join(self.dest_dir, file_name)

        with open(file_path, "r") as f:
            return f.readlines()

    def _get_etl_output(self, data):
        self._write_source_file("parsed.20000101.out.gz", data)

        etl = ETL.etl_process(eventdate="20000101", source=self.source_name, config_path="configs/config.json", use_datadog=False)

        lines = self._read_dest_file("{}.20000101.csv".format(self.out_prefix))

        return lines, etl