from unittest import TestCase, skip
import tempfile
from etl2.io import LocalFileHandler, S3FileHandler
import os
import os.path
import boto
from moto import mock_s3


@skip("Not yet implemented")
class TestFileIOHandler(TestCase):
    def setUp(self):
        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        self.tmp_file_path = tmpfile.name

        with open(tmpfile.name, "w") as f:
            f.write("Hello\nthere\nworld")

        self.file_dir, self.filename = os.path.split(self.tmp_file_path)
        self.lfh = LocalFileHandler(self.file_dir, self.filename)

    def tearDown(self):
        os.unlink(self.tmp_file_path)

    def test_exists(self):
        self.assertTrue(self.lfh.exists())

    def test_filename(self):
        self.assertEqual(self.lfh.filename, self.filename)

    def test_file_dir(self):
        self.assertEqual(self.lfh.file_dir, self.file_dir)

    def test_full_path(self):
        self.assertEqual(
            self.lfh.full_path, os.path.join(self.file_dir, self.filename))

    def test_file_path_arc(self):
        self.assertEqual(
            self.lfh.full_path + '.gz',
            os.path.join(self.file_dir, self.filename) + self.lfh.arc_ext)

    def test_open(self):
        with self.lfh.open() as f:
            self.assertEqual(f.readlines(), ["Hello\n", "there\n", "world"])


@skip("Not yet implemented")
class TestS3IOHandler(TestCase):
    @mock_s3
    def setUp(self):
        self.f = tempfile.NamedTemporaryFile()
        self.bucket_name = "etl-test-bucket"
        conn = boto.connect_s3()
        conn.create_bucket(self.bucket_name)

        self.file_dir, self.filename = os.path.split(self.f.name)
        self.lfh = S3FileHandler(self.bucket_name, self.file_dir, self.filename)

    def tearDown(self):
        self.f.close()

    def test_exists(self):
        self.assertTrue(self.lfh.exists())

    def test_filename(self):
        self.assertEqual(self.lfh.filename, self.filename)

    def test_file_dir(self):
        self.assertEqual(self.lfh.file_dir, self.file_dir)

    def test_full_path(self):
        self.assertEqual(
            self.lfh.full_path,
            os.path.join(self.file_dir, self.filename))

    def test_file_path_arc(self):
        self.assertEqual(
            self.lfh.full_path + '.gz',
            os.path.join(self.file_dir, self.filename) + self.lfh.arc_ext)



