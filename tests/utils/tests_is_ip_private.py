from unittest import TestCase
from nose.tools import raises

from etl2.utils import is_private_ipv4


class TestIsPrivateIP(TestCase):
    def setUp(self):
        pass

    def test_valid_public(self):
        self.assertFalse(is_private_ipv4("1.1.1.1"))

    def test_valid_192_168(self):
        self.assertTrue(is_private_ipv4("192.168.3.6"))

    def test_valid_172(self):
        self.assertTrue(is_private_ipv4("172.16.8.2"))

    def test_valid_10(self):
        self.assertTrue(is_private_ipv4("10.5.2.3"))

    @raises(ValueError)
    def test_valid_v6(self):
        is_private_ipv4("2001:db8:85a3:0:0:8a2e:370:7334")

    @raises(ValueError)
    def test_invalid_v4(self):
        is_private_ipv4("1.1.1.a")
