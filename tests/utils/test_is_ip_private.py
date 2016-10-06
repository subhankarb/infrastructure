import pytest

from etl2.utils import is_private_ipv4


def test_valid_public():
    assert not is_private_ipv4("1.1.1.1")


def test_valid_192_168():
    assert is_private_ipv4("192.168.3.6")


def test_valid_172():
    assert is_private_ipv4("172.16.8.2")


def test_valid_10():
    assert is_private_ipv4("10.5.2.3")


def test_valid_v6():
    with pytest.raises(ValueError):
        assert is_private_ipv4("2001:db8:85a3:0:0:8a2e:370:7334")


def test_invalid_v4():
    with pytest.raises(ValueError):
        assert is_private_ipv4("1.1.1.a")
