import pytest
import rottl

from unittest import mock


@pytest.mark.parametrize("enable_history_fast_reject", [True, False])
class TestRotatingTTLDict:

    def test_basic_get_set_and_contains(self, enable_history_fast_reject):
        r_dict = rottl.RotatingTTLDict(
            ttl=60.0,
            num_buckets=3,
            bucket_capacity=10,
            enable_history_fast_reject=enable_history_fast_reject,
        )
        r_dict["apple"] = True

        assert "apple" in r_dict
        assert "orange" not in r_dict

        assert r_dict["apple"] == True
        assert r_dict.get("apple") == True

        assert r_dict.get("orange") == None
        assert r_dict.get("orange", False) == False

        with pytest.raises(KeyError):
            r_dict["orange"]

    @mock.patch("time.monotonic")
    def test_item_expires_after_ttl(self, mock_monotonic, enable_history_fast_reject):
        mock_monotonic.return_value = 100.0

        r_dict = rottl.RotatingTTLDict(
            ttl=10.0,
            num_buckets=2,
            bucket_capacity=10,
            enable_history_fast_reject=enable_history_fast_reject,
        )
        r_dict["item1"] = True

        mock_monotonic.return_value = 106.0
        r_dict["item2"] = True

        assert "item1" in r_dict
        assert "item2" in r_dict

        mock_monotonic.return_value = 111.0
        assert "item1" not in r_dict
        assert "item2" in r_dict

        with pytest.raises(KeyError):
            r_dict["item1"]

    def test_capacity_enforcement(self, enable_history_fast_reject):
        r_dict = rottl.RotatingTTLDict(
            ttl=60.0,
            num_buckets=2,
            bucket_capacity=1,
            enable_history_fast_reject=enable_history_fast_reject,
        )

        for i in range(3):
            r_dict[i] = True

        assert 0 not in r_dict
        assert 1 in r_dict
        assert 2 in r_dict

    def test_repr(self, enable_history_fast_reject):
        r_dict = rottl.RotatingTTLDict(
            ttl=60.0,
            num_buckets=4,
            bucket_capacity=10_000,
            enable_history_fast_reject=enable_history_fast_reject,
        )

        assert repr(r_dict) == (
            "<RotatingTTLDict(ttl=60.0, num_buckets=1/4, bucket_capacity=10000)>"
        )
