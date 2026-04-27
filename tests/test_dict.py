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

    @mock.patch("time.monotonic")
    def test_get_active_bucket_item_count(
        self, mock_monotonic, enable_history_fast_reject
    ):
        mock_monotonic.return_value = 0.0

        r_dict = rottl.RotatingTTLDict(
            ttl=60.0,
            num_buckets=2,
            bucket_capacity=2,
            enable_history_fast_reject=enable_history_fast_reject,
        )

        assert r_dict.get_active_bucket_item_count() == 0

        r_dict[0] = True
        r_dict[1] = True
        assert r_dict.get_active_bucket_item_count() == 2

        r_dict[0] = True
        assert r_dict.get_active_bucket_item_count() == 1

        # validate we don't count expired buckets
        mock_monotonic.return_value = 120.0
        assert r_dict.get_active_bucket_item_count() == 0

    def test_on_rotate_callbacks(self, enable_history_fast_reject):
        r_dict = rottl.RotatingTTLDict(
            ttl=60.0,
            num_buckets=2,
            bucket_capacity=2,
            enable_history_fast_reject=enable_history_fast_reject,
        )

        cb_count = 0

        def _on_rotate_cb():
            nonlocal cb_count
            cb_count += 1

        r_dict.add_on_rotate_callback(_on_rotate_cb)

        for i in range(5):
            r_dict[i] = True

        assert cb_count == 2

        cb_count = 0
        r_dict.clear_on_rotate_callbacks()

        for i in range(5):
            r_dict[i] = True

        assert cb_count == 0

    def test_clear_removes_all_elements(self, enable_history_fast_reject):
        r_dict = rottl.RotatingTTLDict(
            ttl=60.0,
            num_buckets=2,
            bucket_capacity=10,
            enable_history_fast_reject=enable_history_fast_reject,
        )

        r_dict[0] = True
        r_dict.clear()

        assert 0 not in r_dict

    @mock.patch("time.monotonic")
    def test_rotation_counters(self, mock_monotonic, enable_history_fast_reject):
        mock_monotonic.return_value = 0.0

        r_dict = rottl.RotatingTTLDict(
            ttl=60.0,
            num_buckets=2,
            bucket_capacity=10,
            enable_history_fast_reject=enable_history_fast_reject,
        )

        assert r_dict.rotations_by_ttl == 0
        assert r_dict.rotations_by_capacity == 0

        for i in range(15):
            r_dict[i] = True

        assert r_dict.rotations_by_ttl == 0
        assert r_dict.rotations_by_capacity == 1

        mock_monotonic.return_value = 80.0
        r_dict[20] = True

        assert r_dict.rotations_by_ttl == 1
        assert r_dict.rotations_by_capacity == 1

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
