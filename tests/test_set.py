import pytest
import rottl

from unittest import mock


@pytest.mark.parametrize("enable_history_fast_reject", [True, False])
class TestRotatingTTLSet:

    def test_basic_add_and_contains(self, enable_history_fast_reject):
        r_set = rottl.RotatingTTLSet(
            ttl=60.0,
            num_buckets=3,
            bucket_capacity=10,
            enable_history_fast_reject=enable_history_fast_reject,
        )
        r_set.add("apple")

        assert "apple" in r_set
        assert "orange" not in r_set

    @mock.patch("time.monotonic")
    def test_item_expires_after_ttl(self, mock_monotonic, enable_history_fast_reject):
        mock_monotonic.return_value = 100.0

        r_set = rottl.RotatingTTLSet(
            ttl=10.0,
            num_buckets=2,
            bucket_capacity=10,
            enable_history_fast_reject=enable_history_fast_reject,
        )
        r_set.add("item1")

        mock_monotonic.return_value = 106.0
        r_set.add("item2")

        assert "item1" in r_set
        assert "item2" in r_set

        mock_monotonic.return_value = 111.0
        assert "item1" not in r_set
        assert "item2" in r_set

    def test_capacity_enforcement(self, enable_history_fast_reject):
        r_set = rottl.RotatingTTLSet(
            ttl=60.0,
            num_buckets=2,
            bucket_capacity=1,
            enable_history_fast_reject=enable_history_fast_reject,
        )

        for i in range(3):
            r_set.add(i)

        assert 0 not in r_set
        assert 1 in r_set
        assert 2 in r_set

    @mock.patch("time.monotonic")
    def test_get_active_bucket_len(self, mock_monotonic, enable_history_fast_reject):
        mock_monotonic.return_value = 0.0

        r_set = rottl.RotatingTTLSet(
            ttl=60.0,
            num_buckets=2,
            bucket_capacity=2,
            enable_history_fast_reject=enable_history_fast_reject,
        )

        assert r_set.get_active_bucket_len() == 0

        r_set.add(True)
        r_set.add(False)
        assert r_set.get_active_bucket_len() == 2

        r_set.add(True)
        assert r_set.get_active_bucket_len() == 1

        # validate we don't count expired buckets
        mock_monotonic.return_value = 120.0
        assert r_set.get_active_bucket_len() == 0

    def test_on_rotate_callbacks(self, enable_history_fast_reject):
        r_set = rottl.RotatingTTLSet(
            ttl=60.0,
            num_buckets=2,
            bucket_capacity=2,
            enable_history_fast_reject=enable_history_fast_reject,
        )

        cb_count = 0

        def _on_rotate_cb():
            nonlocal cb_count
            cb_count += 1

        r_set.add_on_rotate_callback(_on_rotate_cb)

        for i in range(5):
            r_set.add(i)

        assert cb_count == 2

        cb_count = 0
        r_set.clear_on_rotate_callbacks()

        for i in range(5):
            r_set.add(i)

        assert cb_count == 0

    def test_clear_removes_all_elements(self, enable_history_fast_reject):
        r_set = rottl.RotatingTTLSet(
            ttl=60.0,
            num_buckets=2,
            bucket_capacity=10,
            enable_history_fast_reject=enable_history_fast_reject,
        )

        r_set.add(0)
        r_set.clear()

        assert 0 not in r_set

    def test_repr(self, enable_history_fast_reject):
        r_set = rottl.RotatingTTLSet(
            ttl=60.0,
            num_buckets=4,
            bucket_capacity=10_000,
            enable_history_fast_reject=enable_history_fast_reject,
        )

        assert repr(r_set) == (
            "<RotatingTTLSet(ttl=60.0, num_buckets=1/4, bucket_capacity=10000)>"
        )
