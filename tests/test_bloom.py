import rottl

from unittest import mock


class TestRotatingTTLBloom:

    def test_basic_add_and_contains(self):
        r_bloom = rottl.RotatingTTLBloom(
            ttl=60.0,
            num_buckets=3,
            bucket_capacity=100,
            bucket_fpr=0.01,
        )
        r_bloom.add("apple")

        assert "apple" in r_bloom
        assert "orange" not in r_bloom

    @mock.patch("time.monotonic")
    def test_item_expires_after_ttl(self, mock_monotonic):
        mock_monotonic.return_value = 100.0

        r_bloom = rottl.RotatingTTLBloom(
            ttl=10.0,
            num_buckets=2,
            bucket_capacity=100,
            bucket_fpr=0.01,
        )
        r_bloom.add("item1")

        mock_monotonic.return_value = 106.0
        r_bloom.add("item2")

        mock_monotonic.return_value = 111.0
        assert "item1" not in r_bloom
        assert "item2" in r_bloom

    def test_maybe_rotate_by_saturation(self):
        r_bloom = rottl.RotatingTTLBloom(
            ttl=60.0,
            num_buckets=3,
            bucket_capacity=2,
            bucket_fpr=0.01,
        )

        for i in range(10):
            r_bloom.add(i + 0.5)

        # Filter is saturated, validate rotation occurred
        assert r_bloom.maybe_rotate_by_saturation()

        # Filter isn't saturated, validate rotation didn't occurred
        assert not r_bloom.maybe_rotate_by_saturation()

    @mock.patch("time.monotonic")
    def test_approx_len(self, mock_monotonic):
        mock_monotonic.return_value = 0.0

        r_bloom = rottl.RotatingTTLBloom(
            ttl=60.0,
            num_buckets=4,
            bucket_capacity=2_000,
            bucket_fpr=0.001,
        )

        assert r_bloom.approx_len() == 0

        for i in range(1_000):
            r_bloom.add(i)

        assert r_bloom.approx_len() > 100

        # validate we don't count expired buckets
        mock_monotonic.return_value = 120.0
        assert r_bloom.approx_len() == 0

    def test_clear_removes_all_elements(self):
        r_bloom = rottl.RotatingTTLBloom(
            ttl=60.0,
            num_buckets=2,
            bucket_capacity=10,
            bucket_fpr=0.001,
        )

        r_bloom.add(0)
        r_bloom.clear()

        assert 0 not in r_bloom

    def test_repr(self):
        r_bloom = rottl.RotatingTTLBloom(
            ttl=60.0,
            num_buckets=4,
            bucket_capacity=10_000,
            bucket_fpr=0.001,
        )

        assert repr(r_bloom) == (
            "<RotatingTTLBloom(ttl=60.0, num_buckets=1/4, bucket_capacity=10000, bucket_fpr=0.001)>"
        )
