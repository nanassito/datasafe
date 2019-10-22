from unittest.mock import Mock

import pytest

import api


@pytest.mark.parametrize(
    ("method", "num_args"),
    [("register_file_metadata", 1), ("upload", 2), ("commit", 2)],
)
def test_ensure_client_is_configured(method, num_args):
    with pytest.raises(AssertionError):
        getattr(api.DataSafeApiClient, method)(*[Mock() for _ in range(num_args)])
