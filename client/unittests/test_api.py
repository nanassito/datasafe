from unittest.mock import Mock, call

import pytest
from pytest_mock import mocker  # noqa: F401

import api
import schemas


@pytest.mark.parametrize(
    ("method", "num_args"),
    [("register_file_metadata", 1), ("upload", 2), ("commit", 1)],
)
def test_ensure_client_is_configured(method, num_args):
    with pytest.raises(AssertionError):
        getattr(api.DataSafeApiClient, method)(*[Mock() for _ in range(num_args)])


def test_register_file_metadata(mocker):  # noqa: F811
    post = mocker.patch("api.requests.post")
    post.return_value.status_code = 200
    deserializer = mocker.patch("api.Registration.from_dict")
    creds = ("username", "password")
    url = "https://unit.test/api"
    api.DataSafeApiClient.configure(
        api.ApiClientConfig(schemas.Credential(*creds), url + "/")
    )

    file_metadata = Mock()
    api.DataSafeApiClient.register_file_metadata(file_metadata)

    assert deserializer.called
    assert post.call_args == call(
        f"{url}/register_file_metadata", auth=creds, data=file_metadata.to_dict()
    )


def test_register_file_metadata_fails(mocker):  # noqa: F811
    post = mocker.patch("api.requests.post")
    post.return_value.status_code = 500

    with pytest.raises(AssertionError):
        api.DataSafeApiClient.register_file_metadata(Mock())


def test_commit(mocker):  # noqa: F811
    post = mocker.patch("api.requests.post")
    post.return_value.status_code = 204
    creds = ("username", "password")
    url = "https://unit.test/api"
    api.DataSafeApiClient.configure(
        api.ApiClientConfig(schemas.Credential(*creds), url + "/")
    )

    api.DataSafeApiClient.commit("registration_id")

    assert post.call_args == call(f"{url}/commit/registration_id", auth=creds)
