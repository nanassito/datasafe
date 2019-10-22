from pathlib import Path
from typing import Type
from dataclasses import dataclass
import requests

from schemas import Credential, FileData, FileMetadata, Registration


@dataclass
class ApiClientConfig:
    credential: Credential
    api_url: str


class DataSafeApiClient:
    _CONFIGURED: bool = False
    _API: str = ""
    _CREDS: Credential = Credential("undefined", "undefined")

    @classmethod
    def configure(
        cls: Type["DataSafeApiClient"],
        client_config: ApiClientConfig,
    ) -> None:
        cls._CREDS = client_config.credential
        cls._API = client_config.api_url.rstrip("/")
        cls._CONFIGURED = True

    @classmethod
    def register_file_metadata(
        cls: Type["DataSafeApiClient"], file_metadata: FileMetadata
    ) -> Registration:
        assert cls._CONFIGURED, "Api client is not `.configured()`"
        resp = requests.post(
            cls._API + "/register_file_metadata",
            auth=cls._CREDS,
            data=file_metadata.to_dict(),
        )
        assert 200 <= resp.status_code < 300, str(resp)
        return Registration.from_dict(resp.json())

    @classmethod
    def upload(
        cls: Type["DataSafeApiClient"], filepath: Path, registration: Registration
    ) -> FileData:
        assert cls._CONFIGURED, "Api client is not `.configured()`"
        raise NotImplementedError()

    @classmethod
    def commit(
        cls: Type["DataSafeApiClient"], file_data: FileData, registration: Registration
    ) -> None:
        assert cls._CONFIGURED, "Api client is not `.configured()`"
        raise NotImplementedError()
