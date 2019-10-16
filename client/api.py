from pathlib import Path

from schemas import FileData, FileMetadata, Registration


class DataSafeApiClient:
    def register_file_metadata(
        self: "DataSafeApiClient", file_metdata: FileMetadata
    ) -> Registration:
        raise NotImplementedError()

    def upload(
        self: "DataSafeApiClient", filepath: Path, registration: Registration
    ) -> FileData:
        raise NotImplementedError()

    def commit(
        self: "DataSafeApiClient", file_data: FileData, registration: Registration
    ) -> None:
        raise NotImplementedError()
