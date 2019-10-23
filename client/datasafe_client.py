"""Datasafe client

This runs on the client's machine to make sure files are tracked and backed up properly.
"""
import logging
import os
from argparse import ArgumentParser
from dataclasses import dataclass
from hashlib import sha256
from itertools import chain
from pathlib import Path
from typing import Dict, Iterator, List

from argparse_logging import add_logging_arguments

from api import DataSafeApiClient, ApiClientConfig
from cache import FsMetadataCache
from schemas import FileMetadata, Signature, Credential

_LOG = logging.getLogger(__name__)


@dataclass
class Source:
    root: Path


@dataclass
class UserConfig:
    sources: List[Source]
    api: ApiClientConfig

    @staticmethod
    def init_from_file() -> "UserConfig":  # pragma: no cover
        # TODO: Implement this
        return UserConfig(
            sources=[Source(Path("/home/dorian/python/Python-3.8.0/"))],
            api=ApiClientConfig(
                credential=Credential("not-a-user", "not-a-credential"),
                api_url="https://localhost:9090",
            ),
        )


def read_file_metadata(
    filepath: Path, cached: FileMetadata, block_size_bytes: int = 4 * 2 ** 20
) -> FileMetadata:
    """Read file metadata from disk or cache.

    The caching mechanism is not implemented yet so we always scan the entire file.
    """
    stats = filepath.stat()
    if getattr(cached, "os_stats", object()) == stats:
        _LOG.info(f"Using cached metadata for file {filepath}.")
        return cached
    file_hash = sha256()
    with open(filepath, "rb") as fd:
        while True:
            data = fd.read(block_size_bytes)
            if not data:
                break
            file_hash.update(data)
    return FileMetadata(
        path=filepath,
        signature=Signature(file_hash.hexdigest()),
        os_stats=stats,
        size_bytes=stats.st_size,
    )


def backup_file(
    filepath: Path, cache: Dict[Path, FileMetadata]
) -> None:  # pragma: no cover
    """Backup a file onto Datasafe infrastructure."""
    # TODO: Make the read block size configurable
    # TODO: Add integration tests
    file_metadata = read_file_metadata(filepath, cache.get(filepath, None))
    cache[filepath] = file_metadata
    registration = DataSafeApiClient.register_file_metadata(file_metadata)
    DataSafeApiClient.upload(filepath, registration)
    DataSafeApiClient.commit(registration.registration_id)


def traverse_fs_from_source(source: Source) -> Iterator[Path]:
    for dirpath, dirnames, filenames in os.walk(source.root):
        for filename in filenames:
            # TODO: Handle symlink pointing to non-existant files.
            yield Path(os.path.join(dirpath, filename)).resolve(strict=True)


def backup_fs_from_sources(sources: List[Source]) -> None:  # pragma: no cover
    """Back up all files within the sources."""
    # TODO: Speed up by adding concurrency if needed.
    # TODO: Add integration tests
    filepaths = chain(*[traverse_fs_from_source(source) for source in sources])
    with FsMetadataCache() as fs_metadata_cache:
        for filepath in filepaths:
            backup_file(filepath, fs_metadata_cache)


def main():  # pragma: no cover
    # TODO: Add integration tests
    parser = ArgumentParser()
    add_logging_arguments(parser)
    user_config = UserConfig.init_from_file()
    DataSafeApiClient.configure(user_config.api)
    backup_fs_from_sources(user_config.sources)


if __name__ == "__main__":  # pragma: no cover
    main()
