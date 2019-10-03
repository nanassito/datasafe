from tempfile import NamedTemporaryFile

import pytest
from pytest_mock import mocker

from client.datasafe import (
    FileMetadata,
    FsMetadataCache,
    Path,
    Signature,
    Source,
    read_all_filesystem_metadata,
)


def test_cache_survives_restart():
    metadata = FileMetadata(
        path=Path("value"), signature=Signature("value"), size_bytes=42
    )
    with NamedTemporaryFile() as fd:
        with FsMetadataCache(fd.name) as cache:
            cache[Path("key")] = metadata
        with FsMetadataCache(fd.name) as cache:
            assert metadata == cache["key"]


def test_read_filesystem_metadata_shares_cache(mocker):
    read_fs_meta = mocker.patch("client.datasafe.read_filesystem_metadata")
    read_all_filesystem_metadata([Source(Path("path1")), Source(Path("path2"))])
    assert len({id(c[0][1]) for c in read_fs_meta.call_args_list}) == 1
