import os
from copy import deepcopy
from tempfile import NamedTemporaryFile

import pytest
from pytest_mock import mocker

from client.datasafe import (
    FileMetadata,
    FsMetadataCache,
    Path,
    Signature,
    read_file_metadata,
)


def test_read_file_metadata_from_cache(mocker):
    path = Path("/unit/test/path")
    os_stat = os.stat_result(range(10))
    metadata = FileMetadata(path, Signature("0x42"), 42, os_stat)
    cache = {path: metadata}
    orig_cache = deepcopy(cache)
    mocker.patch("client.datasafe.os.stat", return_value=os_stat)

    result = read_file_metadata(path, cache)

    assert result == metadata
    assert orig_cache == cache


def test_read_file_metadata_cache_miss(mocker):
    with NamedTemporaryFile() as tmp_fd:
        tmp_fd.write(b"something")
        tmp_fd.flush()

        path = Path(tmp_fd.name)
        metadata = FileMetadata(
            path,
            Signature(
                "3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"
            ),
            9,
            os.stat(tmp_fd.name),
        )
        cache = {}

        result = read_file_metadata(path, cache)

    assert result == metadata
    assert cache == {path: metadata}


def test_read_file_metadata_cache_invalid(mocker):
    with NamedTemporaryFile() as tmp_fd:
        tmp_fd.write(b"something")
        tmp_fd.flush()

        path = Path(tmp_fd.name)
        metadata = FileMetadata(
            path,
            Signature(
                "3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"
            ),
            9,
            os.stat(tmp_fd.name),
        )
        cache = {
            path: FileMetadata(
                path,
                Signature(
                    "3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"
                ),
                9,
                os.stat_result(range(10)),
            )
        }

        result = read_file_metadata(path, cache)

    assert result == metadata
    assert cache == {path: metadata}
