import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
from pytest_mock import mocker

from client import datasafe


def test_cache_survives_restart():
    metadata = datasafe.FileMetadata(
        path=Path("value"),
        signature=datasafe.Signature("value"),
        size_bytes=42,
        os_stat=os.stat_result(range(10)),
    )
    with NamedTemporaryFile() as fd:
        with datasafe.FsMetadataCache(Path(fd.name)) as cache:
            cache[Path("key")] = metadata
        with datasafe.FsMetadataCache(Path(fd.name)) as cache:
            assert metadata == cache[Path("key")]
