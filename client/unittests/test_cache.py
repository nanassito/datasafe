import os
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from cache import FsMetadataCache
from schemas import FileMetadata, Signature


def test_cache_survives_restart():
    metadata = FileMetadata(
        path=Path("value"),
        signature=Signature("value"),
        size_bytes=42,
        os_stats=os.stat_result(range(10)),
    )
    with NamedTemporaryFile() as fd:
        with FsMetadataCache(Path(fd.name)) as cache:
            cache[Path("key")] = metadata
        with FsMetadataCache(Path(fd.name)) as cache:
            assert metadata == cache[Path("key")]


def test_cache_initialize_empty_file():
    with NamedTemporaryFile() as fd:
        with FsMetadataCache(Path(fd.name)) as cache:
            assert cache == {}


def test_cache_create_file_if_not_found():
    with TemporaryDirectory() as tempdir:
        with FsMetadataCache(Path(tempdir) / "not_a_file") as cache:
            assert cache == {}
