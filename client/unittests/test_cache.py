import os
from pathlib import Path
from tempfile import NamedTemporaryFile

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
            
