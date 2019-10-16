import json
import logging
import os
from pathlib import Path
from types import TracebackType
from typing import Dict, Type

from schemas import FileMetadata, Signature

_LOG = logging.getLogger(__name__)


class FsMetadataCache:
    """Implements a persistent key-value cache for the fs metadata."""

    __slots__ = ("_path", "_data")

    def __init__(self: "FsMetadataCache", path: Path = Path("./datasafe.db")) -> None:
        # TODO: Fix default path to go to a user directory
        self._path = path
        if not path.exists() or path.stat().st_size == 0:
            # Init the db file if it doesn't exists
            _LOG.info(f"Couldn't find an existing cache. Creating a new one in {path}.")
            with open(path, "w") as fd:
                json.dump({}, fd)
        assert os.access(path, os.R_OK), f"Make sure {path} is readable."
        assert os.access(path, os.W_OK), f"Make sure {path} is writable."

    def __enter__(self: "FsMetadataCache") -> Dict[Path, FileMetadata]:
        with open(self._path) as fd:
            self._data = {
                Path(key): FileMetadata(
                    path=Path(value["path"]),
                    signature=Signature(value["signature"]),
                    size_bytes=int(value["size_bytes"]),
                    os_stats=os.stat_result(value["os_stats"]),
                )
                for key, value in json.load(fd).items()
            }
        _LOG.info(f"Initialized cache with {len(self._data)} values.")
        return self._data

    def __exit__(
        self: "FsMetadataCache",
        type: Type[BaseException],
        value: BaseException,
        traceback: TracebackType,
    ) -> None:
        with open(self._path, "w") as fd:
            json.dump(
                {
                    str(path): {
                        "path": str(file_metadata.path),
                        "signature": str(file_metadata.signature),
                        "size_bytes": file_metadata.size_bytes,
                        "os_stats": tuple(file_metadata.os_stats),
                    }
                    for path, file_metadata in self._data.items()
                },
                fd,
            )
        _LOG.info(f"Saved cache to disk with {len(self._data)} values.")
