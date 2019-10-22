import json
import logging
import os
from pathlib import Path
from types import TracebackType
from typing import Dict, Type

from schemas import FileMetadata

_LOG = logging.getLogger(__name__)


class FsMetadataCache:
    """Implements a persistent key-value cache for the fs metadata."""

    __slots__ = ("_path", "_data")

    def __init__(self: "FsMetadataCache", path: Path = Path("./datasafe.db")) -> None:
        # TODO: Fix default db path to go to a user directory
        self._path = path
        if not path.exists() or path.stat().st_size == 0:
            # Init the db file if it doesn't exists
            _LOG.warning(
                f"Couldn't find an existing cache. Creating a new one in {path}."
            )
            with open(path, "w") as fd:
                json.dump({}, fd)
        assert os.access(path, os.R_OK), f"Make sure {path} is readable."
        assert os.access(path, os.W_OK), f"Make sure {path} is writable."

    def __enter__(self: "FsMetadataCache") -> Dict[Path, FileMetadata]:
        with open(self._path) as fd:
            self._data = {
                Path(key): FileMetadata.from_dict(spec)
                for key, spec in json.load(fd).items()
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
                    str(path): file_metadata.to_dict()
                    for path, file_metadata in self._data.items()
                },
                fd,
            )
        _LOG.info(f"Saved cache to disk with {len(self._data)} values.")
