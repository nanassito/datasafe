import asyncio
import json
import math
import os
from dataclasses import dataclass, fields
from hashlib import sha256
from itertools import chain
from multiprocessing.pool import ThreadPool
from pathlib import Path
from threading import Lock
from types import TracebackType
from typing import Dict, List, Mapping, NewType, Optional, Set, Type

Signature = NewType("Signature", str)


@dataclass
class User:
    api_token: str


@dataclass
class Source:
    path: Path


@dataclass
class Config:
    user: User
    sources: List[Source]


@dataclass
class FileMetadata:
    path: Path
    signature: Signature
    size_bytes: int
    os_stat: os.stat_result


@dataclass
class Block:
    url: str
    signature: Signature


class FileData:
    __slots__ = ("path", "signature", "block_size_bytes", "size_bytes", "blocks")

    def __init__(
        self: "FileData",
        path: Path,
        signature: Signature,
        block_size_bytes: int,
        size_bytes: int,
        blocks: List[Block],
    ) -> None:
        self.path = path
        self.signature = signature
        self.block_size_bytes = block_size_bytes
        self.size_bytes = size_bytes
        self.blocks = blocks
        self._assert_block_qty()

    def _assert_block_qty(self: "FileData") -> None:
        assert (
            len(self.blocks) == self.num_blocks
        ), f"Need exactly {self.num_blocks} blocks. Got {len(self.blocks)}."

    @property
    def num_blocks(self: "FileData") -> int:
        return math.ceil(self.size_bytes / self.block_size_bytes)


class FsMetadataCache:
    """Implements a persistent key-value cache for the fs metadata."""

    __slots__ = ("_path", "_data")

    def __init__(self: "FsMetadataCache", path: str = "./datasafe.db") -> None:
        self._path = path
        if not os.path.exists(path) or os.stat(path).st_size == 0:
            # Init the db file if it doesn't exists
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
                    os_stat=os.stat_result(value["os_stat"]),
                )
                for key, value in json.load(fd).items()
            }
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
                        "os_stat": tuple(file_metadata.os_stat),
                    }
                    for path, file_metadata in self._data.items()
                },
                fd,
            )


def fetch_client_config() -> Config:
    """Read the user configuration."""
    # TODO: Implement
    return Config(
        user=User("not_a_valid_token"),
        sources=[
            Source(path=Path("~/code/")),
            Source(path=Path("/home/dorian/Videos/")),
        ],
    )


def read_file_metadata(
    path: Path,
    cached: Optional[FileMetadata],
    block_size_bytes: int = 4096,  # TODO: Make the block size configurable
) -> FileMetadata:
    """Return metadata for a file. Optimized against the cached value."""
    stats = os.stat(path)
    if getattr(cached, "os_stat", object()) == stats:
        return cached
    file_hash = sha256()
    with open(path, "rb") as fd:
        while True:
            data = fd.read(block_size_bytes)
            if not data:
                break
            file_hash.update(data)
    return FileMetadata(
        path=path,
        signature=Signature(file_hash.hexdigest()),
        os_stat=stats,
        size_bytes=stats.st_size,
    )


def read_all_filesystem_metadata(sources: List[Source]) -> Set[FileMetadata]:
    """List all files in the sources.

    We not only list the files but also get their signatures. Since computing the
    signature of a file is an expensive operation, we use the os.stat() properties
    of the file to check against a cache to get a sense of whether the file has
    been updated or not.
    """
    # TODO: What do we do when a file isn't readable ?
    cached_fs = {}
    with FsMetadataCache() as fs_metadata_cache:
        for source in sources:
            for dirpath, _dirnames, filenames in os.walk(source.path):
                for filename in filenames:
                    path = Path(os.path.abspath(os.path.join(dirpath, filename)))
                    cached_file_metadata = fs_metadata_cache.get(path, None)
                    # It is possible we override ourselves here if two sources overlap.
                    cached_fs[path] = cached_file_metadata
        pool = ThreadPool()  # TODO: Can tune the number of processes/threads
        fs_metadata = dict(
            pool.map(
                lambda x: (x[0], read_file_metadata(*x)),
                cached_fs.items(),
                # chunksize=None,  # TODO: Can tune the chuncksize
            )
        )
        fs_metadata_cache.update(fs_metadata)
    return set(fs_metadata.values())


class Api:
    """Client side implementation of the API."""

    def __init__(self: "Api", user: User) -> None:
        self.user = user

    async def get_destinations(
        self: "Api", fs_metadata: List[FileMetadata]
    ) -> List[FileData]:
        """Asks the server where to upload the blocks for each file."""
        raise NotImplementedError()

    async def commit(self: "Api", fs: List[FileData]) -> None:
        """Tell the server all the blocks for all the files have been uploaded."""
        raise NotImplementedError()


async def process_file(file: FileData) -> FileData:
    """Make sure all blocks of a file are backed up.
    
    If the signature is unchanged, this won't do anything.
    If the signature has changed, we will upload all the blocks that needs to be.
    
    We then return a new FileData object with all the correct blocks."""
    raise NotImplementedError()


async def main():
    config = fetch_client_config()
    api = Api(config.user)
    fs_metadata = read_all_filesystem_metadata(config.sources)
    fs_data = await api.get_destinations(fs_metadata)
    commit_fs = await asyncio.gather([process_file(file) for file in fs_data])
    await api.commit(commit_fs)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
