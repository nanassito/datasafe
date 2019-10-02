import asyncio
import math
from dataclasses import dataclass
from itertools import chain
from typing import List, NewType

Path = NewType("Path", str)
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


def fetch_client_config() -> Config:
    """Read the user configuration."""
    # TODO: Implement
    return Config(
        user=User("not_a_valid_token"),
        sources=[
            Source(path="~/code/"),
            Source(path="/home/dorian/Videos/"),
        ],
    )


def read_filesystem_metadata(source: Source) -> List[FileMetadata]:
    """List all files in the source.

    We not only list the files but also get their signatures. Since computing the
    signature of a file is an expensive operation, we use the os.stat() properties
    of the file to check against a cache to get a sense of whether the file has
    been updated or not.
    """
    raise NotImplementedError()


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
    fs_metadata = list(
        chain(*[read_filesystem_metadata(source) for source in config.sources])
    )
    fs_data = await api.get_destinations(fs_metadata)
    commit_fs = await asyncio.gather([process_file(file) for file in fs_data])
    await api.commit(commit_fs)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
