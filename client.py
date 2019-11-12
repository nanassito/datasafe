import asyncio
import logging
import os
from argparse import ArgumentParser
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Iterator, List, Pattern, Type

from argparse_logging import add_logging_arguments

import common
import schemas
import server


@dataclass
class Source:
    root: Path
    blacklist: List[Pattern]


@dataclass
class Configuration:
    sources: List[Source]
    user: schemas.User

    @classmethod
    def from_file(cls: Type["Configuration"], configpath: Path,) -> "Configuration":
        # TODO: implement this.
        return cls(
            sources=[Source(root=Path("/home/dorian/test/"), blacklist=[])],
            user=schemas.User(schemas.Username("local@jaminais.fr"), "password"),
        )


class Client(common.Loggable):
    __slots__ = ("config", "server")

    def __init__(self: "Client", configpath: Path = Path("/etc/copieur.json")) -> None:
        super().__init__()
        self.config = Configuration.from_file(configpath)
        self.server = server.Server()

    def walk_fs(self: "Client", source: Source) -> Iterator[Path]:
        for dirpath, _dirnames, filenames in os.walk(source.root):
            for filename in filenames:
                filepath = Path(dirpath) / filename
                ignore = False
                for blacklist in source.blacklist:
                    match = blacklist.match(str(filepath))
                    if match:
                        ignore = True
                        self.log.info(
                            f"Ignoring {filepath} because it matched '{blacklist}': {match}"
                        )
                        break
                if not ignore:
                    yield filepath

    async def backup_block(self: "Client", block_content: bytes) -> schemas.Signature:
        signature = schemas.Signature(sha256(block_content).hexdigest())
        block = schemas.BlockMetadata(
            signature=signature, size_bytes=schemas.NumBytes(len(block_content))
        )
        self.log.info(f"Declaring block {signature}")
        upload_instruction = await self.server.declare_block(self.config.user, block)
        if upload_instruction is None:
            self.log.info(f"No upload instructions for block {signature}")
        else:
            self.log.info(f"Uploading block {signature}")
            # TODO: How can I force the storage class being used ?
            pass  # TODO: Upload to AWS.
        return signature

    async def backup(self: "Client", filepath: Path) -> None:
        self.log.info(f"Processing file {filepath}")
        block_signatures: List[schemas.Signature] = []
        block_size_bytes = await self.server.get_block_size_bytes()
        with open(filepath, "rb") as fd:
            while (block_content := fd.read(block_size_bytes)) :  # noqa: E231
                block_signatures.append(await self.backup_block(block_content))
        self.log.info(f"New commit for {filepath} with {len(block_signatures)} blocks.")
        await self.server.commit(
            self.config.user,
            schemas.CommitData(
                path=filepath,
                size_bytes=schemas.NumBytes(filepath.stat().st_size),
                block_signatures=block_signatures,
            ),
        )

    def main(self: "Client") -> None:
        # TODO: Get the configuration path from the cli.
        loop = asyncio.get_event_loop()
        for source in self.config.sources:
            for filepath in self.walk_fs(source):
                loop.run_until_complete(self.backup(filepath))


if __name__ == "__main__":
    parser = ArgumentParser()
    add_logging_arguments(parser)
    parser.parse_args()
    Client().main()
