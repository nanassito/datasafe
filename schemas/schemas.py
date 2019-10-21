"""Schemas shared across multiple components."""

import os
from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path
from typing import List, NewType, Tuple, TypedDict

Signature = NewType("Signature", str)


class SerializedFileMetadata(TypedDict):
    path: str
    signature: str
    size_bytes: int
    os_stats: Tuple[int, ...]


@dataclass
class FileMetadata:
    path: Path
    signature: Signature
    size_bytes: int
    os_stats: os.stat_result

    @staticmethod
    def from_dict(spec: SerializedFileMetadata) -> "FileMetadata":
        return FileMetadata(
            path=Path(spec["path"]),
            signature=Signature(spec["signature"]),
            size_bytes=int(spec["size_bytes"]),
            os_stats=os.stat_result(spec["os_stats"]),
        )

    def to_dict(self: "FileMetadata") -> SerializedFileMetadata:
        return SerializedFileMetadata(
            {
                "path": str(self.path),
                "signature": str(self.signature),
                "size_bytes": int(self.size_bytes),
                "os_stats": tuple(self.os_stats),  # type: ignore
            }
        )


class SerializedBlock(TypedDict):
    signature: str
    url: str
    is_uploaded: bool


@dataclass
class Block:
    signature: Signature
    url: str
    is_uploaded: bool

    @staticmethod
    def from_dict(spec: SerializedBlock) -> "Block":
        return Block(
            signature=Signature(spec["signature"]),
            url=str(spec["url"]),
            is_uploaded=bool(spec["is_uploaded"]),
        )

    def to_dict(self: "Block") -> SerializedBlock:
        return SerializedBlock(
            {
                "signature": str(self.signature),
                "url": str(self.url),
                "is_uploaded": bool(self.is_uploaded),
            }
        )


class SerializedFileData(TypedDict):
    metadata: SerializedFileMetadata
    blocks: List[SerializedBlock]


@dataclass
class FileData:
    metadata: FileMetadata
    blocks: List[Block]

    @staticmethod
    def from_dict(spec: SerializedFileData) -> "FileData":
        return FileData(
            metadata=FileMetadata.from_dict(spec["metadata"]),
            blocks=[Block.from_dict(b) for b in spec["blocks"]],
        )

    def to_dict(self: "FileData") -> SerializedFileData:
        return SerializedFileData(
            {
                "metadata": self.metadata.to_dict(),
                "blocks": [b.to_dict() for b in self.blocks],
            }
        )


class SerializedRegistration(TypedDict):
    registration_id: str
    file_data: SerializedFileData
    storage_creds: str


@dataclass
class Registration:
    registration_id: str
    file_data: FileData
    storage_creds: str

    @staticmethod
    def from_dict(spec: SerializedRegistration) -> "Registration":
        return Registration(
            registration_id=str(spec["registration_id"]),
            file_data=FileData.from_dict(spec["file_data"]),
            storage_creds=str(spec["storage_creds"]),
        )

    def to_dict(self: "Registration") -> SerializedRegistration:
        return SerializedRegistration(
            {
                "registration_id": self.registration_id,
                "file_data": self.file_data.to_dict(),
                "storage_creds": self.storage_creds,
            }
        )


Credential = namedtuple("Credential", ("username", "password"))
