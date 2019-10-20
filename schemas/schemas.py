"""Schemas shared across multiple components."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, NewType, Dict, Union, Tuple

Signature = NewType("Signature", str)


@dataclass
class FileMetadata:
    def __init__(
        self: "FileMetadata",
        path: Path,
        signature: Signature,
        size_bytes: int,
        os_stats: os.stat_result,
    ) -> None:
        self.path = Path(path)
        self.signature = Signature(signature)
        self.size_bytes = int(size_bytes)
        self.os_stats = os.stat_result(os_stats)  # type: ignore

    def to_dict(self: "FileMetadata") -> Dict[str, Union[str, int, Tuple[int, ...]]]:
        return {
            "path": str(self.path),
            "signature": str(self.signature),
            "size_bytes": self.size_bytes,
            "os_stats": tuple(self.os_stats),  # type: ignore
        }


@dataclass
class Block:
    signature: Signature
    url: str
    is_uploaded: bool


@dataclass
class FileData:
    metadata: FileMetadata
    blocks: List[Block]


@dataclass
class Registration:
    registration_id: str
    file_data: FileData
    aws_token: str
