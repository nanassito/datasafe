"""Schemas shared across multiple components."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, NewType

Signature = NewType("Signature", str)


@dataclass
class FileMetadata:
    path: Path
    signature: Signature
    size_bytes: int
    os_stats: os.stat_result


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
