from typing import NewType, List, Optional
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import re


NumBytes = NewType("NumBytes", int)
Username = NewType("Username", str)
Signature = NewType("Signature", str)
Identifier = NewType("Identifier", str)
CommitId = NewType("CommitId", Identifier)
RestoreId = NewType("RestoreId", Identifier)


@dataclass
class User:
    username: Username
    password: str


@dataclass
class BlockMetadata:
    signature: Signature
    size_bytes: NumBytes
    url: Optional[str] = None


@dataclass
class UploadInstruction:
    url: str
    # TODO: Will need AWS tokens


@dataclass
class CommitData:
    path: Path
    size_bytes: NumBytes
    block_signatures: List[Signature]


@dataclass
class CommitMetadata:
    filepath: Path
    size_bytes: NumBytes
    owner: Username
    commit_utc_datetime: datetime
    commit_id: CommitId


_RX_SIGNATURE = re.compile(r"[0-9a-f]{64}")


def validate_Signature(signature: Signature) -> bool:
    return bool(_RX_SIGNATURE.match(signature))


_RC_IDENTIFIER = re.compile(r"^[0-9a-f]{8}(\-[0-9a-f]{4}){3}\-[0-9a-f]{12}$")


def validate_Id(identifier: Identifier) -> bool:
    return bool(_RC_IDENTIFIER.match(identifier))


def validate_Bytes(size: NumBytes) -> bool:
    return isinstance(size, int)
