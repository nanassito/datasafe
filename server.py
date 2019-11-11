import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from textwrap import dedent
from typing import (
    AsyncContextManager,
    Iterator,
    List,
    NewType,
    Optional,
    Pattern,
    Tuple,
    Type,
)
from uuid import uuid4

import aiomysql

import schemas


class Server:
    """
    CREATE TABLE IF NOT EXISTS Users (
        username VARCHAR(767),
        password_hash TEXT,
        salt TEXT,
        verified BOOL,

        PRIMARY KEY (username)
    );

    CREATE TABLE IF NOT EXISTS Blocks (
        signature VARCHAR(65),
        size_bytes INT,
        url TEXT,

        PRIMARY KEY (signature)
    );

    CREATE TABLE IF NOT EXISTS Commits (
        commit_id VARCHAR(36),
        commit_utc_datetime DATETIME,
        owner TEXT,
        path TEXT,
        size_bytes INT,

        PRIMARY KEY (commit_id)
    );

    CREATE TABLE IF NOT EXISTS Commit2Blocks (
        commit_id VARCHAR(36),
        block_signature VARCHAR(65),
        position INT,

        INDEX idx_commit (commit_id),
        INDEX idx_block (block_signature)
    );

    CREATE TABLE IF NOT EXISTS Restores (
        restore_id VARCHAR(36),
        requester VARCHAR(767),
        request_utc_datetime DATETIME,

        PRIMARY KEY (restore_id)
    );

    CREATE TABLE IF NOT EXISTS Restore2Commits (
        restore_id VARCHAR(36),
        commit_id VARCHAR(36),
        status TEXT,

        INDEX idx_restore_id (restore_id),
        INDEX idx_commit_id (commit_id)
    );
    """

    def __init__(self: "Server") -> None:
        self.log = logging.getLogger(f"{self.__module__}.{self.__class__.__name__}")

    @staticmethod
    def salt_and_pepper(salt, password) -> str:
        return sha256(f"{salt}{password}".encode()).hexdigest()

    @staticmethod
    async def get_block_size_bytes() -> schemas.NumBytes:
        return schemas.NumBytes(4 * 2 ** 20)

    _RX_USERNAME = re.compile(r"^[a-z0-9\.\-\_\+]+@([a-z0-9\-\_]\.?)+$")

    @asynccontextmanager
    async def mysql(
        self: "Server",
    ) -> AsyncContextManager[
        Tuple[aiomysql.connection.Connection, aiomysql.cursors.Cursor]
    ]:
        if not getattr(self, "_mysql", None):
            loop = asyncio.get_event_loop()
            self._mysql = await aiomysql.create_pool(
                host="sandbox.c3fdoqlimzng.us-west-1.rds.amazonaws.com",
                port=3306,
                user="admin",
                password="tW4R3WOMZ3OY9Cdjsx7N",
                db="sandbox",
            )
        async with self._mysql.acquire() as conn:
            async with conn.cursor() as cur:
                yield conn, cur

    async def create_user(self: "Server", user: schemas.User) -> None:
        assert Server._RX_USERNAME.match(user.username)
        salt = uuid4()
        password_hash = self.salt_and_pepper(salt, user.password)
        async with self.mysql() as (connection, cursor):
            await cursor.execute(
                dedent(
                    f"""
                        INSERT INTO Users (
                            username,
                            password_hash,
                            salt,
                            verified
                        ) VALUES (
                            %s,
                            %s,
                            %s,
                            false
                        );
                    """
                ),
                (user.username, password_hash, str(salt),),
            )
            await connection.commit()

    async def validate_user(self: "Server", user: schemas.User) -> bool:
        assert Server._RX_USERNAME.match(user.username)
        sql = dedent(
            f"""
                SELECT
                    Users.salt,
                    Users.password_hash,
                    Users.activated
                FROM Users
                WHERE username={user.username};
            """
        )
        assert activated, "User must be activated."
        return password_hash == self.salt_and_pepper(salt, user.password)

    async def declare_block(
        self: "Server", user: schemas.User, block: schemas.BlockMetadata
    ) -> Optional[schemas.UploadInstruction]:
        assert self.validate_user(user)
        assert schemas.validate_Signature(block.signature)
        assert schemas.validate_Bytes(block.size_bytes)
        url = (
            f"https://s3.copieur.com/{str(uuid4()).replace('-', '/')}/{block.signature}"
        )
        sql = dedent(
            f"""
                INSERT IGNORE INTO Blocks (
                    signature, 
                    size_bytes, 
                    url
                ) VALUES (
                    {block.signature}, 
                    {block.size_bytes}, 
                    {url}
                );

                SELECT 
                    Blocks.signature, 
                    Blocks.size_bytes, 
                    Blocks.url
                FROM Blocks
                WHERE signature={block.signature};
            """
        )
        return schemas.UploadInstruction(url=url)

    async def commit(
        self: "Server", user: schemas.User, commit_data: schemas.CommitData
    ) -> schemas.CommitMetadata:
        assert self.validate_user(user)
        assert schemas.validate_Bytes(commit_data.size_bytes)
        assert all(
            [schemas.validate_Signature(bs) for bs in commit_data.block_signatures]
        )
        commit_id = schemas.CommitId(schemas.Identifier(str(uuid4())))
        # TODO: escape path
        for position, block_signature in enumerate(commit_data.block_signatures):
            sql = dedent(
                f"""
                    INSERT INTO Commit2Blocks (
                        block_signature,
                        commit_id,
                        position
                    ) VALUES (
                        {block_signature},
                        {commit_id},
                        {position}
                    );
                """
            )
        commit_time = datetime.now(timezone.utc)
        sql = dedent(
            f"""
                INSERT INTO Commits (
                    commit_id,
                    commit_utc_datetime,
                    owner,
                    path,
                    size_bytes,
                ) VALUES (
                    {commit_id},
                    {commit_time},
                    {user.username},
                    {commit_data.path},
                    {commit_data.size_bytes}
                );
            """
        )
        return schemas.CommitMetadata(
            filepath=commit_data.path,
            size_bytes=commit_data.size_bytes,
            owner=user.username,
            commit_utc_datetime=commit_time,
            commit_id=commit_id,
        )

    async def list_commits_until(
        self: "Server", user: schemas.User, time: datetime, file_prefix: str
    ) -> List[schemas.CommitMetadata]:
        assert self.validate_user(user)
        sql = dedent(
            f"""
                SELECT
                    Commits.path,
                    Commits.commit_id,
                    Commits.size_bytes,
                    Commits.commit_utc_datetime
                FROM Commits
                WHERE owner = {user.username}
                    AND commit_utc_datetime <= {time}
                    AND path LIKE '{file_prefix}%'
            """
        )
        return [
            schemas.CommitMetadata(
                filepath=Path(row.path),
                size_bytes=schemas.NumBytes(row.size_bytes),
                owner=user.username,
                commit_utc_datetime=row.commit_utc_datetime,
                commit_id=schemas.CommitId(row.commit_id),
            )
            for row in cursor
        ]

    async def request_restore(
        self: "Server", user: schemas.User, commit_ids: List[schemas.CommitId]
    ) -> schemas.RestoreId:
        assert self.validate_user(user)
        assert all([schemas.validate_Id(commit_id) for commit_id in commit_ids])
        restore_id = schemas.RestoreId(schemas.Identifier(str(uuid4())))
        for commit_id in commit_ids:
            sql = dedent(
                f"""
                    INSERT INTO Restore2Commits (
                        restore_id,
                        commit_id,
                        status,
                    ) VALUES (
                        {restore_id},
                        {commit_id},
                        'requested'
                    );
                """
            )
        sql = dedent(
            f"""
                INSERT INTO Restores (
                    restore_id,
                    requester,
                    request_utc_datetime
                ) VALUES (
                    {restore_id}
                )
            """
        )
        return restore_id

    # Background processes.

    async def process_restores(self: "Server") -> None:
        pass

    async def expire_commits(self: "Server") -> None:
        pass

    async def s3_lifecycle(self: "Server") -> None:
        # s3 = boto3.client(
        #     "s3",
        #     aws_access_key_id="AKIATRGC2RRBYCVOAFHZ",
        #     aws_secret_access_key="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",
        # )
        # s3.list_objects_v2(Bucket='ba18ab6c-153a-41b7-87ed-f5f82c1f26a7')
        #
        # This lists all the files across all storage classes. It is paginated.
        #
        # Also https://docs.aws.amazon.com/AmazonS3/latest/user-guide/configure-inventory.html
        # and https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html#storage-inventory-location
        pass


# Might want to enable versioning on the bucket and enable a legalhold worm lock
# after upload. However this means that we will have to target specific versions
# when performing deletions. Also unclear if versions are part of the list/inventory or not.