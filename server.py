from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent
from typing import List, Optional
from uuid import uuid4

import common
import schemas


class Server(common.UserAccess):
    """
    CREATE TABLE IF NOT EXISTS Users (
        username VARCHAR(767),
        password_hash TEXT,
        salt TEXT,
        activated BOOL,

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

    @staticmethod
    async def get_block_size_bytes() -> schemas.NumBytes:
        return schemas.NumBytes(4 * 2 ** 20)

    async def declare_block(
        self: "Server", user: schemas.User, block: schemas.BlockMetadata
    ) -> Optional[schemas.UploadInstruction]:
        assert self.validate_user(user)
        assert schemas.validate_Signature(block.signature)
        assert schemas.validate_Bytes(block.size_bytes)
        prefix = str(uuid4()).replace("-", "/")
        url = f"https://s3.copieur.com/{prefix}/{block.signature}"
        async with self.mysql() as (connection, cursor):
            await cursor.execute(
                """
                    INSERT IGNORE INTO Blocks (
                        signature, size_bytes, url
                    ) VALUES (
                        %s, %d, %s
                    );
                """,
                (block.signature, block.size_bytes, url),
            )
            await connection.commit()
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
        batch_insert_params = [
            (block_signature, commit_id, position)
            for position, block_signature in enumerate(commit_data.block_signatures)
        ]
        async with self.mysql() as (connection, cursor):
            await cursor.executemany(
                dedent(
                    """
                        INSERT INTO Commit2Blocks (
                            block_signature, commit_id, position
                        ) VALUES (
                            %s, %s, %d
                        );
                    """
                ),
                batch_insert_params,
            )
            commit_time = datetime.now(timezone.utc)
            await cursor.execute(
                dedent(
                    """
                        INSERT INTO Commits (
                            commit_id,
                            commit_utc_datetime,
                            owner,
                            path,
                            size_bytes,
                        ) VALUES (
                            %s,
                            %s,
                            %s,
                            %s,
                            %d
                        );
                    """
                ),
                (
                    commit_id,
                    commit_time.isoformat(),
                    user.username,
                    str(commit_data.path),
                    commit_data.size_bytes,
                ),
            )
            await connection.commit()
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
        async with self.mysql() as (connection, cursor):
            await cursor.execute(
                dedent(
                    """
                        SELECT
                            Commits.path,
                            Commits.commit_id,
                            Commits.size_bytes,
                            Commits.commit_utc_datetime
                        FROM Commits
                        WHERE owner = %s
                            AND commit_utc_datetime <= %s
                            AND path LIKE %s
                    """
                ),
                (user.username, time.isoformat(), f"{file_prefix}%"),
            )
            return [
                schemas.CommitMetadata(
                    filepath=Path(row.path),
                    size_bytes=schemas.NumBytes(row.size_bytes),
                    owner=user.username,
                    commit_utc_datetime=row.commit_utc_datetime,
                    commit_id=schemas.CommitId(row.commit_id),
                )
                for row in await cursor.fetchall()
            ]

    async def request_restore(
        self: "Server", user: schemas.User, commit_ids: List[schemas.CommitId]
    ) -> schemas.RestoreId:
        assert self.validate_user(user)
        assert all([schemas.validate_Id(commit_id) for commit_id in commit_ids])
        restore_id = schemas.RestoreId(schemas.Identifier(str(uuid4())))
        async with self.mysql() as (connection, cursor):
            await cursor.executemany(
                dedent(
                    """
                        INSERT INTO Restore2Commits (
                            restore_id, commit_id, status,
                        ) VALUES (
                            %s, %s, 'requested'
                        );
                    """
                ),
                [
                    (restore_id, commit_id)
                    for commit_id in commit_ids
                ],
            )
            await cursor.execute(
                dedent(
                    f"""
                        INSERT INTO Restores (
                            restore_id, requester, request_utc_datetime
                        ) VALUES (
                            %s, %s, %s
                        )
                    """
                ),
                (restore_id, user.username, datetime.now(timezone.utc).isoformat()),
            )
            await cursor.commit()
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
