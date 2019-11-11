import json
import logging
import re
from contextlib import asynccontextmanager
from hashlib import sha256
from textwrap import dedent
from typing import Any, AsyncIterator, Dict, Optional, Tuple

import aiomysql

import schemas


class Loggable:
    __slots__ = ("log",)

    def __init__(self: "Loggable") -> None:
        self.log = logging.getLogger(f"{self.__module__}.{self.__class__.__name__}")


class Config:
    _CACHE: Optional[Dict[str, Any]] = None

    def __init__(self: "Config") -> None:
        if self._CACHE is None:
            with open("config.json") as fd:
                self._CACHE = json.load(fd)

    def __getattribute__(self: "Config", name: str) -> Any:
        if name == "_CACHE":
            return super().__getattribute__(name)
        else:
            assert isinstance(self._CACHE, dict), "Config cache isn't loaded."
            return self._CACHE.get(name)


class MysqlAccess(Loggable):
    @asynccontextmanager
    async def mysql(
        self: "MysqlAccess",
    ) -> AsyncIterator[Tuple[aiomysql.connection.Connection, aiomysql.cursors.Cursor]]:
        if not getattr(self, "_mysql", None):
            self._mysql = await aiomysql.create_pool(**Config().database)
        async with self._mysql.acquire() as conn:
            async with conn.cursor() as cur:
                yield conn, cur


class UserAccess(MysqlAccess):
    @staticmethod
    def salt_and_pepper(salt, password) -> str:
        return sha256(f"{salt}{password}".encode()).hexdigest()

    _RX_USERNAME = re.compile(r"^[a-z0-9\.\-\_\+]+@([a-z0-9\-\_]\.?)+$")

    async def validate_user(self: "UserAccess", user: schemas.User) -> bool:
        assert self._RX_USERNAME.match(user.username)
        sql = dedent(
            """
                SELECT
                    Users.salt,
                    Users.password_hash,
                    Users.activated
                FROM Users
                WHERE username=%s;
            """,
        )
        params = (user.username,)
        async with self.mysql() as (connection, cursor):
            # TODO: Systematically print the resolevd sql statement.
            # print(cursor.mogrify(sql, params))
            await cursor.execute(sql, params)
            salt, password_hash, activated = await cursor.fetchone()
        assert activated, "User must be activated."
        return password_hash == self.salt_and_pepper(salt, user.password)
