import asyncio
from textwrap import dedent
from uuid import uuid4

import common
import schemas


class Admin(common.UserAccess):
    async def create_user(self: "Admin", user: schemas.User) -> None:
        assert Admin._RX_USERNAME.match(user.username)
        salt = uuid4()
        password_hash = self.salt_and_pepper(salt, user.password)
        # TODO: Users to be disabled by default.
        async with self.mysql() as (connection, cursor):
            await cursor.execute(
                dedent(
                    f"""
                        INSERT INTO Users (
                            username, password_hash, salt, activated
                        ) VALUES (
                            %s, %s, %s, true
                        );
                    """
                ),
                (user.username, password_hash, str(salt)),
            )
            await connection.commit()


if __name__ == "__main__":
    admin = Admin()
    user = schemas.User(
        username=schemas.Username(f"test_{uuid4()}@jaminais.fr"),
        password="This is a bad password",
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(admin.create_user(user))
    loop.run_until_complete(admin.validate_user(user))
