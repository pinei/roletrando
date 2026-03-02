import os

from adbc_driver_flightsql import dbapi as gizmosql

Connection = gizmosql.Connection

# Apache Arrow ADBC documentation
# https://arrow.apache.org/adbc/current/python/quickstart.html

class DbManager:
    def __init__(self, url: str, username: str, password: str):
        self._url = url
        self._username = username
        self._password = password
        self._conn: Connection | None = None

    def __enter__(self) -> Connection:
        self._conn = gizmosql.connect(
            uri=self._url,
            db_kwargs={"username": self._username, "password": self._password},
            autocommit=True,
        )
        return self._conn

    def __exit__(self, *args) -> None:
        if self._conn:
            self._conn.close()

    @classmethod
    def from_env(cls) -> "DbManager":
        return cls(
            url=os.environ["GIZMOSQL_URL"],
            username=os.environ["GIZMOSQL_USERNAME"],
            password=os.environ["GIZMOSQL_PASSWORD"],
        )
