import os

from adbc_driver_gizmosql import dbapi as gizmosql

# Apache Arrow ADBC documentation
# https://arrow.apache.org/adbc/current/python/quickstart.html


class Connection:
    """Thin wrapper around a gizmosql ADBC connection.

    Adds execute_ddl() which routes DDL through CommandStatementUpdate
    (DoPut), bypassing the CommandStatementQuery path that GizmoSQL
    does not persist for DDL.

    Issue:
    https://github.com/gizmodata/gizmosql/issues/134
    """

    def __init__(self, inner: gizmosql.Connection) -> None:
        self._inner = inner

    def cursor(self):
        return self._inner.cursor()

    def close(self) -> None:
        self._inner.close()

class DbManager:
    def __init__(self, url: str, username: str, password: str):
        self._url = url
        self._username = username
        self._password = password
        self._conn: Connection | None = None

    def __enter__(self) -> Connection:
        inner = gizmosql.connect(
            uri=self._url,
            db_kwargs={"username": self._username, "password": self._password},
            autocommit=True,
        )
        self._conn = Connection(inner)
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
