from abc import ABC
from contextlib import asynccontextmanager
from typing import Any, Optional, Dict

from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from db_adapters.base_adapter import BaseAdapter
from db_adapters.db_config import DBConfig
from db_adapters.db_constants import DB_OBJ_TYPE_ENUM
from db_adapters.db_exception import (
    DatabaseError,
    DatabaseConnectionError,
    TransactionError,
)


class SqlAlchemyAdapter(BaseAdapter, ABC):
    """
    Base class for database adapters using SQLAlchemy with async support.
    """

    def __init__(
        self,
        config: DBConfig,
        args: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize SQLAlchemy adapter.

        :param config: Database configuration instance
        :param args: Additional engine arguments
        """
        super().__init__(config)
        self.engine = None
        self.session_factory = None
        self.engine_args = args or {}

        self.in_nested = False
        self._session: Optional[AsyncSession] = None

    async def connect(self) -> None:
        """
        Establish an asynchronous connection to the database using SQLAlchemy.

        Raises:
            DatabaseError: If database connection fails
        """
        try:
            # Create async engine
            self.engine = create_async_engine(
                self.config.get_dsn(), **(self.engine_args or {})
            )

            # Create session factory
            self.session_factory = sessionmaker(
                bind=self.engine, class_=AsyncSession, expire_on_commit=False
            )

            # Test the connection
            async with self.engine.begin():
                pass

        except Exception as e:
            await self.close()
            raise DatabaseError(f"Database connection failed: {e}") from e

    async def close(self) -> None:
        """
        Closes all database connections and disposes of the engine.
        """
        await self.release()
        if self.engine:
            try:
                await self.engine.dispose()
            except Exception as e:
                raise DatabaseError(f"Error closing database connection: {e}") from e

    async def release(self) -> None:
        """
        Release the current session if it exists.
        """
        if self._session:
            try:
                if self.config.readonly:
                    # For readonly session, roll back automatically
                    await self._session.rollback()
                else:
                    # Otherwise, commit current session
                    await self._session.commit()
            finally:
                await self._session.close()
                self._session = None

    async def begin(self) -> None:
        """
        Start a new transaction.
        """
        if not self.session_factory:
            raise DatabaseConnectionError()

        await self.release()

        self.in_nested = True
        self._session = self.session_factory()
        await self._session.begin()

    async def commit(self) -> None:
        """
        Commit the current transaction.
        """
        if not self._session:
            raise TransactionError("No active transaction to commit.")

        try:
            await self._session.commit()
        finally:
            await self._session.close()
            self._session = None
            self.in_nested = False

    async def rollback(self) -> None:
        """
        Rollback the current transaction.
        """
        if not self._session:
            raise TransactionError("No active transaction to rollback.")

        try:
            await self._session.rollback()
        finally:
            await self._session.close()
            self._session = None
            self.in_nested = False

    @asynccontextmanager
    async def session_context(self):
        """
        Async context manager for session.
        If a session exists, reuse it; otherwise, create a new one and close it on exit.
        Nested transactions are supported if n_isolation_level == 2.
        """
        session = self._session
        if not session:
            session = self.session_factory()
            await session.begin()

        try:
            if self.in_nested:
                async with session.begin_nested():
                    yield session
            else:
                yield session

            if not self.in_nested:
                if self.config.readonly:
                    await session.rollback()
                else:
                    await session.commit()

        except SQLAlchemyError as e:
            if not self.in_nested:
                await session.rollback()
            raise e

        finally:
            if not self.in_nested:
                await session.close()
                self._session = None

    async def execute_query(self, sql: str) -> Any:
        """
        Executes a raw SQL query.

        :param sql: SQL query string.
        :return: Query results or #rows affected.
        """
        if not self.session_factory:
            raise DatabaseConnectionError()

        async with self.session_context() as session:
            result = await session.execute(text(sql))

            if getattr(result, "returns_rows", False):
                return result.fetchall()
            else:
                return result.rowcount

    async def get_database_schema(self) -> Dict[str, Dict]:
        """
        Retrieve the database schema.

        :return: A dictionary mapping table names to their schema details.
        """
        if not self.engine:
            raise DatabaseConnectionError()

        def _get(sync_conn):
            inspector = inspect(sync_conn)

            # Table only for current version
            database_schema = {DB_OBJ_TYPE_ENUM.TABLE: {}}

            # Unify tables and views
            table_names = list(inspector.get_table_names()) + list(inspector.get_view_names())
            for table_name in table_names:
                database_schema[DB_OBJ_TYPE_ENUM.TABLE][table_name] = self.get_table_info(
                    inspector, table_name
                )

            return database_schema

        async with self.engine.connect() as conn:
            return await conn.run_sync(_get)

    async def get_top_level_objects(self) -> Dict[str, Any]:
        """
        Retrieve a dict of top-level objects in the database.

        :return: Dict of object names.
        """
        if not self.session_factory:
            raise DatabaseConnectionError()

        def _get(sync_conn):
            inspector = inspect(sync_conn)

            return {
                DB_OBJ_TYPE_ENUM.TABLE: inspector.get_table_names(),
                DB_OBJ_TYPE_ENUM.VIEW: inspector.get_view_names()
            }

        async with self.engine.connect() as conn:
            return await conn.run_sync(_get)

    async def get_table_details(self, table: str) -> Dict[str, Any]:
        """
        Retrieve a list of columns for a given table.

        :param table: Table name for which to retrieve columns.
        :return: List of dictionaries, each representing column metadata.
        :raises DatabaseConnectionError: If database is not connected.
        """

        if not self.session_factory:
            raise DatabaseConnectionError()

        def _get(sync_conn):
            inspector = inspect(sync_conn)
            return self.get_table_info(inspector, table_name=table)

        async with self.engine.connect() as conn:
            return await conn.run_sync(_get)

    def get_table_info(self, inspector, table_name):
        """
        Get detailed information about a table including columns, keys, and indexes.

        :param inspector: SQLAlchemy inspector instance
        :param table_name: Name of the table to inspect
        :return: Dictionary containing table information with columns, primary_keys, foreign_keys, and indexes
        """
        table_info = {
            DB_OBJ_TYPE_ENUM.COL: [],
            DB_OBJ_TYPE_ENUM.PK: [],
            DB_OBJ_TYPE_ENUM.FK: [],
            DB_OBJ_TYPE_ENUM.INDEX: [],
        }

        # Columns
        columns = inspector.get_columns(table_name)
        for col in columns:
            table_info[DB_OBJ_TYPE_ENUM.COL].append(
                {
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col["nullable"],
                }
            )

        # Primary Keys
        pk_constraint = inspector.get_pk_constraint(table_name)
        if pk_constraint and "constrained_columns" in pk_constraint:
            table_info[DB_OBJ_TYPE_ENUM.PK] = pk_constraint["constrained_columns"]

        # Foreign Keys
        fk_constraints = inspector.get_foreign_keys(table_name)
        for fk in fk_constraints:
            referred_table = fk["referred_table"]
            local_col = fk["constrained_columns"][0]
            remote_col = fk["referred_columns"][0]
            table_info[DB_OBJ_TYPE_ENUM.FK].append(
                {
                    "local_column": local_col,
                    "remote_table": referred_table,
                    "remote_column": remote_col,
                }
            )

        # Indexes
        indexes = inspector.get_indexes(table_name)
        for idx in indexes:
            table_info[DB_OBJ_TYPE_ENUM.INDEX].append(
                {
                    "name": idx["name"],
                    "columns": idx["column_names"],
                    "unique": idx["unique"],
                }
            )

        return table_info
