from abc import ABC, abstractmethod
from typing import Any, Dict, List

from db_adapters.db_config import DBConfig


class BaseAdapter(ABC):
    """
    Base adapter class for all database adapters.
    """

    def __init__(self, config: DBConfig):
        """
        Initialize a new instance of BaseAdapter.

        :param config: DBConfig instance for the database connection.
        """
        self.config = config

    def __del__(self):
        """
        Cleanup the database adapter.
        """
        pass

    @abstractmethod
    async def connect(self):
        """
        Establishes a connection to the database.
        """
        pass

    @abstractmethod
    async def close(self):
        """
        Closes the active database connection.
        """
        pass

    @abstractmethod
    async def execute_query(self, sql: str) -> Any:
        """
        Executes an SQL query and returns the results.

        :param sql: The SQL query string to execute.
        :return: A list of rows returned by the query or #rows affected by the DML.
        """
        pass

    @abstractmethod
    async def get_user_privileges(self) -> Dict[Any, Dict[Any, List]]:
        """
        Retrieves the privileges of the currently connected user.

        The structure of privileges is as follows:
            {
                'SELECT': {
                            'TABLE':  ['public.table1', 'public.table2'],
                            'COLUMN': ['public.table3.col1', 'public.table3.col2'],
                            ...
                           },
                ...
            }

        :return: Nested dictionaries of user privileges.
        """
        pass

    @abstractmethod
    async def get_top_level_objects(self) -> Dict[str, Any]:
        """
        Retrieve a dict of top-level objects in the database.

        :return: Dict of object names.
        """
        pass

    @abstractmethod
    async def get_table_details(self, table: str) -> Dict[str, Any]:
        """
        Retrieve details for a given table.

        :param table: Table name for which to retrieve details.
        :return: Dictionary containing table metadata and column information.
        """
        pass

    @abstractmethod
    async def get_database_schema(self) -> Dict[str, Dict]:
        """
        Retrieve the database schema.

        :return: A dictionary mapping top-level database objects to their schema details.
        """
        pass

    @abstractmethod
    async def begin(self) -> None:
        """
        Begin a new transaction.
        """
        pass

    @abstractmethod
    async def commit(self) -> None:
        """
        Commit the current transaction.
        """
        pass

    @abstractmethod
    async def rollback(self) -> None:
        """
        Roll back the current transaction.
        """
        pass
