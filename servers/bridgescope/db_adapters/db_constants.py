from enum import Enum

# Mapping from database type to its corresponding async driver
DB_2_ASYNC_DRIVER = {"postgresql": "asyncpg"}


class S_ENUM(str, Enum):
    """
    Base string enumeration class that inherits from both str and Enum.
    """

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class DB_ENUM(S_ENUM):
    """
    Supported database types.
    """

    PG = "postgresql"


class DB_OBJ_TYPE_ENUM(S_ENUM):
    """
    Supported database object types
    """

    TABLE = "TABLE"
    VIEW = "VIEW"
    COL = "COLUMN"
    PK = "PRIMARY_KEY"
    FK = "FOREIGN_KEY"
    INDEX = "INDEXES"


class DB_PRIV_ENUM(S_ENUM):
    """
    Supported database action types (for access control).
    """

    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
