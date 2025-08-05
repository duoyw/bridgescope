import re
from typing import Optional, Union

from db_adapters.db_constants import DB_2_ASYNC_DRIVER, DB_ENUM
from db_adapters.db_exception import DatabaseConfigError


class DBConfig:
    """
    Database configuration class for managing connection parameters.

    Attributes:
        db_type (DB_ENUM): Database type
        db_host (str): Database server hostname
        db_port (str): Database server port
        db_user (str): Database username
        db_user_pwd (str): Database password
        db_name (str): Database name
    """

    def __init__(self, db_dsn: Optional[str] = None, readonly = True) -> None:
        """
        Initialize database configuration.

        :param db_dsn: Database connection string (postgresql://user:password@host:port/db_name)
        :param readonly: Run SQL in readonly mode (all data modifications will be rolled back automatically) unless explict transactional commit
        """
        self.db_type: DB_ENUM = DB_ENUM.PG
        self.db_host: str = ""
        self.db_port: str = ""
        self.db_user: str = ""
        self.db_user_pwd: str = ""
        self.db_name: str = ""

        self.readonly = readonly

        if db_dsn is not None:
            self.build_from_dsn(db_dsn)

    def __str__(self) -> str:
        """Return string representation of configuration."""
        return self.__dict__.__str__()

    def build_from_dsn(self, dsn: str) -> None:
        """
        Parse DSN string and build configuration.

        :param dsn: Database connection string
        :raises DatabaseConfigError: If DSN format is invalid
        """
        if not isinstance(dsn, str) or not dsn.strip():
            raise DatabaseConfigError("DSN must be a non-empty string")

        pattern = r"^(?P<db_type>postgresql)://(?P<db_user>[^:]+):(?P<db_user_pwd>[^@]+)@(?P<db_host>[^:]+):(?P<db_port>\d+)/(?P<db_name>[^?]+)$"
        match = re.match(pattern, dsn.strip())

        if not match:
            raise DatabaseConfigError(f"Invalid DSN format: {dsn}")

        self.build(**match.groupdict())

    def build(
        self,
        db_type: Optional[Union[str, DB_ENUM]] = None,
        db_host: Optional[str] = None,
        db_port: Optional[Union[str, int]] = None,
        db_user: Optional[str] = None,
        db_user_pwd: Optional[str] = None,
        db_name: Optional[str] = None,
    ) -> None:
        """
        Build configuration with provided parameters.

        :param db_type: Database type
        :param db_host: Database hostname
        :param db_port: Database port
        :param db_user: Database username
        :param db_user_pwd: Database password
        :param db_name: Database name
        :raises DatabaseConfigError: If parameters are invalid or incomplete
        """
        if db_type is not None:
            if isinstance(db_type, str):
                try:
                    self.db_type = DB_ENUM(db_type)
                except ValueError:
                    raise DatabaseConfigError(f"Unsupported database type: {db_type}")
            elif isinstance(db_type, DB_ENUM):
                self.db_type = db_type
            else:
                raise DatabaseConfigError(f"Invalid db_type: {type(db_type)}")

        if db_host is not None:
            self.db_host = str(db_host).strip()

        if db_port is not None:
            self.db_port = str(db_port).strip()

        if db_user is not None:
            self.db_user = str(db_user).strip()

        if db_user_pwd is not None:
            self.db_user_pwd = str(db_user_pwd)

        if db_name is not None:
            self.db_name = str(db_name).strip()

        # Validate configuration after building
        self._validate_config()

    def _validate_config(self) -> None:
        """
        Validate the current configuration.

        :raises DatabaseConfigError: If configuration is invalid or incomplete
        """
        # Check required fields
        required_fields = {
            "db_type": self.db_type,
            "db_host": self.db_host,
            "db_port": self.db_port,
            "db_user": self.db_user,
            "db_user_pwd": self.db_user_pwd,
            "db_name": self.db_name,
        }

        missing_fields = [
            field for field, value in required_fields.items() if not value
        ]
        if missing_fields:
            raise DatabaseConfigError(f"Missing required fields: {missing_fields}")

        # Check async driver availability
        db_type_str = self.db_type.value
        if db_type_str not in DB_2_ASYNC_DRIVER:
            raise DatabaseConfigError(
                f"No async driver for database type: {db_type_str}"
            )

    def get_dsn(self) -> str:
        """
        Generate DSN string for async database connection.

        :return: Complete DSN string with async driver
        """
        db_type_str = self.db_type.value
        async_driver = DB_2_ASYNC_DRIVER[db_type_str]
        return f"{db_type_str}+{async_driver}://{self.db_user}:{self.db_user_pwd}@{self.db_host}:{self.db_port}/{self.db_name}"
