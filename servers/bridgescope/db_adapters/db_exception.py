class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    pass

class DatabaseConfigError(Exception):
    """Custom exception for database configuration errors."""
    pass

class TransactionError(Exception):
    """Custom exception for transaction-related errors."""
    pass

class DatabaseConnectionError(Exception):
    """Custom exception for database connection errors."""
    def __init__(self):
        self.message = "Database is not connected yet."
        super().__init__(self.message)
