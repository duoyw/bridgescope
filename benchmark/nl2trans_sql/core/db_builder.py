import subprocess

import asyncpg

from benchmark.nl2trans_sql.core.utils import sync_exec
from db_adapters.db_config import DBConfig
from db_adapters.pg_adapter import PostgresAdapter


class DatabaseBuilder:
    def __init__(self):
        self.dbs = [
        "california_schools", "card_games", "codebase_community", "debit_card_specializing", "european_football_2",
        "financial", "student_club", "superhero", "thrombosis_prediction", "toxicology"]

        self.db_path = 'postgresql://postgres:postgres@localhost:5432/{}'

        self.postgres_conn = PostgresAdapter(DBConfig(self.db_path.format('postgres')))
        sync_exec(self.postgres_conn.connect)

    async def drop_database(self):
        connection = await asyncpg.connect(
            self.db_path.format('postgres')
        )
        for db_name in self.dbs:
            try:
                await connection.execute(f'DROP DATABASE {db_name}')
                print(f'Database {db_name} dropped successfully.')
            except Exception as e:
                print(f"Error dropping database {db_name}: {e}")
        await connection.close()

    async def create_database(self):
        connection = await asyncpg.connect(
            self.db_path.format('postgres')
        )
        for db_name in self.dbs:
            try:
                await connection.execute(f'CREATE DATABASE {db_name}')
                print(f'Database {db_name} created successfully.')
            except Exception as e:
                print(f"Error creating database {db_name}: {e}")
        await connection.close()

    async def drop_user(self):
        connection = await asyncpg.connect(
            self.db_path.format('postgres')
        )

        try:
            result = await connection.fetch("SELECT usename FROM pg_catalog.pg_user")
            users = [record['usename'] for record in result]

            user_to_drop = [u for u in users if not u.startswith('vis_') and u!='postgres']

            for u in user_to_drop:
                await connection.execute(f'DROP USER {u}')
                print(f'User {u} dropped successfully.')
        except Exception as e:
            print(f"Error dropping user: {e}")
        finally:
            await connection.close()

    def restore_databases(self, db_dumps, user='postgres', host='localhost'):
        for db_name, db_dump in db_dumps.items():
            try:
                command = [
                    "pg_restore", "--dbname", db_name, f"--username={user}", "--host", host, db_dump
                ]
                subprocess.run(command, check=True)
                print(f"Database {db_name} restored from {db_dump}.")
            except subprocess.CalledProcessError as e:
                print(f"Error restoring {db_name}: {e}")


if __name__ == "__main__":
    dbb = DatabaseBuilder()
    import asyncio

    # asyncio.run(dbb.drop_database())
    asyncio.run(dbb.create_database())


