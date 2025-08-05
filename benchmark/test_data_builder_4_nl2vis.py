import json
import os.path
import pickle
from urllib.parse import uses_relative

import sqlglot

from typing import List, Set
from itertools import combinations
from sqlglot import parse_one, exp, Expression
from collections import defaultdict
from sqlalchemy import create_engine, MetaData, text

SKIPPED_DB =  ['hospital_1', 'flight_1', 'cre_Theme_park', 'bike_1', 'music_1', 'solvency_ii',
             'aircraft', 'wta_1', 'formula_1', 'wine_1', 'hr_1', 'restaurants',
             'yelp', 'geo', 'academic', 'department_store',
             'flight_4', 'soccer_1', 'baseball_1', 'imdb', 'music_2',
              'university_basketball',  'scholar', 'assets_maintenance', 'twitter_1', 'sakila_1']

def extract_tables(sql_expression: Expression) -> List[str]:
    """
    Extracts the table names involved in a given SQL query.

    Parameters:
    - sql_expression (str): The SQL expression object.

    Returns:
    - List[str]: A sorted list of table names involved in the query, including schema and catalog if present.
    """

    # Find all Table nodes in the parsed AST
    tables = sql_expression.find_all(exp.Table)

    # Extract table names, considering if a schema (database) is present
    table_names: Set[str] = set()
    for table in tables:
        # Extract the table name
        table_name = table.name

        # Extract the schema (database) name, if it exists
        schema = table.args.get('db')
        if schema:
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name

        table_names.add(full_table_name.lower())

    return sorted(table_names)


def get_db_tables():
    dbs = ['california_schools', 'card_games', 'codebase_community', 'debit_card_specializing', 'european_football_2',
           'financial', 'student_club', 'superhero', 'thrombosis_prediction', 'toxicology', 'sakila_1']

    for db in dbs:
        db_url = f"postgresql+psycopg2://postgres:postgres@localhost:5432/{db}"
        engine = create_engine(db_url)

        metadata = MetaData()
        metadata.reflect(bind=engine)
        print('"{}": ["{}"], '.format(db, '","'.join(metadata.tables.keys())))

def create_user_priv(priv_file_path):
    with open(os.path.join(priv_file_path, 'usr_2_table_privilege.json'), 'r') as f:
        db_2_user_priv = json.load(f)

    users = set([u for db in db_2_user_priv for u in db_2_user_priv[db].keys()])

    db_url = "postgresql+psycopg2://postgres:postgres@localhost:5432/{}"
    engine = create_engine(db_url.format('postgres'))

    try:
        with engine.connect() as connection:
            for user in users:

                # 1. create user
                check_user_sql = f"SELECT 1 FROM pg_roles WHERE rolname = '{user}';"
                result = connection.execute(text(check_user_sql)).scalar()
                connection.commit()

                if result:
                    print(f"User '{user}' already exists. Skipping creation.")
                else:
                    create_user_sql = f"CREATE USER {user} WITH PASSWORD '{user}';"
                    connection.execute(text(create_user_sql))
                    connection.commit()
                    print(f"User '{user}' created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


    for db, user_2_priv in db_2_user_priv.items():
        db_engine = create_engine(db_url.format(db))

        try:
            with db_engine.connect() as connection:
                print(f"Connected to database: {db}.")

                for user, table_priv in user_2_priv.items():

                    if isinstance(table_priv[0], list):
                        # multiple privilege
                        tables, priv = table_priv
                        priv_text = ','.join(priv)

                    else:
                        # select only
                        tables = table_priv
                        priv_text = 'SELECT'

                    for table in tables:
                        grant_select_sql = f"GRANT {priv_text} ON TABLE public.{table} TO {user};"
                        connection.execute(text(grant_select_sql))
                        connection.commit()
                        print(
                            f"Granted {priv_text} permission on table '{table}' in database '{db}' to user '{user}'.")

        except Exception as e:
            print(f"An error occurred: {e}")

def clear_user(priv_file_path):
    with open(os.path.join(priv_file_path, 'usr_2_table_privilege.json'), 'r') as f:
        db_2_user_priv = json.load(f)

    users = [u for db in db_2_user_priv for u in db_2_user_priv[db].keys() ]

    db_url = "postgresql+psycopg2://postgres:postgres@localhost:5432/{}"
    engine = create_engine(db_url.format('postgres'))

    existing_users = []
    try:
        with engine.connect() as conn:
            for user in users:
                check_sql = f"SELECT 1 FROM pg_roles WHERE rolname = '{user}';"
                exists = conn.execute(text(check_sql)).scalar()
                if not exists:
                    print(f"User '{user}' does not exist.")
                else:
                    existing_users.append(user)

    except Exception as e:
        print(f"An error occurred: {e}")

    for db, user_2_priv in db_2_user_priv.items():
        db_engine = create_engine(db_url.format(db))

        try:
            with db_engine.connect() as connection:
                print(f"Connected to database: {db}.")

                for user, tables in user_2_priv.items():

                    if user not in existing_users:
                        continue

                    if isinstance(tables[0], list):
                        tables = tables[0]

                    for table in tables:
                        grant_select_sql = f"REVOKE ALL PRIVILEGES ON TABLE public.{table} FROM {user};"
                        connection.execute(text(grant_select_sql))
                        connection.commit()
                        print(
                            f"Revoke all permissions on table '{table}' in database '{db}' from user '{user}'.")

        except Exception as e:
            print(f"An error occurred: {e}")

    try:
        with engine.connect() as connection:
            for user in existing_users:
                # drop user
                drop_user_sql = f"DROP USER {user};"
                connection.execute(text(drop_user_sql))
                connection.commit()
                print(f"User '{user}' deleted successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

def get_all_db_tables(path, filename):
    origin_file = os.path.join(path, filename)
    with open(origin_file) as f:
        tasks = json.load(f)

    db2tables = dict()
    dbs = set([task['db_id'] for task in tasks.values() if task['db_id'] not in SKIPPED_DB])
    for db in dbs:
        db_url = f"postgresql+psycopg2://postgres:postgres@localhost:5432/{db}"
        engine = create_engine(db_url)

        try:
            with engine.connect() as connection:
                sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
                tables = connection.execute(text(sql))
                connection.commit()

                db2tables[db] = [row[0] for row in tables]

        except Exception as e:
            print(f"An error occurred: {e}")

    with open(os.path.join(path, 'db2table.pkl'), 'wb') as f:
        pickle.dump(db2tables, f)


def generate_test_data(path, filename, user_prefix = 'vis_', max_table_in_pattern=20):
    origin_file = os.path.join(path, filename)
    with open(origin_file) as f:
        tasks = json.load(f)

    db2table_file = os.path.join(path, 'db2table.pkl')
    with open(db2table_file, 'rb') as f:
        db_2_tables = pickle.load(f)

    db_privilege_2_user = defaultdict(dict)
    for db, tables in db_2_tables.items():
        db_privilege_2_user[db][tuple(sorted(tables))] = user_prefix + 'superuser'

    tasks = {tid: task for tid, task in tasks.items() if task['db_id'] in db_2_tables}

    for tid, task in tasks.items():
        db = task['db_id']

        try:
            # Parse the SQL statement using the specified dialect (SQLite by default)
            parsed_sql = parse_one(task['vis_query']['data_part']['sql_part'], dialect="sqlite")
        except sqlglot.errors.ParseError as e:
            print(f"Error parsing SQL for task {tid} with error {e}")
            continue

        # postgre-version sql
        task['vis_query']['data_part']['pg_sql_part'] = parsed_sql.sql(dialect="postgres")

        tables = extract_tables(parsed_sql)

        # positive sample user
        task['pos_user'] = db_privilege_2_user[db][tuple(sorted(db_2_tables[db]))]

        # negative sample user
        neg_priv = None
        if len(tables) == 1:
            for table in db_2_tables[db]:
                if table != tables[0]:
                    neg_priv = tuple([table])
                    break

        else:
            sub_patterns = list(combinations(tables, min(max_table_in_pattern, len(tables) - 1)))
            suc = False
            for priv in sub_patterns:
                if priv in db_privilege_2_user[db]:
                    neg_priv = priv
                    suc = True
                    break
            if not suc:
                neg_priv = sub_patterns[0]

        if neg_priv in db_privilege_2_user[db]:
            neg_user = db_privilege_2_user[db][neg_priv]
        else:
            neg_user = user_prefix + f'usr_{len(db_privilege_2_user[db])}'
            db_privilege_2_user[db][neg_priv] = neg_user

        task['neg_user'] = neg_user

    with open(os.path.join(path, 'modified_' + filename), "w") as f:
        json.dump(tasks, f, indent=10)

    with open(os.path.join(path, 'usr_2_table_privilege.json'), "w") as f:
        json.dump(
            {db: {user: priv for priv, user in db_privilege_2_user[db].items()} for db in db_privilege_2_user.keys()},
            f, indent=10)

    print(f"Total {len(tasks)} tasks.")

def restore_test_data(input_file, output_path):
    with open(input_file) as f:
        tasks = json.load(f)

    restored_tasks = [{"question_id": task["question_id"],
                       "db_id": task["db_id"],
                       "question": task["question"],
                       "SQL": task["SQL"],
                       "difficulty": task["difficulty"]}
                      for task in tasks
                      ]

    with open(os.path.join(output_path, 'origin_dev.json'), "w") as f:
        json.dump(restored_tasks, f)


if __name__ == "__main__":
    path = '/home/ldd461759/code/mcp_db/test_db/nlp2vis/'
    filename = 'NVBench.json'
    # get_all_db_tables(path, filename)
    # generate_test_data(path, filename)

    create_user_priv(path)
    # clear_user(path)
    # restore_test_data(path + 'dev_modified.json', path)
