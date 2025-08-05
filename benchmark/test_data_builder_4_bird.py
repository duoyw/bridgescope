import json
import os.path

import sqlglot

from typing import List, Set
from itertools import combinations
from sqlglot import parse_one, exp, Expression
from collections import defaultdict
from sqlalchemy import create_engine, MetaData, text

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
           'financial', 'student_club', 'superhero', 'thrombosis_prediction', 'toxicology']

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
        db_engine = create_engine(db_url.format(db + '_3'))

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


def generate_test_data_v1(input_file, output_path, max_table_in_pattern=20):
    db_2_tables = {
        "california_schools": ["satscores", "schools", "frpm"],
        "card_games": ["foreign_data", "legalities", "cards", "rulings", "sets", "set_translations"],
        "codebase_community": ["postlinks", "tags", "users", "badges", "comments", "posthistory", "posts", "votes"],
        "debit_card_specializing": ["customers", "gasstations", "products", "transactions_1k", "yearmonth"],
        "european_football_2": ["country", "league", "match", "player", "player_attributes", "team", "team_attributes"],
        "financial": ["district", "account", "disp", "client", "card", "loan", "order", "trans"],
        "student_club": ["event", "attendance", "member", "major", "zip_code", "budget", "expense", "income"],
        "superhero": ["attribute", "hero_attribute", "superhero", "alignment", "colour", "gender", "publisher", "race",
                      "hero_power", "superpower"],
        "thrombosis_prediction": ["examination", "patient", "laboratory"],
        "toxicology": ["molecule", "atom", "connected", "bond"],
    }

    db_privilege_2_user = defaultdict(dict)
    for db, tables in db_2_tables.items():
        db_privilege_2_user[db][tuple(sorted(tables))] = 'superuser'

    with open(input_file) as f:
        tasks = json.load(f)

    tasks = [task for task in tasks if task['db_id'] != 'formula_1']

    for task in tasks:
        db = task['db_id']

        try:
            # Parse the SQL statement using the specified dialect (SQLite by default)
            parsed_sql = parse_one(task['SQL'], dialect="sqlite")
        except sqlglot.errors.ParseError as e:
            print(f"Error parsing SQL: {e}")
            return []

        # postgre-version sql
        task['PG_SQL'] = parsed_sql.sql(dialect="postgres")

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
            neg_user = f'usr_{len(db_privilege_2_user[db])}'
            db_privilege_2_user[db][neg_priv] = neg_user

        task['neg_user'] = neg_user

    if not os.path.exists(output_path):
        os.mkdir(output_path)

    with open(os.path.join(output_path, 'dev.json'), "w") as f:
        json.dump(tasks, f)

    with open(os.path.join(output_path, 'usr_2_table_privilege.json'), "w") as f:
        json.dump(
            {db: {user: priv for priv, user in db_privilege_2_user[db].items()} for db in db_privilege_2_user.keys()},
            f)

def generate_test_data_v2(input_file, output_path):
    db_2_tables = {
        "california_schools": ["satscores", "schools", "frpm"],
        "card_games": ["foreign_data", "legalities", "cards", "rulings", "sets", "set_translations"],
        "codebase_community": ["postlinks", "tags", "users", "badges", "comments", "posthistory", "posts", "votes"],
        "debit_card_specializing": ["customers", "gasstations", "products", "transactions_1k", "yearmonth"],
        "european_football_2": ["country", "league", "match", "player", "player_attributes", "team", "team_attributes"],
        "financial": ["district", "account", "disp", "client", "card", "loan", "order", "trans"],
        "student_club": ["event", "attendance", "member", "major", "zip_code", "budget", "expense", "income"],
        "superhero": ["attribute", "hero_attribute", "superhero", "alignment", "colour", "gender", "publisher", "race",
                      "hero_power", "superpower"],
        "thrombosis_prediction": ["examination", "patient", "laboratory"],
        "toxicology": ["molecule", "atom", "connected", "bond"],
    }

    # 测如下版本：
    # 有全部权限
    # 有读没有写权限
    # 全部权限都没有

    db_privilege_2_user = defaultdict(dict)

    with open(input_file) as f:
        tasks = json.load(f)

    for task in tasks:
        db = task['db_id']

        try:
            # Parse the SQL statement using the specified dialect
            parsed_sql = parse_one(task['SQL'], dialect="postgres")
        except sqlglot.errors.ParseError as e:
            print(f"Error parsing SQL: {e}")
            return []

        table = tuple([extract_tables(parsed_sql)[0]])

        # positive sample user
        if db not in db_privilege_2_user:
            tables = db_2_tables[db]
            db_privilege_2_user[db][tuple([tuple(sorted(tables)), tuple(['SELECT', 'UPDATE', 'INSERT', 'DELETE'])])] = 'v2_superuser'

        task['pos_user'] = 'v2_superuser'

        # negative user
        semi_neg_priv = tuple([table, tuple(['SELECT'])])
        neg_priv = None
        for table_iter in db_2_tables[db]:
            if table_iter != table[0]:
                neg_priv = tuple([tuple([table_iter]), tuple(['SELECT', 'UPDATE', 'INSERT', 'DELETE'])])
                break

        for priv, user_type in zip([semi_neg_priv, neg_priv], ['semi_neg_user', 'neg_user']):
            if priv in db_privilege_2_user[db]:
                user = db_privilege_2_user[db][priv]
            else:
                user = f'v2_usr_{len(db_privilege_2_user[db])}'
                db_privilege_2_user[db][priv] = user

            task[user_type] = user

    if not os.path.exists(output_path):
        os.mkdir(output_path)

    with open(os.path.join(output_path, 'dev.json'), "w") as f:
        json.dump(tasks, f)

    with open(os.path.join(output_path, 'usr_2_table_privilege.json'), "w") as f:
        json.dump(
            {db: {user: priv for priv, user in db_privilege_2_user[db].items()} for db in db_privilege_2_user.keys()},
            f)

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
#     # get_db_tables()
    path = '/home/ldd461759/code/mcp_db/test_db/mc_test_v2/'
    # generate_test_data_v2(path + 'origin_dev.json', path)
    # generate_test_data_v1(path + 'origin_dev.json', path)
    # clear_user(path)
    create_user_priv(path)

    # restore_test_data(path + 'dev_modified.json', path)
