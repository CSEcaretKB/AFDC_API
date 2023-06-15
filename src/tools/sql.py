import keyring
import psycopg2 as psy
import psycopg2.sql as psysql
import pandas as pd
import yaml
from tqdm import tqdm
import os
from src.tools.db_structure import DatabaseStructure
from src.tools.db_connection import DatabaseConnection
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


class SQL:
    sql_config = 'configs/db_server.yaml'
    admin_user = 'postgres'

    def __init__(self):
        self.NAMESPACE = os.environ.get('NAMESPACE')
        self.DATABASE_NAME = os.environ.get('NAMESPACE')
        self.database = self.DATABASE_NAME.lower()
        self.db_configs = {}
        self.db_structure = None
        self.extract_db_structure()
        self.load_db_configs()
        self.validate_admin_password()
        self.create_database()
        self.create_tables()
        self.create_read_only_users()
        self.truncate_db()

    def extract_db_structure(self):
        self.db_structure = DatabaseStructure()

    def load_db_configs(self):
        with open(self.sql_config, 'r') as yml:
            db_configs = yaml.full_load(yml)

        # Create user keys in db_configs
        for user in db_configs['users']:
            self.db_configs[user] = {
                'hostname': db_configs['hostname'],
                'port': db_configs['port'],
                'username': user
            }

    def validate_admin_password(self):
        user = 'postgres'
        self._validate_db_user_passwords(user=user)

    def create_read_only_users(self):
        admin_password = keyring.get_password(self.NAMESPACE, f'{self.admin_user}_password')
        users = [x for x in self.db_configs.keys() if x != 'postgres']
        for user in users:

            for user in users:

                admin_configs = self.db_configs[self.admin_user]

                with DatabaseConnection(host=admin_configs['hostname'],
                                        database=None,
                                        port=admin_configs['port'],
                                        user=admin_configs['username'],
                                        password=admin_configs['password']) as conn:

                    if conn is not None:
                        conn.autocommit = True
                        cur = conn.cursor()

                        # Create new user (if user doesn't exist)
                        stmt = psysql.SQL("""SELECT EXISTS( SELECT  1
                                                                    FROM    pg_roles
                                                                    WHERE   rolname={username}
                                                                    );""").format(username=psysql.Literal(user))

                        cur.execute(stmt)
                        # conn.commit()
                        user_exists = cur.fetchone()[0]

                        if not user_exists:
                            stmt = psysql.SQL("""CREATE USER {username} WITH PASSWORD {password};""") \
 \
                                .format(username=psysql.Identifier(user),
                                        password=psysql.Literal(f'{user}_{admin_password}'))
                        cur.execute(stmt)
                        # conn.commit()

                    # Grant connect access
                    # for client, database in self.db_names.items():
                        stmt = psysql.SQL("""GRANT CONNECT ON DATABASE {database} TO {username};""") \
                            .format(database=psysql.Identifier(self.database),
                                    username=psysql.Identifier(user))
                        cur.execute(stmt)
                        # conn.commit()

                    cur.close()

            # for client, database in self.db_names.items():
                    with DatabaseConnection(host=admin_configs['hostname'],
                                            database=self.database,
                                            port=admin_configs['port'],
                                            user=admin_configs['username'],
                                            password=admin_configs['password']) as conn:

                        if conn is not None:
                            conn.autocommit = True
                            cur = conn.cursor()

                            # Grant usage on schema

                            stmt = psysql.SQL("""GRANT USAGE ON SCHEMA {schema} TO {username};""") \
                                .format(schema=psysql.Identifier('public'),
                                        username=psysql.Identifier(user))
                            cur.execute(stmt)
                            # conn.commit()

                            # Grant SELECT for specified tables
                            read_only_tables = self.db_structure.read_only_tables
                            for table in read_only_tables:
                                stmt = psysql.SQL("""GRANT SELECT ON {table} TO {username};""") \
                                    .format(table=psysql.Identifier(table),
                                            username=psysql.Identifier(user))
                                cur.execute(stmt)
                                # conn.commit()

                            cur.close()

                # Append user passwords to db_configs
                self.db_configs[user]['password'] = f'{user}_{admin_password}'

    def _validate_db_user_passwords(self, user):
        # Validate postgres db server password
        password_key = f'{user}_password'
        credential = keyring.get_credential(self.NAMESPACE, password_key)

        key_valid = False
        if credential:
            self.db_configs[user]['password'] = credential.password
            key_valid = self.test_db_password(user=user)

        if not key_valid:
            prompt = f'\n*** Please provide the password for the Postgres Database Server - {user} User***\n'
            print(prompt)
            password_input = input('Enter password:')
            self.db_configs[user]['password'] = password_input
            print('*** Testing Password Provided ***')
            key_valid = self.test_db_password(user=user)
            if key_valid:
                print('*** Password Validated! ***')
                keyring.set_password(self.NAMESPACE, password_key, self.db_configs[user]['password'])
            else:
                raise Exception(f'Please supply the correct password for the Postgres Database Server - {user} User')

    def test_db_password(self, user):
        conn_test = None

        user_configs = self.db_configs[user]

        with DatabaseConnection(host=user_configs['hostname'],
                                database=None,
                                port=user_configs['port'],
                                user=user_configs['username'],
                                password=user_configs['password']) as conn:

            if conn is not None:
                conn_test = True
            else:
                conn_test = False
        return conn_test

    # @staticmethod
    # def connect_db_server(user):
    #     conn = None
    #     try:
    #         conn = psy.connect(
    #             user=user['username'],
    #             password=user['password'],
    #             host=user['hostname'],
    #             port=user['port']
    #         )
    #     except (Exception, psy.DatabaseError) as error:
    #         print(error)
    #     return conn
    #
    # @staticmethod
    # def connect_db(user, database):
    #     conn = None
    #     try:
    #         conn = psy.connect(
    #             database=database,
    #             user=user['username'],
    #             password=user['password'],
    #             host=user['hostname'],
    #             port=user['port']
    #         )
    #     except (Exception, psy.DatabaseError) as error:
    #         print(error)
    #     return conn

    def create_database(self):
        print(f'\n--- VERIFY THAT {self.database.upper()} DATABASE EXISTS ---')

        admin_configs = self.db_configs[self.admin_user]
        db_exists = None

        # Verify that database exists
        with DatabaseConnection(host=admin_configs['hostname'],
                                database=None,
                                port=admin_configs['port'],
                                user=admin_configs['username'],
                                password=admin_configs['password']) as conn:

            if conn is not None:
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute("SELECT datname FROM pg_database;")
                # conn.commit()
                list_databases = cur.fetchall()
                check_db = self.database

                if (check_db,) in list_databases:
                    print(f'\t{check_db} database already exists')
                    db_exists = True
                else:
                    print(f'\t\t{check_db} database missing')
                    db_exists = False
                    # create database if missing
                    cur.execute(f"CREATE DATABASE {self.database.upper()};")
                    # conn.commit()
                    print(f'\tSuccessfully created {check_db} database')
                cur.close()

        # If database is being created, create the postgis extension
        if not db_exists:
            with DatabaseConnection(host=admin_configs['hostname'],
                                    database=self.database,
                                    port=admin_configs['port'],
                                    user=admin_configs['username'],
                                    password=admin_configs['password']) as conn:
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute("CREATE EXTENSION postgis;")
                # conn.commit()
                cur.close()

    def create_tables(self):
        print(f'\n--- VERIFY THAT {self.database.upper()} DATABASE TABLES EXISTS ---')

        admin_configs = self.db_configs[self.admin_user]

        with DatabaseConnection(host=admin_configs['hostname'],
                                database=self.database,
                                port=admin_configs['port'],
                                user=admin_configs['username'],
                                password=admin_configs['password']) as conn:

            if conn is not None:
                conn.autocommit = True

                cur = conn.cursor()

                # Loop through list of tables
                client_db_structure = self.db_structure.db_structure
                for table, sql_stmt in client_db_structure.items():
                    cur.execute("""SELECT EXISTS(
                                                SELECT * 
                                                FROM information_schema.tables 
                                                WHERE table_name=%(table)s
                                                );""",
                                {'table': table})
                    # conn.commit()
                    table_exists = cur.fetchone()[0]

                    if table_exists:
                        print(f'\t{table} table exists')
                    else:
                        print(f'\t{table} table missing')

                        cur.execute(sql_stmt)
                        # conn.commit()
                        print(f'\t\t{table} table created')
                cur.close()

    def df_insert_to_database(self, dataframe, table_name):
        retry_insert = False

        # Instantiate empty dataframe to store SQL INSERT ERRORS
        failures = pd.DataFrame()

        # Generate SQL Commands
        sql_commands = []
        for ix, row in tqdm(dataframe.iterrows(),
                            total=dataframe.shape[0],
                            desc=f'Generating INSERT INTO statements for {table_name}'):
            sql_commands.append(
                self.generate_sql_row_insert(table=table_name,
                                             data_series=row))

        # Attempt # 1 (Batch Execution)
        if sql_commands:
            try:
                self.execute_batch_sql_commands(sql_commands=sql_commands)
            except:
                retry_insert = True

        # Attempt # 2 (Individual Execution)
        if retry_insert:
            # Create database connection
            admin_configs = self.db_configs[self.admin_user]

            with DatabaseConnection(host=admin_configs['hostname'],
                                    database=self.database,
                                    port=admin_configs['port'],
                                    user=admin_configs['username'],
                                    password=admin_configs['password']) as conn:

                for i, sql_command in enumerate(tqdm(sql_commands,
                                                     total=len(sql_commands),
                                                     desc=f'Executing individual INSERT INTO statements for {table_name}')):
                    # Create cursor
                    conn.autocommit = True
                    cur = conn.cursor()

                    try:
                        # Try executing single SQL statement
                        cur.execute(sql_command)
                    except:
                        failures = failures.append(dataframe.iloc[i], ignore_index=True)
                        conn.rollback()
                    # else:
                    #     conn.commit()
                    finally:
                        cur.close()

        return failures

    def generate_sql_row_insert(self, table, data_series):
        sql_statement = f"""INSERT INTO \n\t{table} """

        cols = list(data_series.index)

        # Update SQL statement with column string
        sql_statement += f"""({', '.join(cols)})\nVALUES\n"""

        # Update SQL statement with row values
        # Add format string placeholders
        value_placeholders = []
        for col in cols:
            value_placeholders.append("{" + f"{col}" + "}")

        sql_statement += f"""\t({', '.join([x for x in value_placeholders])});\n"""

        # Extract row values
        row_dict = {}
        for col in cols:
            if pd.isna(data_series[col]):
                row_dict[col] = f"{'NULL'}"
            else:
                if isinstance(data_series[col], str):
                    row_dict[col] = f"""'{data_series[col].replace("'", "")}'"""
                else:
                    row_dict[col] = f"""'{data_series[col]}'"""

        sql_statement = sql_statement.format(**row_dict)
        return sql_statement

    def execute_batch_sql_commands(self, sql_commands):
        batch_query = ''.join(sql_commands)

        admin_configs = self.db_configs[self.admin_user]

        with DatabaseConnection(host=admin_configs['hostname'],
                                database=self.database,
                                port=admin_configs['port'],
                                user=admin_configs['username'],
                                password=admin_configs['password']) as conn:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(batch_query)
            # conn.commit()
            cur.close()

    def truncate_db(self):
        print(f'\n--- TRUNCATING EXISTING {self.database.upper()} DATABASE TABLES ---')

        admin_configs = self.db_configs[self.admin_user]

        with DatabaseConnection(host=admin_configs['hostname'],
                                database=self.database,
                                port=admin_configs['port'],
                                user=admin_configs['username'],
                                password=admin_configs['password']) as conn:

            if conn is not None:
                conn.autocommit = True

                cur = conn.cursor()

                # Loop through list of tables
                client_db_structure = self.db_structure.db_structure
                for table in client_db_structure.keys():
                    cur.execute("""SELECT EXISTS(
                                                        SELECT * 
                                                        FROM information_schema.tables 
                                                        WHERE table_name=%(table)s
                                                        );""",
                                {'table': table})
                    # conn.commit()
                    table_exists = cur.fetchone()[0]

                    if table_exists:
                        cur.execute(f"""TRUNCATE TABLE {table} CASCADE;""")
                cur.close()
