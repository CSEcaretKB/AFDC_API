import keyring
import psycopg2 as psy
import psycopg2.sql as psysql
import pandas as pd
import numpy as np
from datetime import datetime
import yaml
from tqdm import tqdm
from getpass import getpass
# from src.client.CALeVIP.database_structure import CALeVIP_DB
# from src.client.PECO.database_structure import PECO_DB
# from src.client.PECO.exports.data_retreival import PECO_DB_Retrieval

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


class SQL:
    sql_config = 'configs/core/db_server.yaml'
    admin_user = 'postgres'
    NAMESPACE = 'AFDC_API'
    DATABASE_NAME = 'AFDC_API'

    def __init__(self, clients, prod_env, db_name):
        self.clients = clients
        self.prod_env = prod_env
        self.db_suffix = db_name
        self.db_names = {}
        self.db_configs = {}
        self.db_structures = {}
        self.db_retrieval = {}
        # self.generate_db_names()
        # self.extract_db_structures()
        # self.load_db_configs()
        # self.validate_admin_password()
        # self.create_databases()
        # self.create_tables()
        # self.create_read_only_users()
        # self.extract_db_data_retrieval()

    def generate_db_names(self):
        for client in self.clients:
            if self.db_suffix is None:
                db_title = '_'.join([client.lower(), self.prod_env.lower()])
            else:
                db_title = '_'.join([client.lower(), self.prod_env.lower(), self.db_suffix.lower()])
            self.db_names[client] = db_title

    @staticmethod
    def db_structure_lookup(client):
        if client == 'PECO':
            return PECO_DB()
        elif client == 'CALeVIP':
            return CALeVIP_DB()

    @staticmethod
    def db_data_retrieval_lookup(client):
        if client == 'PECO':
            return PECO_DB_Retrieval()
        elif client == 'CALeVIP':
            return None

    def extract_db_structures(self):
        for client in self.clients:
            self.db_structures[client] = self.db_structure_lookup(client=client)

    def extract_db_data_retrieval(self):
        for client in self.clients:
            self.db_retrieval[client] = self.db_data_retrieval_lookup(client=client)

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

            conn = self.connect_db_server(user=self.db_configs[self.admin_user])

            if conn is not None:
                conn.autocommit = True
                cur = conn.cursor()

                # Create new user (if user doesn't exist)
                stmt = psysql.SQL("""SELECT EXISTS( SELECT  1
                                                    FROM    pg_roles
                                                    WHERE   rolname={username}
                                                    );""").format(username=psysql.Literal(user))
                cur.execute(stmt)
                user_exists = cur.fetchone()[0]

                if not user_exists:
                    stmt = psysql.SQL("""CREATE USER {username} WITH PASSWORD {password};""") \
                        .format(username=psysql.Identifier(user),
                                password=psysql.Literal(f'{user}_{admin_password}'))
                    cur.execute(stmt)

                # Grant connect access
                for client, database in self.db_names.items():
                    stmt = psysql.SQL("""GRANT CONNECT ON DATABASE {database} TO {username};""") \
                        .format(database=psysql.Identifier(database),
                                username=psysql.Identifier(user))
                    cur.execute(stmt)

                conn.close()

            for client, database in self.db_names.items():
                conn = self.connect_db(user=self.db_configs[self.admin_user], database=database)

                if conn is not None:
                    conn.autocommit = True
                    cur = conn.cursor()

                    # Grant usage on schema
                    stmt = psysql.SQL("""GRANT USAGE ON SCHEMA {schema} TO {username};""") \
                        .format(schema=psysql.Identifier('public'),
                                username=psysql.Identifier(user))
                    cur.execute(stmt)

                    # Grant SELECT for specified tables
                    read_only_tables = self.db_structures[client].read_only_tables
                    for table in read_only_tables:
                        stmt = psysql.SQL("""GRANT SELECT ON {table} TO {username};""") \
                            .format(table=psysql.Identifier(table),
                                    username=psysql.Identifier(user))
                        cur.execute(stmt)

                    conn.close()

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
            password_input = getpass()
            self.db_configs[user]['password'] = password_input
            print('*** Testing Password Provided ***')
            key_valid = self.test_db_password(user=user)
            if key_valid:
                print('*** Password Validated! ***')
                keyring.set_password(self.NAMESPACE, password_key, self.db_configs[user]['password'])
            else:
                raise Exception(f'Please supply the correct password for the Postgres Database Server - {user} User')

    def test_db_password(self, user):
        conn = self.connect_db_server(user=self.db_configs[user])
        if conn:
            return True
        else:
            return False

    @staticmethod
    def connect_db_server(user):
        conn = None
        try:
            conn = psy.connect(
                user=user['username'],
                password=user['password'],
                host=user['hostname'],
                port=user['port']
            )
        except (Exception, psy.DatabaseError) as error:
            print(error)
        return conn

    @staticmethod
    def connect_db(user, database):
        conn = None
        try:
            conn = psy.connect(
                database=database,
                user=user['username'],
                password=user['password'],
                host=user['hostname'],
                port=user['port']
            )
        except (Exception, psy.DatabaseError) as error:
            print(error)
        return conn

    def create_databases(self):
        for client, database in self.db_names.items():
            print(f'\n--- VERIFY THAT {database.upper()} DATABASE EXISTS ---')

            # Verify that database exists
            conn = self.connect_db_server(user=self.db_configs[self.admin_user])
            db_exists = None

            if conn is not None:
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute("SELECT datname FROM pg_database;")
                list_databases = cur.fetchall()
                check_db = database

                if (check_db,) in list_databases:
                    print(f'\t{check_db} database already exists')
                    db_exists = True
                else:
                    print(f'\t\t{check_db} database missing')
                    db_exists = False
                    # create database if missing
                    cur.execute(f"CREATE DATABASE {database.upper()};")
                    print(f'\tSuccessfully created {check_db} database')

                # Close connection
                cur.close()
                conn.close()

            # If database is being created, create the postgis extension
            if not db_exists:
                conn = self.connect_db(user=self.db_configs[self.admin_user], database=database)
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute("CREATE EXTENSION postgis;")
                cur.close()
                conn.close()

    def create_tables(self):
        for client, database in self.db_names.items():
            print(f'\n--- VERIFY THAT {database.upper()} DATABASE TABLES EXISTS ---')

            # Create connection
            conn = self.connect_db(user=self.db_configs[self.admin_user], database=database)

            if conn is not None:
                conn.autocommit = True

                # Loop through list of tables
                client_db_structure = self.db_structures[client].db_structure
                for table, sql_stmt in client_db_structure.items():
                    cur = conn.cursor()
                    cur.execute("""SELECT EXISTS(
                                                SELECT * 
                                                FROM information_schema.tables 
                                                WHERE table_name=%(table)s
                                                );""",
                                {'table': table})
                    table_exists = cur.fetchone()[0]

                    if table_exists:
                        print(f'\t{table} table exists')
                    else:
                        print(f'\t{table} table missing')

                        cur.execute(sql_stmt)
                        print(f'\t\t{table} table created')
                conn.close()

    def insert_audit_record(self, queue, audit_id, created_dt):
        for client, queue in queue.items():
            conn = self.connect_db(user=self.db_configs[self.admin_user],
                                   database=self.db_names[client])

            if conn is not None:
                conn.autocommit = True
                cur = conn.cursor()

                stmt = psysql.SQL("""INSERT INTO audit_process_log (audit_id, created_dt) 
                                    VALUES ({audit_id}, {created_dt});""") \
                    .format(audit_id=psysql.Literal(audit_id[client]),
                            created_dt=psysql.Literal(created_dt))

                cur.execute(stmt)
                conn.close()

    def load_s3_files_from_db(self, client):
        stmt = """SELECT evsp, filename FROM audit_s3_files;"""
        df = self.sql_to_df(client=client,
                            sql_stmt=stmt)
        return df

    def sql_to_df(self, client, sql_stmt):
        conn = self.connect_db(user=self.db_configs[self.admin_user],
                               database=self.db_names[client])
        df = pd.read_sql_query(sql_stmt, con=conn)
        conn.close()
        return df

    def df_insert_to_database(self, client, dataframe, table_name, add_created_dt=False, add_updated_dt=False,
                              add_delivered=False):
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
                                             data_series=row,
                                             add_created_dt=add_created_dt,
                                             add_updated_dt=add_updated_dt,
                                             add_delivered=add_delivered))

        # Attempt # 1 (Batch Execution)
        if sql_commands:
            try:
                self.execute_batch_sql_commands(client=client,
                                                sql_commands=sql_commands)
            except:
                retry_insert = True

        # Attempt # 2 (Individual Execution)
        if retry_insert:
            # Create database connection
            conn = self.connect_db(user=self.db_configs[self.admin_user],
                                   database=self.db_names[client])
            # Create cursor
            cur = conn.cursor()

            for i, sql_command in enumerate(tqdm(sql_commands,
                                                 total=len(sql_commands),
                                                 desc=f'Executing individual INSERT INTO statements for {table_name}')):
                try:
                    # Try executing single SQL statement
                    cur.execute(sql_command)
                    conn.commit()
                except:
                    failures = failures.append(dataframe.iloc[i], ignore_index=True)

                    # Clean up SQL Transaction
                    conn.rollback()

            # Close connection
            conn.close()

        return failures

    def generate_sql_row_insert(self, table, data_series, add_created_dt=False, add_updated_dt=False,
                                add_delivered=False):
        sql_statement = f"""INSERT INTO \n\t{table} """

        cols = list(data_series.index)
        created_series = pd.Series(dtype='datetime64[ns]')
        updated_series = pd.Series(dtype='datetime64[ns]')
        delivered_series = pd.Series(dtype=bool)

        # Add additional columns
        if add_created_dt:
            cols.append('created_dt')
            created_series = pd.Series({'created_dt': datetime.utcnow()}, dtype='datetime64[ns]')
            created_series['created_dt'] = created_series['created_dt'].tz_localize('UTC').tz_convert('UTC')

        if add_updated_dt:
            cols.append('updated_dt')
            updated_series = pd.Series({'updated_dt': datetime.utcnow()}, dtype='datetime64[ns]')
            updated_series['updated_dt'] = updated_series['updated_dt'].tz_localize('UTC').tz_convert('UTC')

        if add_delivered:
            cols.append('delivered')
            delivered_series = pd.Series({'delivered': False}, dtype=bool)

        # Append additional columns to original series
        if not created_series.empty:
            data_series = pd.concat([data_series, created_series])

        if not updated_series.empty:
            data_series = pd.concat([data_series, updated_series])

        if not delivered_series.empty:
            data_series = pd.concat([data_series, delivered_series])

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
                row_dict[col] = f"'{data_series[col]}'"

        sql_statement = sql_statement.format(**row_dict)
        return sql_statement

    def execute_batch_sql_commands(self, client, sql_commands):
        batch_query = ''.join(sql_commands)
        conn = self.connect_db(user=self.db_configs[self.admin_user],
                               database=self.db_names[client])
        cur = conn.cursor()
        cur.execute(batch_query)
        conn.commit()
        conn.close()

    @staticmethod
    def key_columns(table):
        lookup = {
            'audit_process_log': ['audit_id'],
            'audit_s3_files': ['file_id'],
            'audit_data_imports': ['audit_id', 'file_id'],
            'site': ['site_id'],
            'evse': ['site_id', 'evse_id'],
            'port': ['evse_id', 'port_id'],
            'session': ['evse_id', 'port_id', 'session_id'],
            'interval': ['session_id', 'interval_id'],
            'downtime': ['site_id', 'evse_id', 'port_id', 'event_start_utc'],
            'initial_application': ['application_id'],
            'post_installation': ['application_id'],
            'equipment': ['application_id', 'evse_id'],
            'program_administration': ['date'],
            'staging_site': ['audit_id', 'evsp', 'evsp_site_id'],
            'staging_evse': ['audit_id', 'evsp', 'evsp_site_id', 'evsp_evse_id'],
            'staging_port': ['audit_id', 'evsp', 'evsp_evse_id', 'evsp_port_id'],
            'staging_session': ['audit_id', 'evsp', 'evsp_port_id', 'evsp_session_id'],
            'staging_interval': ['audit_id', 'evsp', 'evsp_session_id', 'evsp_interval_id'],
            'staging_downtime': ['audit_id', 'evsp', 'evsp_site_id', 'evsp_evse_id', 'evsp_port_id', 'event_start_utc'],
            'staging_initial_application': [],
            'staging_post_installation': [],
            'staging_equipment': ['audit_id', 'application_id', 'evse_serial_number'],
            'staging_program_administration': [],
            'historical_site': [],
            'historical_evse': [],
            'historical_port': [],
            'historical_session': [],
            'historical_interval': [],
            'historical_downtime': [],
            'historical_initial_application': [],
            'historical_post_installation': [],
            'historical_equipment': [],
            'historical_program_administration': [],
        }

        return lookup[table]

    @staticmethod
    def non_null_columns(table):
        lookup = {
            'site': ['audit_id', 'file_id', 'evsp'],
            'evse': ['audit_id', 'file_id', 'evsp'],
            'port': ['audit_id', 'file_id', 'evsp'],
            'session': ['audit_id', 'file_id', 'evsp', 'charge_session_start_date', 'charge_session_start_time',
                        'charge_session_end_date', 'charge_session_end_time', 'charge_session_start_utc',
                        'charge_session_end_utc', 'connection_duration', 'energy_consumed_kwh'],
            'interval': ['audit_id', 'file_id', 'evsp', 'interval_start_date', 'interval_start_time',
                         'interval_end_date', 'interval_end_time', 'interval_start_utc',
                         'interval_end_utc', 'interval_duration', 'interval_energy_consumed_kwh', 'estimate'],
            'downtime': ['audit_id', 'file_id', 'evsp', 'event_start_utc'],
            'initial_application': ['audit_id', 'file_id'],
            'post_installation': ['audit_id', 'file_id'],
            'equipment': ['audit_id', 'file_id'],
            'program_administration': ['audit_id', 'file_id'],
        }

        return lookup[table]

    @staticmethod
    def key_delete_columns(table):
        lookup = {
            'staging_site': ['cse_site_id'],
            'staging_evse': ['cse_site_id', 'cse_evse_id'],
            'staging_port': ['cse_evse_id', 'cse_port_id'],
            'staging_session': ['cse_port_id', 'cse_session_id'],
            'staging_interval': ['cse_session_id', 'cse_interval_id'],
            'staging_downtime': ['cse_site_id', 'cse_evse_id', 'cse_port_id', 'cse_downtime_id'],
            'staging_initial_application': ['application_id'],
            'staging_post_installation': ['application_id'],
            'staging_equipment': ['application_id', 'evse_id'],
            'staging_program_administration': ['date'],
        }
        return lookup[table]

    @staticmethod
    def delivered_update_key_columns(table):
        lookup = {
            'site': ['site_id'],
            'evse': ['site_id', 'evse_id'],
            'port': ['evse_id', 'port_id'],
            'session': ['port_id', 'session_id'],
            'interval': ['session_id', 'interval_id'],
            'downtime': ['site_id', 'evse_id', 'port_id', 'event_start_date', 'event_start_time'],
            'initial_application': ['application_id'],
            'post_installation': ['application_id'],
            'equipment': ['application_id', 'evse_id'],
            'program_administration': ['date'],
        }

        return lookup[table]

    @staticmethod
    def validation_key_columns(table):
        lookup = {
            'site': ['cse_site_id'],
            'evse': ['site_id', 'evse_id'],
            'port': ['evse_id', 'port_id'],
            'session': ['port_id', 'session_id'],
            'interval': ['session_id', 'interval_id'],
            'downtime': ['site_id', 'evse_id', 'port_id', 'event_start_date', 'event_start_time'],
            'initial_application': ['application_id'],
            'post_installation': ['application_id'],
            'equipment': ['application_id', 'evse_id'],
            'program_administration': ['date'],
        }

        return lookup[table]

    @staticmethod
    def update_key_columns(table):
        lookup = {
            'site': ['site_id'],
            'evse': ['site_id', 'evse_id'],
            'port': ['evse_id', 'port_id'],
            'session': ['port_id', 'session_id'],
            'interval': ['session_id', 'interval_id'],
            'downtime': ['site_id', 'evse_id', 'port_id', 'downtime_id'],
            'initial_application': ['application_id'],
            'post_installation': ['application_id'],
            'equipment': ['application_id', 'evse_id'],
            'program_administration': ['date'],
        }

        return lookup[table]

    @staticmethod
    def audit_etl_process(audit_id, evsp, filename):
        file_dict = {'evsp': evsp,
                     'filename': filename}

        file_query = """  SELECT  *
                    FROM    audit_s3_files
                    WHERE   evsp = '{evsp}'
                            AND filename = '{filename}'""".format(**file_dict)

        return file_query

    def df_update_to_database(self, client, dataframe, table_name, update_dict, add_created_dt=False,
                              add_updated_dt=False, add_delivered=False, delivered_status=False):
        sql_commands = []
        for ix, row in tqdm(dataframe.iterrows(),
                            total=dataframe.shape[0],
                            desc=f'Generating UPDATE statements for {table_name}'):
            sql_commands.append(
                self.generate_sql_row_update(table=table_name,
                                             data_series=row,
                                             update_dict=update_dict[ix],
                                             add_created_dt=add_created_dt,
                                             add_updated_dt=add_updated_dt,
                                             add_delivered=add_delivered,
                                             delivered_status=delivered_status))

        # Execute SQL commands
        if sql_commands:
            self.execute_batch_sql_commands(client=client,
                                            sql_commands=sql_commands)

    def generate_sql_row_update(self, table, data_series, update_dict, add_created_dt=False, add_updated_dt=True,
                                add_delivered=False, delivered_status=False):
        sql_statement = f"""UPDATE {table} SET """
        cols = list(data_series.index)
        created_series = pd.Series(dtype='datetime64[ns]')
        updated_series = pd.Series(dtype='datetime64[ns]')
        delivered_series = pd.Series(dtype=bool)

        # Generate WHERE clause keys
        if delivered_status:
            where_keys = self.delivered_update_key_columns(table=table)
        else:
            if ('staging' in table) | ('historical' in table) | ('audit' in table):
                where_keys = self.key_columns(table=table)
            else:
                where_keys = self.update_key_columns(table=table)

        # Add additional columns
        if add_created_dt:
            cols.append('created_dt')
            created_series = pd.Series({'created_dt': datetime.utcnow()}, dtype='datetime64[ns]')
            created_series['created_dt'] = created_series['created_dt'].tz_localize('UTC').tz_convert('UTC')
            update_dict['created_dt'] = True

        if add_updated_dt:
            cols.append('updated_dt')
            updated_series = pd.Series({'updated_dt': datetime.utcnow()}, dtype='datetime64[ns]')
            updated_series['updated_dt'] = updated_series['updated_dt'].tz_localize('UTC').tz_convert('UTC')
            update_dict['updated_dt'] = True

        if add_delivered:
            cols.append('delivered')
            delivered_series = pd.Series({'delivered': delivered_status}, dtype=bool)
            update_dict['delivered'] = True

        # Append additional columns to original series
        if not created_series.empty:
            data_series = pd.concat([data_series, created_series])

        if not updated_series.empty:
            data_series = pd.concat([data_series, updated_series])

        if not delivered_series.empty:
            data_series = pd.concat([data_series, delivered_series])

        # Update SQL statement with row values
        # Add format string placeholders
        value_placeholders = []
        col_names = []
        for col in cols:
            if update_dict.get(col):
                value_placeholders.append('{' + f'{col}' + '}')
                col_names.append(col)

        sql_statement += f"""{', '.join([f'{col_names[i]} = {x}' for i, x in enumerate(value_placeholders)])}"""

        # Extract row values
        row_dict = {}
        for col in col_names:
            if pd.isna(data_series[col]):
                row_dict[col] = f"{'NULL'}"
            else:
                row_dict[col] = f"'{data_series[col]}'"

        sql_statement = sql_statement.format(**row_dict)

        # Extract where clause values
        where_values = []
        for key in where_keys:
            where_values.append(f"{key} = '{data_series[key]}'")

        # Update SQL statement with where clause
        if where_values:
            sql_statement += f""" WHERE {' AND '.join([x for x in where_values])};"""
        else:
            sql_statement += ';'

        return sql_statement

    def gather_key_links(self, client, table, evsp):
        # Extract relevant key data
        if table == 'downtime':
            df = self.downtime_key_validation(client=client,
                                              table=table,
                                              evsp=evsp)
        else:
            df = self.charging_key_validation(client=client,
                                              table=table,
                                              evsp=evsp)

        return df

    def charging_key_validation(self, client, table, evsp):
        key_lookup = ['site', 'evse', 'port', 'session', 'interval']

        sql_stmt = f"""
                        SELECT	ls.evsp,
                                ls.site_id as evsp_site_id,
                                le.evse_id as evsp_evse_id,
                                lp.port_id as evsp_port_id,
                                lsn.session_id as evsp_session_id,
                                li.interval_id as evsp_interval_id,
                                ls.key as cse_site_id,
                                le.key as cse_evse_id,
                                lp.key as cse_port_id,
                                lsn.key as cse_session_id,
                                li.key as cse_interval_id

                        FROM	link_site as ls

                            FULL OUTER JOIN	link_evse as le
                                ON	le.evsp = ls.evsp
                                AND le.site_id = ls.site_id

                            FULL OUTER JOIN	link_port as lp
                                ON	lp.evsp = le.evsp
                                AND lp.evse_id = le.evse_id

                            FULL OUTER JOIN	link_session as lsn
                                ON	lsn.evsp = lp.evsp
                                AND lsn.evse_id = lp.evse_id
                                AND lsn.port_id = lp.port_id

                            FULL OUTER JOIN	link_interval as li
                                ON	li.evsp = lsn.evsp
                                AND li.session_id = lsn.session_id

                        WHERE   ls.evsp = '{evsp}'
                """

        df = self.sql_to_df(client=client,
                            sql_stmt=sql_stmt)

        # Determine which columns to keep
        keep_cols = ['evsp']
        col_prefix = ['evsp', 'cse']
        table_idx = key_lookup.index(table)
        if (table_idx + 1) == len(key_lookup):
            col_ids = key_lookup.copy()
        else:
            col_ids = key_lookup[:table_idx + 1].copy()

        for prefix in col_prefix:
            for col in col_ids:
                keep_cols.append(f'{prefix}_{col}_id')

        # Restrict column subset
        df = df[keep_cols]

        # Drop missing keys
        for prefix in col_prefix:
            df = df.loc[df[f'{prefix}_{table}_id'].notna(), :]

        # Remove duplicate rows
        df = df.drop_duplicates(ignore_index=True)

        # Check if there is a broken link
        check_break_cols = [c for c in keep_cols if c.startswith('evsp_')]
        breaks_present = np.where(df[check_break_cols].isna().sum(axis=1), True, False)
        df['broken_link'] = breaks_present

        return df

    def downtime_key_validation(self, client, table, evsp):
        sql_stmt = f"""
                        SELECT	ls.evsp,
                                ls.site_id as evsp_site_id,
                                le.evse_id as evsp_evse_id,
                                lp.port_id as evsp_port_id,
                                ls.key as cse_site_id,
                                le.key as cse_evse_id,
                                lp.key as cse_port_id,
                                ld.key as cse_downtime_id,
                                ld.event_start_utc 

                        FROM	link_site as ls

                            FULL OUTER JOIN	link_evse as le
                                ON	le.evsp = ls.evsp
                                AND le.site_id = ls.site_id

                            FULL OUTER JOIN	link_port as lp
                                ON	lp.evsp = le.evsp
                                AND lp.evse_id = le.evse_id

                            FULL OUTER JOIN	link_downtime as ld
                                ON	ld.evsp = lp.evsp
                                AND ld.site_id = ls.site_id
                                AND ld.evse_id = le.evse_id
                                AND ld.port_id = lp.port_id

                        WHERE   ls.evsp = '{evsp}'
                """

        df = self.sql_to_df(client=client,
                            sql_stmt=sql_stmt)

        # # Determine which columns to keep
        # keep_cols = ['evsp']
        # col_prefix = ['evsp', 'cse']
        # table_idx = key_lookup.index(table)
        # if (table_idx + 1) == len(key_lookup):
        #     col_ids = key_lookup.copy()
        # else:
        #     col_ids = key_lookup[:table_idx + 1].copy()
        #
        # for prefix in col_prefix:
        #     for col in col_ids:
        #         keep_cols.append(f'{prefix}_{col}_id')
        #
        # # Restrict column subset
        # df = df[keep_cols]

        # # Drop missing keys
        # for prefix in col_prefix:
        #     df = df.loc[df[f'{prefix}_{table}_id'].notna(), :]

        # Remove duplicate rows
        df = df.drop_duplicates(ignore_index=True)

        # Check if there is a broken link
        check_break_cols = [c for c in list(df) if c.startswith('evsp_')]
        breaks_present = np.where(df[check_break_cols].isna().sum(axis=1), True, False)
        df['broken_link'] = breaks_present

        return df

    def df_delete_from_database(self, client, dataframe, table_name):
        sql_commands = []
        for ix, row in tqdm(dataframe.iterrows(),
                            total=dataframe.shape[0],
                            desc=f'Generating DELETE FROM statements for {table_name}'):
            sql_commands.append(
                self.generate_sql_row_delete(table=table_name,
                                             data_series=row))

        # Execute SQL commands
        if sql_commands:
            self.execute_batch_sql_commands(client=client,
                                            sql_commands=sql_commands)

    def generate_sql_row_delete(self, table, data_series):
        sql_statement = f"""DELETE FROM {table} """

        # Generate WHERE clause keys
        where_keys = self.key_delete_columns(table=table)

        # Extract where clause values
        where_values = []
        for key in where_keys:
            where_values.append(f"{key} = '{data_series[key]}'")

        # Update SQL statement with where clause
        if where_values:
            sql_statement += f""" WHERE {' AND '.join([x for x in where_values])};"""
        else:
            sql_statement += ';'

        return sql_statement
