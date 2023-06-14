import psycopg2


class DatabaseConnection:
    def __init__(self, host, database, port, user, password):
        self.conn = None
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def __enter__(self):
        if self.database is None:
            # Create connection to DB Server
            self.conn = psycopg2.connect(**self.connect_db_server())
        else:
            # Create connection to Database
            self.conn = psycopg2.connect(**self.connect_db())
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self.conn = None

    def connect_db(self):
        connect_dict = {
            'host': self.host,
            'dbname': self.database,
            'port': self.port,
            'user': self.user,
            'password': self.password
        }
        return connect_dict

    def connect_db_server(self):
        connect_dict = {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password
        }
        return connect_dict
