import psycopg2


class Database:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.conn = psycopg2.connect(
            host=self.host, port=self.port, user=self.username, password=self.password
        )
