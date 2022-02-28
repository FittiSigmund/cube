class Postgres:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port


def postgres(dbname, user, password, host, port):
    return Postgres(dbname, user, password, host, port)
