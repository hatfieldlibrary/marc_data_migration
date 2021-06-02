import psycopg2


class DatabaseConnector:

  def get_connection(self, database_name, password):
    conn = psycopg2.connect(database=database_name, user="postgres", password=password, host="127.0.0.1", port="5432")
    return conn
