import os
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor
from psycopg2.extras import execute_values
from app.config import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME, DB_PORT


class Database:
    def __init__(self):
        self.conn = None

    def connect(self):
        """Establishes a new database connection."""
        try:
            print(f"==>> DB_HOST: {DB_HOST}")
            self.conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT,
            )
        except OperationalError as e:
            print(f"[DB ERROR] Connection failed: {e}")
            self.conn = None
        return self.conn

    def in_transaction(self):
        return (
            self.conn is not None
            and self.conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION
        )

    def execute(self, query, params=None, commit=False):
        """Executes a raw SQL query."""
        if not self.conn:
            self.connect()

        try:
            cur = self.conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query, params)
            if commit:
                self.conn.commit()
            return cur

        except Exception as e:
            print(f"[DB ERROR] Query failed: {e}")
            self.conn.rollback()
            return None

    def bulk_execute_values(self, query, data_tuples):
        """Executes bulk insert/update using execute_values."""
        if not self.conn:
            self.connect()
        if not self.conn:
            return False

        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, data_tuples)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"[DB ERROR] Bulk execute failed: {e}")
            self.conn.rollback()
            return False
