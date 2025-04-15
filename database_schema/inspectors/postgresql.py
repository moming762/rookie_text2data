# database_schema/inspectors/postgresql.py
from sqlalchemy.sql import text
from sqlalchemy.engine import reflection
from .base import BaseInspector
from urllib.parse import quote_plus

class PostgreSQLInspector(BaseInspector):
    """PostgreSQL 元数据获取实现"""
    
    def __init__(self, host: str, port: int, database: str, 
                username: str, password: str, schema_name: str = None, **kwargs):
        super().__init__(host, port, database, username, password)
        self.schema_name = schema_name or "public"
    
    def build_conn_str(self, host: str, port: int, database: str,
                     username: str, password: str) -> str:
        encoded_password = quote_plus(password)
        return f"postgresql+psycopg2://{username}:{encoded_password}@{host}:{port}/{database}"
    
    def get_table_names(self, inspector: reflection.Inspector) -> list[str]:
        return inspector.get_table_names(schema=self.schema_name)
    
    def get_table_comment(self, inspector: reflection.Inspector, 
                        table_name: str) -> str:
        with self.engine.connect() as conn:
            sql = """
                SELECT obj_description(c.oid, 'pg_class')
                FROM pg_catalog.pg_class c
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = :schema AND c.relname = :table
            """
            result = conn.execute(
                text(sql), 
                {"schema": self.schema_name, "table": table_name}
            ).scalar()
            return result or ""
    
    def get_column_comment(self, inspector: reflection.Inspector,
                         table_name: str, column_name: str) -> str:
        with self.engine.connect() as conn:
            sql = """
                SELECT pg_catalog.col_description(c.oid, a.attnum)
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                WHERE n.nspname = :schema
                AND c.relname = :table
                AND a.attname = :column
                AND a.attnum > 0
                AND NOT a.attisdropped
            """
            result = conn.execute(
                text(sql),
                {
                    "schema": self.schema_name,
                    "table": table_name,
                    "column": column_name
                }
            ).scalar()
            return result or ""
    
    def normalize_type(self, raw_type: str) -> str:
        type_map = {
            'jsonb': 'JSON',
            'bytea': 'BLOB',
            'serial': 'INTEGER',
            'bigserial': 'BIGINT',
            'uuid': 'UUID',
            'int4': 'INTEGER',
            'timestamptz': 'TIMESTAMP WITH TIME ZONE'
        }
        return type_map.get(raw_type.lower(), raw_type)