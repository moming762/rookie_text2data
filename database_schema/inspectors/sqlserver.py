from sqlalchemy.sql import text
from .base import BaseInspector
from sqlalchemy.engine import reflection
from urllib.parse import quote_plus

class SQLServerInspector(BaseInspector):
    """SQL Server元数据获取实现"""

    def __init__(self, host, port, database, username, password, schema_name = None, **kwargs):
        super().__init__(host, port, database, username, password, schema_name, **kwargs)
        self.schema_name = schema_name if schema_name != None else 'dbo'
    
    def build_conn_str(self, host: str, port: int, database: str,
                      username: str, password: str) -> str:

        # import os
        # driver = 'ODBC+Driver+17+for+SQL+Server' if os.name == 'posix' else 'SQL Server'
        # driver = 'ODBC+Driver+17+for+SQL+Server'
        return (
            f"mssql+pymssql://{quote_plus(username)}:{quote_plus(password)}"
            f"@{host}:{port}/{database}"
        )
    
    def get_table_names(self, inspector: reflection.Inspector) -> list[str]:
        return inspector.get_table_names(schema=self.schema_name)
    
    def get_table_comment(self, inspector: reflection.Inspector,
                         table_name: str) -> str:
        sql = """
            SELECT ep.value
            FROM sys.tables t
            LEFT JOIN sys.extended_properties ep ON 
                ep.major_id = t.object_id AND
                ep.minor_id = 0 AND
                ep.name = 'MS_Description'
            WHERE t.name = :table_name AND
                    SCHEMA_NAME(t.schema_id) = :schema_name
        """
        return self.conn.execute(text(sql), {
            'table_name': table_name,
            'schema_name': self.schema_name
        }).scalar() or ""
    
    def get_column_comment(self, inspector: reflection.Inspector,
                          table_name: str, column_name: str) -> str:
        sql = """
            SELECT ep.value
            FROM sys.columns c
            INNER JOIN sys.tables t ON c.object_id = t.object_id
            LEFT JOIN sys.extended_properties ep ON 
                ep.major_id = c.object_id AND
                ep.minor_id = c.column_id AND
                ep.name = 'MS_Description'
            WHERE t.name = :table_name AND
                    c.name = :column_name AND
                    SCHEMA_NAME(t.schema_id) = :schema_name
        """
        return self.conn.execute(text(sql), {
            'table_name': table_name,
            'column_name': column_name,
            'schema_name': self.schema_name
        }).scalar() or ""
    
    def normalize_type(self, raw_type: str) -> str:
        # 移除括号内的长度或精度信息
        return raw_type.split('(')[0].upper()