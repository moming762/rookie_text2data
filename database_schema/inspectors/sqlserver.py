from sqlalchemy.sql import text
from .base import BaseInspector
from sqlalchemy.engine import reflection  # 新增导入
from urllib.parse import quote_plus

class SQLServerInspector(BaseInspector):
    """SQL Server元数据获取实现"""
    
    def build_conn_str(self, host: str, port: int, database: str,
                      username: str, password: str) -> str:
        return (
            f"mssql+pyodbc://{quote_plus(username)}:{quote_plus(password)}"
            f"@{host}:{port}/{database}?"
            f"driver=ODBC+Driver+17+for+SQL+Server"
        )
    
    def get_table_names(self, inspector: reflection.Inspector) -> list[str]:
        return inspector.get_table_names(schema="dbo")
    
    def get_table_comment(self, inspector: reflection.Inspector,
                         table_name: str) -> str:
        with self.engine.connect() as conn:
            sql = """
                SELECT CAST(ep.value AS NVARCHAR(4000))
                FROM sys.tables t
                LEFT JOIN sys.extended_properties ep 
                    ON ep.major_id = t.object_id 
                    AND ep.minor_id = 0 
                    AND ep.name = 'MS_Description'
                WHERE SCHEMA_NAME(t.schema_id) = 'dbo' 
                    AND t.name = :table_name
            """
            return conn.execute(text(sql), {
                'table_name': table_name
            }).scalar() or ""
    
    def get_column_comment(self, inspector: reflection.Inspector,
                          table_name: str, column_name: str) -> str:
        with self.engine.connect() as conn:
            sql = """
                SELECT CAST(ep.value AS NVARCHAR(4000))
                FROM sys.columns c
                LEFT JOIN sys.extended_properties ep 
                    ON ep.major_id = c.object_id 
                    AND ep.minor_id = c.column_id 
                    AND ep.name = 'MS_Description'
                WHERE OBJECT_SCHEMA_NAME(c.object_id) = 'dbo'
                    AND OBJECT_NAME(c.object_id) = :table_name 
                    AND c.name = :column_name
            """
            return conn.execute(text(sql), {
                'table_name': table_name,
                'column_name': column_name
            }).scalar() or ""
    
    def normalize_type(self, raw_type: str) -> str:
        type_map = {
            'DATETIME2': 'DATETIME',
            'NVARCHAR': 'VARCHAR',
            'VARCHAR(MAX)': 'TEXT'
        }
        return type_map.get(raw_type.upper(), raw_type)
