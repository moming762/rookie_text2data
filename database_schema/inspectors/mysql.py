from sqlalchemy.sql import text
from .base import BaseInspector
from sqlalchemy.engine import reflection  # 新增导入
from urllib.parse import quote_plus

class MySQLInspector(BaseInspector):
    """MySQL元数据获取实现"""
    
    def __init__(self, host: str, port: int, database: str, 
                username: str, password: str, schema_name: str = None, **kwargs):
        super().__init__(host, port, database, username, password, schema_name)
        # MySQL 中 schema 等同于 database
        self.schema_name = database  # 强制使用连接时指定的数据库名
    
    def build_conn_str(self, host: str, port: int, database: str,
                      username: str, password: str) -> str:
        return (
            f"mysql+pymysql://{quote_plus(username)}:{quote_plus(password)}"
            f"@{host}:{port}/{database}?charset=utf8mb4"
        )
    
    def get_table_names(self, inspector: reflection.Inspector) -> list[str]:
        return inspector.get_table_names()
    
    def get_table_comment(self, inspector: reflection.Inspector,
                         table_name: str) -> str:
        return inspector.get_table_comment(table_name).get("text", "")
    
    def get_column_comment(self, inspector: reflection.Inspector,
                          table_name: str, column_name: str) -> str:
        with self.engine.connect() as conn:
            sql = """
                SELECT COLUMN_COMMENT 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = :table_name 
                    AND COLUMN_NAME = :column_name
            """
            return conn.execute(text(sql), {
                'table_name': table_name,
                'column_name': column_name
            }).scalar() or ""
    
    def normalize_type(self, raw_type: str) -> str:
        return raw_type.split('(')[0].upper()