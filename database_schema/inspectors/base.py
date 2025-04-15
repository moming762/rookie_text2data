# database_schema/base.py
from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from urllib.parse import quote_plus

class BaseInspector(ABC):
    """元数据获取抽象基类"""
    
    def __init__(self, host: str, port: int, database: str, 
                username: str, password: str, schema_name: str = None, **kwargs):
        self.engine = create_engine(
            self.build_conn_str(host, port, database, username, password)
        )
        self.schema_name = schema_name  # 所有子类都会继承这个属性
    
    @abstractmethod
    def build_conn_str(self, host: str, port: int, database: str,
                     username: str, password: str) -> str:
        """构造数据库连接字符串"""
        pass
    
    @abstractmethod
    def get_table_names(self, inspector: reflection.Inspector) -> list[str]:
        """获取所有表名"""
        pass
    
    @abstractmethod
    def get_table_comment(self, inspector: reflection.Inspector, 
                        table_name: str) -> str:
        """获取表注释"""
        pass
    
    @abstractmethod
    def get_column_comment(self, inspector: reflection.Inspector,
                         table_name: str, column_name: str) -> str:
        """获取列注释"""
        pass
    
    @abstractmethod
    def normalize_type(self, raw_type: str) -> str:
        """标准化字段类型"""
        pass