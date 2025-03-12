from collections.abc import Generator
from typing import Any
from urllib.parse import urlparse, urlunparse, quote, unquote
from dify_plugin.interfaces.agent import LLMModelConfig
import pymysql
from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage
from pymysql.cursors import DictCursor
from pydantic import BaseModel

class BasicParams(BaseModel):
    model: LLMModelConfig
    query: str
class RookieText2dataTool(Tool):
    
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        params = BasicParams(**tool_parameters)
        # 获取连接参数
        _, conn_params = self._build_mysql_dsn(
            self.runtime.credentials['db_url'],
            self.runtime.credentials['db_password']
        )
        # 获取元数据
        # metadata = self._get_metadata(conn_params)
        yield self.create_json_message({
            "status": "success",
            "data": conn_params
        })

    def _build_mysql_dsn(self, db_url: str, db_password: str) -> tuple[str, dict[str, Any]]:
        """
        将数据库URL和密码拼接为完整DSN，并返回解析后的连接参数
        
        参数：
            db_url (str): 格式示例 mysql://user@host:port/database
            db_password (str): 数据库密码（明文）
        
        返回：
            tuple: (完整DSN, 解析后的连接参数字典)
        
        异常：
            ValueError: 当URL格式无效时抛出
        """
        # 基础解析验证
        parsed = urlparse(db_url)
        if parsed.scheme != 'mysql':
            raise ValueError("仅支持mysql协议，当前协议：{}".format(parsed.scheme))

        # 解析用户名和主机信息
        username = parsed.username or 'root'
        password = quote(db_password, safe='')  # 处理特殊字符
        hostname = parsed.hostname or 'localhost'
        port = parsed.port or 3306

        # 处理数据库路径
        database = parsed.path.lstrip('/')
        if not database:
            database = 'test'

        # 构建新的netloc
        auth_part = f"{username}:{password}"
        netloc = f"{auth_part}@{hostname}"
        if parsed.port:
            netloc += f":{port}"

        # 生成完整DSN
        full_dsn = urlunparse((
            parsed.scheme,
            netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment
        ))

        # 生成连接参数字典（类型安全）
        connection_params = {
            'host': hostname,
            'port': port,
            'user': unquote(username),
            'password': unquote(db_password),  # 注意这里返回明文用于连接
            'database': database,
            'charset': 'utf8mb4',
            'connect_timeout': 5
        }

        return full_dsn, connection_params

    def _get_metadata(self, conn_params: dict[str, Any]) -> dict[str, Any]:
        """
        获取数据库元数据（表结构信息）
        
        返回结构示例：
        {
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "int", "comment": "主键ID"},
                        {"name": "name", "type": "varchar(255)", "comment": "用户名"}
                    ]
                }
            ]
        }
        """
        metadata = {"tables": []}
        
        try:
            with pymysql.connect(
                host=conn_params['host'],
                port=conn_params['port'],
                user=conn_params['user'],
                password=conn_params['password'],
                database=conn_params['database'],
                charset=conn_params['charset'],
                cursorclass=DictCursor
            ) as conn:
                with conn.cursor() as cursor:
                    # 获取所有表信息
                    cursor.execute("""
                        SELECT TABLE_NAME AS table_name,
                               TABLE_COMMENT AS table_comment
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_TYPE = 'BASE TABLE'
                    """)
                    tables = cursor.fetchall()

                    # 获取每个表的列信息
                    for table in tables:
                        cursor.execute("""
                            SELECT COLUMN_NAME AS name,
                                   COLUMN_TYPE AS type,
                                   COLUMN_COMMENT AS comment,
                                   IS_NULLABLE AS nullable,
                                   COLUMN_KEY AS key_type
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = DATABASE()
                            AND TABLE_NAME = %s
                            ORDER BY ORDINAL_POSITION
                        """, (table['table_name'],))
                        
                        columns = []
                        for col in cursor.fetchall():
                            columns.append({
                                "name": col['name'],
                                "type": col['type'],
                                "comment": col['comment'] or "",
                                "nullable": col['nullable'] == 'YES',
                                "primary_key": col['key_type'] == 'PRI'
                            })
                        
                        metadata['tables'].append({
                            "name": table['table_name'],
                            "comment": table['table_comment'] or "",
                            "columns": columns
                        })
        
        except pymysql.Error as e:
            code, msg = e.args
            error_map = {
                1142: ("权限不足，无法访问元数据表", 403),
                1045: ("数据库认证失败", 401),
                2003: ("无法连接数据库服务器", 503)
            }
            error_info = error_map.get(code, (f"数据库错误: {msg}", 500))
            raise RuntimeError(f"{error_info[0]} (错误码: {code})") from e
            
        return metadata