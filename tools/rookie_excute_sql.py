from dify_plugin import Tool
from typing import Any
from collections.abc import Generator
from dify_plugin.entities.tool import ToolInvokeMessage
from utils.alchemy_db_client import execute_sql

class RookieExcuteSqlTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        # 获取传入的 SQL 语句
        sql = tool_parameters.get("sql")
        if not sql:
            raise ValueError("SQL 语句不能为空")
        # 简单的风险判断：检测是否包含敏感的关键字
        risk_keywords = ["DROP", "DELETE", "TRUNCATE", "ALTER", "UPDATE", "INSERT"]
        if any(keyword in sql.upper() for keyword in risk_keywords):
            raise ValueError("SQL 语句存在风险")

        # 获取数据库连接参数
        db_type = tool_parameters.get("db_type")
        host = tool_parameters.get("host")
        port = tool_parameters.get("port")
        database = tool_parameters.get("db_name")
        username = tool_parameters.get("username")
        password = tool_parameters.get("password")

        if not all([db_type, host, port, database, username, password]):
            raise ValueError("数据库连接参数不能为空")
        
        try:
            # 执行 SQL 语句（查询或非查询）
            result = execute_sql(db_type, host, int(port), database, username, password, sql,"")
            yield self.create_json_message({
                    "status": "success",
                    "result": result
                }  
            )
        except Exception as e:
            raise ValueError(f"数据库操作失败：{str(e)}")
