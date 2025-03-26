from dify_plugin import Tool
from typing import Any
from collections.abc import Generator
from dify_plugin.entities.tool import ToolInvokeMessage
from utils.alchemy_db_client import execute_sql
import json
from datetime import datetime, date
from decimal import Decimal

class RookieExcuteSqlTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        # 获取传入的 SQL 语句
        sql = tool_parameters.get("sql")
        if not sql:
            raise ValueError("SQL 语句不能为空")
        
        # 改进后的风险检测
        if self._contains_risk_commands(sql):
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
            result = execute_sql(
                db_type, host, int(port), database, 
                username, password, sql, ""
            )
            if (tool_parameters['result_format'] == 'json'):
                yield self.create_json_message({
                        "status": "success",
                        "result": result
                    }  
                )
            else:
                if result is not None:
                    message_text = json.dumps(
                        result, 
                        ensure_ascii=False, 
                        default=self._custom_serializer  # 关键修改点
                    )
                else:
                    message_text = "No data found"
                yield self.create_text_message(message_text)
        except Exception as e:
            raise ValueError(f"数据库操作失败：{str(e)}")

    def _contains_risk_commands(self, sql: str) -> bool:
        import re
        risk_keywords = {"DROP", "DELETE", "TRUNCATE", "ALTER", "UPDATE", "INSERT"}
        # 移除注释
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        sql = re.sub(r'--.*', '', sql)
        # 分割语句
        statements = re.split(r';\s*', sql)
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue
            # 匹配第一个单词（不区分大小写）
            match = re.match(r'\s*([^\s]+)', stmt, re.IGNORECASE)
            if match:
                first_word = match.group(1).upper()
                if first_word in risk_keywords:
                    return True
        return False
    
    def _custom_serializer(self, obj: Any) -> Any:
        """处理数据库常见不可序列化类型"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()  # 转换为 ISO8601 字符串
        elif isinstance(obj, Decimal):
            return float(obj)  # Decimal转浮点数
        # 添加其他需要处理的类型（如 bytes）
        raise TypeError(f"Unserializable type {type(obj)}")
