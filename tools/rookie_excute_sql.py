from dify_plugin import Tool
from typing import Any
from collections.abc import Generator
from dify_plugin.entities.tool import ToolInvokeMessage

class RookieExcuteSqlTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        return "Hello world"
