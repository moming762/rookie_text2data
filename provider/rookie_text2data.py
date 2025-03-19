from typing import Any
from dify_plugin import ToolProvider
from dify_plugin.errors.tool import ToolProviderCredentialValidationError
from urllib.parse import urlparse, parse_qs
import pymysql

class RookieText2dataProvider(ToolProvider):
    def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        try:
            # 参数校验
            if not credentials.get('db_url') or not credentials.get('db_password'):
                raise ValueError("Database URL and password are required")
            
            # 解析数据库URL
            db_url = credentials['db_url']
            parsed = urlparse(db_url)
            
            # 验证协议
            if parsed.scheme != 'mysql':
                raise ValueError("Only mysql:// scheme is supported")

            # 提取连接参数
            username = parsed.username or 'root'
            password = credentials['db_password']
            host = parsed.hostname or 'localhost'
            port = parsed.port or 3306
            database = parsed.path.lstrip('/') or 'test'

            # 提取SSL模式（优先使用用户显式配置）
            query_params = parse_qs(parsed.query)
            ssl_mode = query_params.get('ssl_mode', [None])[0]

            # 第一次连接尝试：默认不启用SSL（除非用户显式要求）
            ssl_config = None
            if ssl_mode == 'REQUIRED':
                ssl_config = {'ssl': True}

            conn = None
            try:
                conn = pymysql.connect(
                    host=host,
                    port=port,
                    user=username,
                    password=password,
                    database=database,
                    ssl=ssl_config,
                    connect_timeout=5,
                )
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
            except pymysql.OperationalError as oe:
                # 如果是SSL相关错误且未显式配置SSL，尝试重连
                if self._is_ssl_handshake_error(oe) and ssl_mode is None:
                    # 第二次连接尝试：强制启用SSL
                    conn = pymysql.connect(
                        host=host,
                        port=port,
                        user=username,
                        password=password,
                        database=database,
                        ssl={'ssl': True},  # 启用基础SSL（不验证证书）
                        connect_timeout=5,
                    )
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                else:
                    raise  # 非SSL错误或已配置过SSL，直接抛出异常
            finally:
                if conn:
                    conn.close()

        except ValueError as ve:
            raise ToolProviderCredentialValidationError(f"配置错误: {str(ve)}")
        except pymysql.OperationalError as oe:
            error_msg = f"连接失败: {str(oe)}"
            if "caching_sha2_password" in str(oe):
                error_msg += "\n请确认已安装：pip install pymysql[cryptography]"
            raise ToolProviderCredentialValidationError(error_msg)
        except Exception as e:
            raise ToolProviderCredentialValidationError(f"未知错误: {str(e)}")

    def _is_ssl_handshake_error(self, oe: pymysql.OperationalError) -> bool:
        """判断是否为SSL握手错误"""
        error_code = oe.args[0]
        error_msg = str(oe).lower()
        # 常见SSL错误特征（可根据实际报错补充）
        ssl_keywords = ['ssl', 'handshake', 'encryption']
        return any(kw in error_msg for kw in ssl_keywords) or error_code == 1043