from typing import Any
from dify_plugin import ToolProvider
from dify_plugin.errors.tool import ToolProviderCredentialValidationError
from urllib.parse import urlparse, parse_qs, unquote
import pymysql

class RookieText2dataProvider(ToolProvider):
    def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        try:
            # 强制依赖检查
            try:
                import cryptography
            except ImportError:
                raise RuntimeError("pymysql[cryptography] 未安装")

            # 参数校验
            if not credentials.get('db_url') or not credentials.get('db_password'):
                raise ValueError("Database URL and password are required")
            
            # 解析数据库URL并解码特殊字符
            db_url = credentials['db_url']
            parsed = urlparse(db_url)
            
            if parsed.scheme != 'mysql':
                raise ValueError("仅支持 mysql:// 协议")

            # 解码用户名和密码
            username = unquote(parsed.username) if parsed.username else 'root'
            password = unquote(credentials['db_password'])  # 关键解码
            host = parsed.hostname or 'localhost'
            port = parsed.port or 3306
            database = parsed.path.lstrip('/') or 'test'

            # 第一次连接尝试：默认不启用SSL
            ssl_config = None
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
                if self._is_ssl_handshake_error(oe):
                    # 重试SSL连接
                    conn = pymysql.connect(
                        host=host,
                        port=port,
                        user=username,
                        password=password,
                        database=database,
                        ssl={'ssl': True},
                        connect_timeout=5,
                    )
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                else:
                    raise
            finally:
                if conn:
                    conn.close()

        except ValueError as ve:
            raise ToolProviderCredentialValidationError(f"配置错误: {str(ve)}")
        except pymysql.OperationalError as oe:
            error_msg = f"连接失败: {str(oe)}"
            if "caching_sha2_password" in str(oe) or "sha256_password" in str(oe):
                error_msg += "\n请执行：pip install pymysql[cryptography]"
            raise ToolProviderCredentialValidationError(error_msg)
        except RuntimeError as re:
            # 捕获依赖缺失错误
            raise ToolProviderCredentialValidationError(str(re))
        except Exception as e:
            raise ToolProviderCredentialValidationError(f"未知错误: {str(e)}")

    def _is_ssl_handshake_error(self, oe: pymysql.OperationalError) -> bool:
        error_code = oe.args[0]
        error_msg = str(oe).lower()
        return ('ssl' in error_msg or 'handshake' in error_msg) or error_code == 1043