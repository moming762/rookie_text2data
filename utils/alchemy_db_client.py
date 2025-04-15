from typing import Any
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus # 用于对URL进行编码
from typing import Any, Optional, Union

def get_db_schema(
        db_type: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        table_names: str | None = None
) -> dict[str, Any] | None:
    """
    获取数据库表结构信息
    :param db_type: 数据库类型 (mysql/oracle/sqlserver/postgresql)
    :param host: 主机地址
    :param port: 端口号
    :param database: 数据库名
    :param username: 用户名
    :param password: 密码
    :param table_names: 要查询的表名，以逗号分隔的字符串，如果为None则查询所有表
    :return: 包含所有表结构信息的字典
    """
    result: dict[str, Any] = {}
    # 构建连接URL
    driver = {
        'mysql': 'pymysql',
        'oracle': 'cx_oracle',
        'sqlserver': 'pymssql',
        'postgresql': 'psycopg2'
    }.get(db_type.lower(), '')

    encoded_username = quote_plus(username)
    encoded_password = quote_plus(password)

    engine = create_engine(f'{db_type.lower()}+{driver}://{encoded_username}:{encoded_password}@{host}:{port}/{database}')
    inspector = inspect(engine)

    # 获取字段注释的SQL语句
    column_comment_sql = {
        'mysql': f"SELECT COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = :table_name AND COLUMN_NAME = :column_name",
        'oracle': "SELECT COMMENTS FROM ALL_COL_COMMENTS WHERE TABLE_NAME = :table_name AND COLUMN_NAME = :column_name",
        'sqlserver': "SELECT CAST(ep.value AS NVARCHAR(MAX)) FROM sys.columns c LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id WHERE OBJECT_NAME(c.object_id) = :table_name AND c.name = :column_name",
        'postgresql': """
            SELECT pg_catalog.col_description(c.oid, cols.ordinal_position::int)
            FROM pg_catalog.pg_class c
            JOIN information_schema.columns cols
            ON c.relname = cols.table_name
            WHERE c.relname = :table_name AND cols.column_name = :column_name
        """
    }.get(db_type.lower(), "")

    try:
        # 获取所有表名
        all_tables = inspector.get_table_names()

        # 如果指定了table_names，则过滤表名
        target_tables = all_tables

        if table_names:
            target_tables = [table.strip() for table in table_names.split(',')]
            # 过滤出实际存在的表
            target_tables = [table for table in target_tables if table in all_tables]
        print(f"Retrieving table metadata for {len(target_tables)} tables...")
        for table_name in target_tables:
            # 获取表注释
            table_comment = ""
            try:
                table_comment = inspector.get_table_comment(table_name).get("text") or ""
            except SQLAlchemyError as e:
                raise ValueError(f"Failed to retrieve table comments: {str(e)}")

            table_info = {
                'comment': table_comment,
                'columns': []
            }

            for column in inspector.get_columns(table_name):
                # 获取字段注释
                column_comment = ""
                try:
                    with engine.connect() as conn:
                        stmt = text(column_comment_sql)
                        column_comment = conn.execute(stmt, {
                            'table_name': table_name,
                            'column_name': column['name']
                        }).scalar() or ""
                except SQLAlchemyError as e:
                    print(f"Warning: failed to get comment for {table_name}.{column['name']} - {e}")
                    column_comment = ""

                table_info['columns'].append({
                    'name': column['name'],
                    'comment': column_comment,
                    'type': str(column['type'])
                })

            result[table_name] = table_info
        return result
    except SQLAlchemyError as e:
        raise ValueError(f"Failed to retrieve database table metadata: {str(e)}")
    finally:
        engine.dispose()

def format_schema_dsl(schema: dict[str, Any], with_type: bool = True, with_comment: bool = False) -> str:
    """
    将数据库表结构压缩为DSL格式
    :param schema: get_db_schema 返回的结构
    :param with_type: 是否保留字段类型
    :param with_comment: 是否保留字段注释
    :return: 压缩后的 DSL 字符串
    """
    type_aliases = {
        'INTEGER': 'i', 'INT': 'i', 'BIGINT': 'i', 'SMALLINT': 'i', 'TINYINT': 'i',
        'VARCHAR': 's', 'TEXT': 's', 'CHAR': 's',
        'DATETIME': 'dt', 'TIMESTAMP': 'dt', 'DATE': 'dt',
        'DECIMAL': 'f', 'NUMERIC': 'f', 'FLOAT': 'f', 'DOUBLE': 'f',
        'BOOLEAN': 'b', 'BOOL': 'b',
        'JSON': 'j'
    }
    lines = []
    for table_name, table_data in schema.items():
        column_parts = []

        for col in table_data['columns']:
            parts = [col['name']]
            if with_type:
                raw_type = col['type'].split('(')[0].upper()
                col_type = type_aliases.get(raw_type, raw_type.lower())
                parts.append(col_type)
            if with_comment and col.get('comment'):
                parts.append(f"# {col['comment']}")
            column_parts.append(":".join(parts))

        # 构建表注释
        if with_comment and table_data.get('comment'):
            lines.append(f"# {table_data['comment']}")
        lines.append(f"T:{table_name}({', '.join(column_parts)})")

    return "\n".join(lines)

def execute_sql(
    db_type: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    sql: str,
    params: Optional[dict[str, Any]] = None,
    schema: Optional[str] = None
) -> Union[list[dict[str, Any]], dict[str, Any], None]:
    """
    增强版 SQL 执行函数，支持 PostgreSQL schema
    
    参数新增:
        schema: 指定目标schema（主要用于PostgreSQL）
    """
    # 参数预处理
    params = params or {}
    driver = _get_driver(db_type)
    encoded_username = quote_plus(username)
    encoded_password = quote_plus(password)
    connect_args = {}

    # PostgreSQL 特殊处理
    if db_type.lower() == 'postgresql' and schema:
        connect_args['options'] = f"-c search_path={schema}"
    
    # 构建连接字符串
    connection_uri = _build_connection_uri(
        db_type, driver, encoded_username, encoded_password,
        host, port, database
    )

    try:
        engine = create_engine(connection_uri, connect_args=connect_args)
        with engine.begin() as conn:
            # 显式设置schema（部分数据库需要）
            if db_type.lower() == 'postgresql' and schema:
                conn.execute(text(f"SET search_path TO {schema}"))
                
            result_proxy = conn.execute(text(sql), params)
            
            return _process_result(result_proxy)
            
    except SQLAlchemyError as e:
        raise ValueError(f"数据库操作失败：{str(e)}")
    finally:
        if 'engine' in locals():
            engine.dispose()

def _get_driver(db_type: str) -> str:
    """获取数据库驱动"""
    drivers = {
        'mysql': 'pymysql',
        'oracle': 'cx_oracle',
        'sqlserver': 'pymssql',
        'postgresql': 'psycopg2'
    }
    return drivers.get(db_type.lower(), '')

def _build_connection_uri(
    db_type: str,
    driver: str,
    username: str,
    password: str,
    host: str,
    port: int,
    database: str
) -> str:
    """构建数据库连接字符串"""
    if db_type.lower() == 'sqlserver':
        return f"mssql+{driver}://{username}:{password}@{host}:{port}/{database}?charset=utf8"
    return f"{db_type}+{driver}://{username}:{password}@{host}:{port}/{database}"

def _process_result(result_proxy) -> Union[list[dict], dict, None]:
    """处理执行结果"""
    if result_proxy.returns_rows:
        return [dict(row._mapping) for row in result_proxy]
    return {"rowcount": result_proxy.rowcount}