from typing import Any
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

def get_db_schema(
        db_type: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        table_names: str | None = None
) -> dict[str, Any] | None:
    result: dict[str, Any] = {}

    driver = {
        'mysql': 'pymysql',
        'oracle': 'cx_oracle',
        'sqlserver': 'pymssql',
        'postgresql': 'psycopg2'
    }.get(db_type.lower(), '')
    
    engine = create_engine(f'{db_type.lower()}+{driver}://{username}:{password}@{host}:{port}/{database}')
    inspector = inspect(engine)

    # 字段注释查询语句
    column_comment_sql = {
        'mysql': f"SELECT COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = :table_name AND COLUMN_NAME = :column_name",
        'oracle': "SELECT COMMENTS FROM ALL_COL_COMMENTS WHERE TABLE_NAME = :table_name AND COLUMN_NAME = :column_name",
        'sqlserver': "SELECT CAST(ep.value AS NVARCHAR(MAX)) FROM sys.columns c LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id WHERE OBJECT_NAME(c.object_id) = :table_name AND c.name = :column_name",
        'postgresql': "SELECT col_description(:table_name::regclass, ordinal_position) FROM information_schema.columns WHERE table_name = :table_name AND column_name = :column_name"
    }.get(db_type.lower(), "")

    try:
        all_tables = inspector.get_table_names()
        target_tables = all_tables
        if table_names:
            target_tables = [t.strip() for t in table_names.split(',') if t.strip() in all_tables]

        for table_name in target_tables:
            # 修复点1：处理表注释返回值
            try:
                comment_data = inspector.get_table_comment(table_name)
                # 处理不同返回类型：字符串或字典
                table_comment = comment_data.get("text") if isinstance(comment_data, dict) else str(comment_data)
            except Exception as e:
                table_comment = f"Error getting comment: {str(e)}"

            # 确保columns初始化为列表
            table_info = {
                'comment': table_comment,
                'columns': []  # 明确初始化为列表
            }

            # 修复点2：添加列信息收集逻辑
            for column in inspector.get_columns(table_name):
                column_comment = ""
                try:
                    with engine.connect() as conn:
                        stmt = text(column_comment_sql)
                        res = conn.execute(stmt, {'table_name': table_name, 'column_name': column['name']})
                        column_comment = res.scalar() or ""
                except Exception as e:
                    column_comment = f"Error getting column comment: {str(e)}"

                # 安全添加列信息
                table_info['columns'].append({
                    'name': column['name'],
                    'type': str(column['type']),
                    'comment': column_comment,
                    'nullable': column.get('nullable', False),
                    'default': str(column.get('default', ''))
                })

            result[table_name] = table_info

        return result
    except SQLAlchemyError as e:
        raise ValueError(f"Database error: {str(e)}")
    finally:
        engine.dispose()