# database_schema/core.py
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from .factory import InspectorFactory

def get_db_schema(
    db_type: str,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    table_names: str | None = None,
    schema_name: str | None = None
) -> dict | None:
    """
    获取数据库表结构信息
    """
    engine: Engine | None = None
    try:
        inspector = InspectorFactory.create_inspector(
            db_type=db_type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema_name=schema_name
        )
        
        engine = inspector.engine
        inspector_obj = inspect(engine)
        
        # 获取所有表名
        all_tables = inspector.get_table_names(inspector_obj)
        target_tables = (
            [t.strip() for t in table_names.split(',')] 
            if table_names 
            else all_tables
        )
        target_tables = [t for t in target_tables if t in all_tables]
        
        result = {}
        for table in target_tables:
            try:
                table_comment = inspector.get_table_comment(inspector_obj, table)
            except Exception as e:
                print(f"Failed to get table comment for {table}: {str(e)}")
                table_comment = ""
            
            columns = []
            for col in inspector_obj.get_columns(table, schema=inspector.schema_name):
                try:
                    raw_type = str(col['type'])
                    col_type = inspector.normalize_type(raw_type)
                    col_comment = inspector.get_column_comment(
                        inspector_obj, 
                        table, 
                        col['name']
                    )
                except Exception as e:
                    print(f"Error processing column {table}.{col['name']}: {str(e)}")
                    continue
                
                columns.append({
                    'name': col['name'],
                    'type': col_type,
                    'comment': col_comment
                })
            
            result[table] = {
                'comment': table_comment,
                'columns': columns
            }
        return result
        
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        return None
    finally:
        if engine:
            engine.dispose()