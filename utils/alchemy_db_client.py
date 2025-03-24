from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

class AlchemyDBClient:
    """基于SQLAlchemy的统一数据库客户端"""
    
    # 数据库URL模式映射
    URL_PATTERNS = {
        'mysql': 'mysql+pymysql://{user}:{password}@{host}:{port}/{database}',
        'sqlserver': 'mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server',
        'pgsql': 'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    }

    def __init__(self, db_type: str, 
                 host: str, port: int, 
                 db_name: str, user: str, 
                 password: str, 
                 pool_size: int = 5):
        """
        初始化数据库客户端
        :param db_type: 数据库类型(mysql/sqlserver/pgsql)
        :param pool_size: 连接池大小(默认5)
        """
        if db_type not in self.URL_PATTERNS:
            raise ValueError(f"Unsupported database type: {db_type}")

        self.engine = create_engine(
            self.URL_PATTERNS[db_type].format(
                user=user,
                password=password,
                host=host,
                port=port,
                database=db_name
            ),
            pool_size=pool_size,
            pool_recycle=3600,
            echo_pool=False
        )
        
        self.Session = scoped_session(sessionmaker(bind=self.engine))

    def get_session(self):
        """获取线程安全的Session"""
        return self.Session()

    def execute_query(self, sql: str, params: dict = None):
        """执行查询语句"""
        with self.engine.connect() as conn:
            result = conn.execute(sql, params or {})
            return result.fetchall()

    def execute_update(self, sql: str, params: dict = None):
        """执行更新语句"""
        session = self.get_session()
        try:
            result = session.execute(sql, params or {})
            session.commit()
            return result.rowcount
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
