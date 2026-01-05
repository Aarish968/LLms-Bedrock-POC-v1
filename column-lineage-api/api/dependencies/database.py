"""Database connection dependencies."""

from functools import lru_cache
from typing import AsyncGenerator, Optional

from sqlalchemy import create_engine, Engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy.orm import sessionmaker

from api.core.config import get_settings
from api.core.logging import get_logger

logger = get_logger(__name__)


@lru_cache()
def get_database_engine() -> Engine:
    """Get Snowflake database engine based on POC setting."""
    settings = get_settings()
    
    if settings.POC:
        # Use modern FastAPI connection method
        return _get_modern_database_engine()
    else:
        # Use legacy VPN-based connection method
        return _get_legacy_database_engine()


def _get_modern_database_engine() -> Optional[Engine]:
    """Get Snowflake database engine using modern method."""
    settings = get_settings()
    
    # Skip database connection if Snowflake credentials are not provided
    if not all([
        settings.SNOWFLAKE_ACCOUNT,
        settings.SNOWFLAKE_USER,
        settings.SNOWFLAKE_PASSWORD,
        settings.SNOWFLAKE_DATABASE,
    ]):
        logger.warning("Snowflake credentials not provided, skipping database connection")
        return None
    
    try:
        logger.info(
            "Attempting to connect to Snowflake using modern method",
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
        )
        
        # Create engine using snowflake-sqlalchemy with explicit connection parameters
        # This avoids URL parsing issues with organization-account format
        from snowflake.sqlalchemy import URL
        
        engine = create_engine(
            URL(
                account=settings.SNOWFLAKE_ACCOUNT,
                user=settings.SNOWFLAKE_USER,
                password=settings.SNOWFLAKE_PASSWORD,
                database=settings.SNOWFLAKE_DATABASE,
                schema=settings.SNOWFLAKE_SCHEMA,
                warehouse=settings.SNOWFLAKE_WAREHOUSE,
                role=settings.SNOWFLAKE_ROLE,
            ),
            echo=settings.DEBUG,
            pool_pre_ping=True,
            pool_recycle=3600,
            # Add connection arguments for Snowflake
            connect_args={
                "application": "column-lineage-api",
                "client_session_keep_alive": True,
            }
        )
        
        # Test connection
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT CURRENT_VERSION()"))
            version = result.fetchone()[0]
            logger.info("Modern database connection successful", snowflake_version=version)
        
        logger.info("Modern database engine created successfully")
        return engine
        
    except Exception as e:
        logger.error(
            "Failed to create modern database engine", 
            error=str(e),
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            database=settings.SNOWFLAKE_DATABASE,
        )
        return None


def _get_legacy_database_engine() -> Optional[Engine]:
    """Get Snowflake database engine using legacy VPN-based method."""
    settings = get_settings()
    
    try:
        logger.info(
            "Attempting to connect to Snowflake using legacy VPN method",
            environment=settings.RUN_ENV
        )
        
        # Import the legacy connection class
        try:
            from .database_connection import SnowflakeConnection
            logger.info("Successfully imported SnowflakeConnection")
        except ImportError as import_err:
            logger.error(f"Failed to import SnowflakeConnection: {import_err}")
            print(f"IMPORT ERROR: {import_err}")
            return None
        
        # Create legacy connection
        try:
            sf_connection = SnowflakeConnection(sf_env=settings.RUN_ENV)
            logger.info("SnowflakeConnection instance created")
        except Exception as init_err:
            logger.error(f"Failed to initialize SnowflakeConnection: {init_err}")
            print(f"INITIALIZATION ERROR: {init_err}")
            return None
        
        # Create connection engine
        try:
            engine = sf_connection.create_connection()
            logger.info("create_connection() called")
        except Exception as conn_err:
            logger.error(f"Failed in create_connection(): {conn_err}")
            print(f"CONNECTION CREATION ERROR: {conn_err}")
            print(f"Error type: {type(conn_err).__name__}")
            import traceback
            print(f"Full traceback:\n{traceback.format_exc()}")
            return None
        
        if engine:
            # Test connection
            try:
                with engine.connect() as conn:
                    from sqlalchemy import text
                    result = conn.execute(text("SELECT CURRENT_VERSION()"))
                    version = result.fetchone()[0]
                    logger.info("Legacy database connection successful", snowflake_version=version)
                    print(f"SUCCESS: Connected to Snowflake version {version}")
            except Exception as test_err:
                logger.error(f"Connection test failed: {test_err}")
                print(f"CONNECTION TEST ERROR: {test_err}")
                print(f"Error type: {type(test_err).__name__}")
                import traceback
                print(f"Full traceback:\n{traceback.format_exc()}")
                return None
            
            logger.info("Legacy database engine created successfully")
            return engine
        else:
            logger.error("create_connection() returned None")
            print("ERROR: create_connection() returned None - check AWS credentials and VPN")
            return None
            
    except Exception as e:
        logger.error(
            "Failed to create legacy database engine", 
            error=str(e),
            environment=settings.RUN_ENV
        )
        print(f"UNEXPECTED ERROR: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        print(f"Full traceback:\n{traceback.format_exc()}")
        return None


def get_database_session():
    """Get database session dependency."""
    engine = get_database_engine()
    SessionLocal = sessionmaker(bind=engine)
    
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


class DatabaseManager:
    """Database connection manager."""
    
    def __init__(self):
        self.settings = get_settings()
        self.engine = get_database_engine()
        self.logger = get_logger(self.__class__.__name__)
        self.mock_mode = self.engine is None
        
        if self.mock_mode:
            self.logger.info("Running in mock mode - no database connection")
        else:
            connection_type = "modern FastAPI" if self.settings.POC else "legacy VPN"
            self.logger.info(f"Database manager initialized with {connection_type} connection")
    
    def get_session(self):
        """Get a new database session."""
        if self.mock_mode:
            return None
        
        SessionLocal = sessionmaker(bind=self.engine)
        return SessionLocal()
    
    def execute_query(self, query: str, params: dict = None):
        """Execute a query and return results."""
        if self.mock_mode:
            self.logger.info("Mock query execution", query=query[:100])
            return []
        
        try:
            with self.engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text(query), params or {})
                
                # Commit the transaction for DDL and DML statements
                if any(keyword in query.upper().strip() for keyword in ['CREATE', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'TRUNCATE']):
                    conn.commit()
                
                return result.fetchall()
        except Exception as e:
            self.logger.error("Query execution failed", query=query, error=str(e))
            raise
    
    def test_connection(self) -> bool:
        """Test database connection."""
        if self.mock_mode:
            self.logger.info("Mock connection test - returning True")
            return True
        
        try:
            with self.engine.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            self.logger.error("Database connection test failed", error=str(e))
            return False
    
    def get_legacy_connection(self):
        """Get legacy SnowflakeConnection instance if using legacy mode."""
        if self.settings.POC:
            self.logger.warning("Legacy connection requested but POC=true (modern mode)")
            return None
        
        try:
            from api.common.database_connection import SnowflakeConnection
            return SnowflakeConnection(sf_env=self.settings.RUN_ENV)
        except Exception as e:
            self.logger.error("Failed to create legacy connection", error=str(e))
            return None