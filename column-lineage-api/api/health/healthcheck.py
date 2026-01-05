"""Health check endpoints."""

from datetime import datetime
from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from api.core.config import get_settings
from api.core.logging import get_logger
from api.dependencies.database import DatabaseManager

logger = get_logger(__name__)
router = APIRouter()


class HealthStatus(BaseModel):
    """Health status response model."""
    status: str
    timestamp: datetime
    version: str
    environment: str


class DetailedHealthStatus(BaseModel):
    """Detailed health status response model."""
    status: str
    timestamp: datetime
    version: str
    environment: str
    services: Dict[str, Any]


@router.get("/", response_model=HealthStatus)
async def health_check():
    """Basic health check endpoint."""
    settings = get_settings()
    
    return HealthStatus(
        status="healthy",
        timestamp=datetime.utcnow(),
        version=settings.VERSION,
        environment=settings.RUN_ENV,
    )


@router.get("/detailed", response_model=DetailedHealthStatus)
async def detailed_health_check():
    """Detailed health check with service status."""
    settings = get_settings()
    services = {}
    overall_status = "healthy"
    
    # Check database connection
    try:
        db_manager = DatabaseManager()
        if db_manager.mock_mode:
            services["database"] = {
                "status": "mock",
                "type": "snowflake",
                "details": "Running in mock mode - no database connection configured"
            }
        else:
            db_healthy = db_manager.test_connection()
            services["database"] = {
                "status": "healthy" if db_healthy else "unhealthy",
                "type": "snowflake",
                "details": "Connection test successful" if db_healthy else "Connection failed"
            }
            if not db_healthy:
                overall_status = "degraded"
    except Exception as e:
        logger.error("Database health check failed", error=str(e))
        services["database"] = {
            "status": "unhealthy",
            "type": "snowflake",
            "details": f"Error: {str(e)}"
        }
        overall_status = "unhealthy"
    
    # Check Redis connection (if configured)
    try:
        if settings.REDIS_URL and settings.REDIS_URL != "redis://localhost:6379/0":
            import redis
            redis_client = redis.from_url(settings.REDIS_URL)
            redis_client.ping()
            services["redis"] = {
                "status": "healthy",
                "type": "redis",
                "details": "Connection successful"
            }
        else:
            services["redis"] = {
                "status": "mock",
                "type": "redis",
                "details": "Redis not configured - using default mock settings"
            }
    except Exception as e:
        logger.warning("Redis health check failed", error=str(e))
        services["redis"] = {
            "status": "unhealthy",
            "type": "redis",
            "details": f"Error: {str(e)}"
        }
        if overall_status == "healthy":
            overall_status = "degraded"
    
    # Add system information
    services["system"] = {
        "status": "healthy",
        "type": "system",
        "details": {
            "python_version": "3.10+",
            "fastapi_version": "0.116.2+",
            "mock_mode": not bool(settings.SNOWFLAKE_ACCOUNT),
        }
    }
    
    return DetailedHealthStatus(
        status=overall_status,
        timestamp=datetime.utcnow(),
        version=settings.VERSION,
        environment=settings.RUN_ENV,
        services=services,
    )


@router.get("/ready")
async def readiness_check():
    """Kubernetes readiness probe endpoint."""
    try:
        db_manager = DatabaseManager()
        if db_manager.mock_mode:
            return {"status": "ready", "mode": "mock"}
        
        if not db_manager.test_connection():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection failed"
            )
        
        return {"status": "ready", "mode": "database"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Readiness check failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not ready"
        )


@router.get("/live")
async def liveness_check():
    """Kubernetes liveness probe endpoint."""
    return {"status": "alive"}