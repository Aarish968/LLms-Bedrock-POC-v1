"""Main FastAPI application module."""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse

from api.core.config import get_settings
from api.core.logging import setup_logging
from api.dependencies.database import get_database_engine
from api.health.healthcheck import router as health_router
from api.v1.routers import lineage

# Setup structured logging
setup_logging()
logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager."""
    settings = get_settings()
    logger.info("Starting Column Lineage API", version=settings.VERSION)
    
    # Initialize database connection (optional)
    try:
        engine = get_database_engine()
        if engine:
            logger.info("Database connection initialized")
        else:
            logger.info("Running without database connection (mock mode)")
    except Exception as e:
        logger.warning("Database connection failed, continuing in mock mode", error=str(e))
    
    yield
    
    logger.info("Shutting down Column Lineage API")


# Create FastAPI application
settings = get_settings()
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="FastAPI backend for database view column lineage analysis",
    openapi_url=f"{settings.API_V1_PREFIX}/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS if settings.CORS_ORIGINS != ["*"] else ["*"],
    allow_credentials=settings.CORS_ORIGINS != ["*"],  # Dynamic based on origins
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=settings.ALLOWED_HOSTS,
)


@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """Log all requests and responses."""
    logger.info(
        "Request started",
        method=request.method,
        url=str(request.url),
        client_ip=request.client.host if request.client else None,
    )
    
    try:
        response = await call_next(request)
        logger.info(
            "Request completed",
            method=request.method,
            url=str(request.url),
            status_code=response.status_code,
        )
        return response
    except Exception as e:
        logger.error(
            "Request failed",
            method=request.method,
            url=str(request.url),
            error=str(e),
        )
        raise


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler."""
    logger.error(
        "Unhandled exception",
        method=request.method,
        url=str(request.url),
        error=str(exc),
        exc_info=True,
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "error": str(exc) if settings.DEBUG else "An unexpected error occurred",
        },
    )


# Include routers
app.include_router(health_router, prefix="/health", tags=["health"])
app.include_router(
    lineage.router,
    prefix=f"{settings.API_V1_PREFIX}/lineage",
    tags=["lineage"],
)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Column Lineage API",
        "version": settings.VERSION,
        "docs": "/docs",
        "health": "/health",
    }