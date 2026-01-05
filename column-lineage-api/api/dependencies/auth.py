"""Authentication dependencies."""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from pydantic import BaseModel

from api.core.config import get_settings
from api.core.logging import get_logger

logger = get_logger(__name__)
security = HTTPBearer()


class TokenData(BaseModel):
    """Token data model."""
    username: Optional[str] = None
    user_id: Optional[str] = None
    email: Optional[str] = None
    roles: list[str] = []


class User(BaseModel):
    """User model."""
    id: str
    username: str
    email: str
    roles: list[str] = []
    is_active: bool = True


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create JWT access token."""
    settings = get_settings()
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.JWT_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(
        to_encode, 
        settings.JWT_SECRET_KEY, 
        algorithm=settings.JWT_ALGORITHM
    )
    return encoded_jwt


def verify_cognito_token(token: str) -> TokenData:
    """Verify Cognito JWT token."""
    try:
        # For development/testing, we'll use a simpler approach
        # In production, you should implement proper Cognito JWT verification
        
        # Get unverified payload for now (NOT SECURE FOR PRODUCTION)
        payload = jwt.get_unverified_claims(token)
        
        # Extract user data from Cognito token
        username = payload.get("cognito:username") or payload.get("email") or payload.get("username")
        user_id = payload.get("sub") or payload.get("user_id")
        email = payload.get("email")
        roles = payload.get("cognito:groups", []) or payload.get("roles", [])
        
        if not user_id and not username:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload"
            )
        
        return TokenData(
            username=username or "unknown",
            user_id=user_id or "unknown",
            email=email or "unknown@example.com",
            roles=roles
        )
        
    except Exception as e:
        logger.error("JWT verification failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


def verify_token(token: str) -> TokenData:
    """Verify JWT token and extract data."""
    # Use Cognito verification instead of custom JWT
    return verify_cognito_token(token)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> User:
    """Get current authenticated user."""
    token_data = verify_token(credentials.credentials)
    
    # In a real application, you would fetch user from database
    # For now, we'll create a user from token data
    user = User(
        id=token_data.user_id or "unknown",
        username=token_data.username or "unknown",
        email=token_data.email or "unknown@example.com",
        roles=token_data.roles,
    )
    
    return user


async def get_current_active_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """Get current active user."""
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )
    return current_user


def require_roles(required_roles: list[str]):
    """Dependency to require specific roles."""
    def role_checker(current_user: User = Depends(get_current_active_user)):
        if not any(role in current_user.roles for role in required_roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions"
            )
        return current_user
    return role_checker