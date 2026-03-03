from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from typing import Dict

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer


@dataclass(slots=True)
class User:
    username: str
    role: str


DEV_ADMIN_USERNAME = os.getenv("DEV_ADMIN_USERNAME", "admin")
DEV_ADMIN_PASSWORD = os.getenv("DEV_ADMIN_PASSWORD", "admin")
OIDC_ISSUER_URL = os.getenv("OIDC_ISSUER_URL", "")
OIDC_CLIENT_ID = os.getenv("OIDC_CLIENT_ID", "")
OIDC_ENABLED = bool(OIDC_ISSUER_URL and OIDC_CLIENT_ID)

TOKEN_STORE: Dict[str, User] = {}
security = HTTPBearer(auto_error=False)


def issue_dev_token(username: str, password: str) -> str:
    if username != DEV_ADMIN_USERNAME or password != DEV_ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = secrets.token_urlsafe(24)
    TOKEN_STORE[token] = User(username=username, role="admin")
    return token


def current_user(
    creds: HTTPAuthorizationCredentials | None = Depends(security),
) -> User:
    if creds is None:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    user = TOKEN_STORE.get(creds.credentials)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return user


def require_admin(user: User = Depends(current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")
    return user
