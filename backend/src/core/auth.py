import logging
import os
from typing import Dict, Optional
from fastapi import Header, HTTPException, Request, status

import firebase_admin
from firebase_admin import auth as firebase_auth
from firebase_admin import exceptions as firebase_exceptions
import jwt
from src.services.bigquery_service import extract_ldap

logger = logging.getLogger(__name__)

FIREBASE_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "gcc-agent-catalog-dev")
FIREBASE_ISSUER = os.getenv("FIREBASE_ISSUER", f"https://securetoken.google.com/{FIREBASE_PROJECT_ID}")

def ensure_firebase_initialized():
    if not firebase_admin._apps:
        try:
            firebase_admin.initialize_app()
        except Exception as e:
            logger.error("Failed to initialize Firebase Admin SDK: %s", e)

def verify_firebase_id_token(token: str) -> Dict:
    ensure_firebase_initialized()
    try:
        decoded_token = firebase_auth.verify_id_token(token)
        email = decoded_token.get("email") or ""
        name = decoded_token.get("name") or ""
        ldap = extract_ldap(email, fallback=name)
        return {
            "email": email,
            "ldap": ldap,
            "uid": decoded_token.get("uid"),
            "name": name,
        }
    except firebase_exceptions.FirebaseError as e:
        logger.warning("Firebase ID Token verification error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid Firebase ID Token: {e}",
        )

async def get_current_user(
    request: Request,
    authorization: Optional[str] = Header(None, alias="Authorization"),
    x_goog_user: Optional[str] = Header(None, alias="X-Goog-Authenticated-User-Email"),
) -> Dict:
    """Dependency to get the current authenticated user, supporting IAP and Firebase tokens."""
    if x_goog_user:
        ldap = extract_ldap(x_goog_user)
        email = x_goog_user.split(":")[-1] if ":" in x_goog_user else x_goog_user
        return {"email": email, "ldap": ldap, "uid": ldap, "name": ldap}

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    token = authorization.split(" ", 1)[1].strip()
    return verify_firebase_id_token(token)

