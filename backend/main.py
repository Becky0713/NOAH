from __future__ import annotations

import asyncio
from typing import Optional

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api_router import router
from .config import settings


app = FastAPI(title=settings.app_name)


# Global async HTTP client (created on startup, closed on shutdown)
http_client: Optional[httpx.AsyncClient] = None


@app.on_event("startup")
async def on_startup() -> None:
    global http_client
    http_client = httpx.AsyncClient(timeout=settings.http_timeout_seconds)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global http_client
    if http_client is not None:
        await http_client.aclose()
        http_client = None


# CORS
allow_origins = [origin.strip() for origin in settings.cors_allow_origins.split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(router)


@app.get("/")
async def root() -> dict:
    return {"message": "Welcome to NYC Housing Hub API"}



