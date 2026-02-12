# -*- coding: utf-8 -*-
"""
Configuration for the parser service.
"""
import os
from dataclasses import dataclass

# Try to load .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass
class Config:
    # Service settings
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8001"))
    
    # Database (парсер и бот работают с одной БД через API парсера)
    # DATABASE_PATH обязательно должен быть указан в .env файле
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "")
    
    # Sessions
    SESSIONS_DIR: str = os.getenv("SESSIONS_DIR", "sessions")
    
    # Default API credentials (optional, can be provided per session)
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "")
    
    # Bot token for notifications
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")


config = Config()
