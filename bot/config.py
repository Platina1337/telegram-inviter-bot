# -*- coding: utf-8 -*-
"""
Configuration for the bot service.
"""
import os
from dataclasses import dataclass, field
from typing import List

# Try to load .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass
class Config:
    # Bot token
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    
    # API settings for parser service
    PARSER_SERVICE_URL: str = os.getenv("PARSER_SERVICE_URL", "http://localhost:8001")
    
    # Admin user IDs
    ADMIN_IDS: List[int] = field(default_factory=lambda: [
        int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()
    ])
    
    # Default API credentials
    # Если в .env ничего нет, используем публичные ключи от Telegram Desktop
    # Это позволяет запустить бота без лишних настроек
    _env_api_id = os.getenv("API_ID")
    _env_api_hash = os.getenv("API_HASH")
    
    API_ID: int = int(_env_api_id) if _env_api_id and _env_api_id.strip() != "0" else 2040
    API_HASH: str = _env_api_hash if _env_api_hash and _env_api_hash.strip() else "b18441a1ff607e10a989891a5462e627"


config = Config()

# Export ADMIN_IDS for handlers
ADMIN_IDS = config.ADMIN_IDS if config.ADMIN_IDS else []
