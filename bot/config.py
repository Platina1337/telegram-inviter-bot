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
    
    # Bot proxy (optional)
    BOT_PROXY: str = os.getenv("BOT_PROXY", "")
    
    # Use IPv6 (can help avoid blocks if ISP supports it)
    USE_IPV6: bool = os.getenv("USE_IPV6", "False").lower() == "true"
    
    def get_bot_proxy_dict(self) -> dict:
        """Parse BOT_PROXY URL into a dictionary for Pyrogram Client."""
        if not self.BOT_PROXY:
            return None
            
        try:
            from shared.validation import validate_proxy_string
            is_valid, clean_proxy, error_msg = validate_proxy_string(self.BOT_PROXY)
            if not is_valid:
                return None
                
            # Basic parsing of scheme://user:pass@host:port
            scheme_split = clean_proxy.split("://")
            scheme = scheme_split[0].lower()
            rest = scheme_split[1]
            
            # Normalize scheme for Pyrogram (it likes socks5, socks4, http)
            if scheme.startswith("socks5"):
                scheme = "socks5"
            elif scheme.startswith("socks4"):
                scheme = "socks4"
            elif scheme not in ["http", "https"]:
                # Default to socks5 if unknown scheme
                scheme = "socks5"
                
            auth_split = rest.split("@")
            if len(auth_split) > 1:
                auth = auth_split[0]
                host_port = auth_split[1]
                user_pass = auth.split(":")
                username = user_pass[0]
                password = user_pass[1] if len(user_pass) > 1 else ""
            else:
                host_port = auth_split[0]
                username = None
                password = None
                
            hp_split = host_port.split(":")
            hostname = hp_split[0]
            port = int(hp_split[1]) if len(hp_split) > 1 else 1080
            
            return {
                "scheme": scheme,
                "hostname": hostname,
                "port": port,
                "username": username,
                "password": password
            }
        except Exception as e:
            # Import logger here to avoid circular imports
            import logging
            logging.getLogger(__name__).warning(f"Error parsing BOT_PROXY: {e}")
            return None


config = Config()

# Export ADMIN_IDS for handlers
ADMIN_IDS = config.ADMIN_IDS if config.ADMIN_IDS else []
