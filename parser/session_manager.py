# -*- coding: utf-8 -*-
"""
Session Manager for Inviter Service.
Manages Telegram client sessions for inviting users.
"""
import os
import logging
import asyncio
import time
import httpx
from typing import Dict, List, Optional, Any
from pyrogram import Client
from pyrogram.errors import (
    AuthKeyUnregistered, AuthKeyDuplicated, SessionPasswordNeeded,
    PhoneCodeInvalid, FloodWait, UserPrivacyRestricted, UserNotMutualContact,
    UserChannelsTooMuch, ChatAdminRequired, PeerFlood, UserAlreadyParticipant,
    UserNotParticipant, ChatWriteForbidden, ChannelPrivate
)

from .database import Database
from .config import config
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from models import SessionMeta

logger = logging.getLogger(__name__)

# Try to import httpx-socks for SOCKS5 support
try:
    from httpx_socks import AsyncProxyTransport
    SOCKS5_SUPPORT = True
except ImportError:
    SOCKS5_SUPPORT = False
    logger.warning("httpx-socks не установлен. Проверка IP через SOCKS5 прокси может не работать. Установите: pip install httpx-socks")


def parse_proxy_string(proxy_str: str) -> Optional[Dict[str, Any]]:
    """
    Parse proxy string into Pyrogram dict format.
    Format: scheme://user:pass@host:port or scheme://host:port
    """
    if not proxy_str:
        return None
    
    try:
        if "://" not in proxy_str:
            return None
            
        scheme, remainder = proxy_str.split("://", 1)
        
        username = None
        password = None
        
        if "@" in remainder:
            auth, host_port = remainder.split("@", 1)
            if ":" in auth:
                username, password = auth.split(":", 1)
            else:
                username = auth
        else:
            host_port = remainder
            
        if ":" in host_port:
            hostname, port_str = host_port.split(":", 1)
            port = int(port_str)
        else:
            hostname = host_port
            port = 80 if scheme == "http" else 1080
            
        return {
            "scheme": scheme,
            "hostname": hostname,
            "port": port,
            "username": username,
            "password": password
        }
    except Exception as e:
        logger.error(f"Ошибка парсинга строки прокси '{proxy_str}': {e}")
        return None


async def ensure_peer_resolved(client: Client, chat_id: int, username: str = None) -> Optional[Any]:
    """
    Убедиться, что peer разрешён в сессии клиента.
    Если peer не разрешён по ID, попытаться разрешить через username.

    Args:
        client: Pyrogram клиент
        chat_id: ID чата/группы/канала
        username: Username чата (если известен)

    Returns:
        chat: Объект чата или None если не удалось разрешить
    """
    try:
        # 1. Пробуем получить чат по ID
        chat = await client.get_chat(int(chat_id))
        return chat
    except ChannelPrivate as e:
        logger.warning(f"Нет доступа к каналу/супергруппе {chat_id}: {e}")
    except ValueError as e:
        if "Peer id invalid" not in str(e):
            raise
    except Exception as e:
        logger.warning(f"Не удалось получить чат {chat_id}: {e}")

    # 2. Пробуем найти чат в диалогах по ID (работает для приватных групп без username)
    try:
        async for dialog in client.get_dialogs(limit=200):
            if str(dialog.chat.id) == str(chat_id):
                logger.info(f"Peer {chat_id} найден в диалогах")
                return dialog.chat
    except Exception as e2:
        logger.debug(f"Не удалось найти чат через диалоги: {e2}")

    # 3. Если есть username, пробуем разрешить peer через username
    if username:
        try:
            await client.get_chat(username)
            chat = await client.get_chat(int(chat_id))
            logger.info(f"Peer {chat_id} успешно разрешён через username @{username}")
            return chat
        except Exception as e3:
            logger.error(f"Не удалось разрешить peer через username @{username}: {e3}")

    # 4. Последняя попытка: попробовать снова по ID после всех проверок
    try:
        chat = await client.get_chat(int(chat_id))
        return chat
    except Exception:
        logger.error(f"Не удалось разрешить peer для chat_id {chat_id}")
        return None


class SessionManager:
    """Manager for multiple Telegram sessions for inviting."""
    
    def __init__(self, db: Database, session_dir: str = None):
        self.db = db
        self.session_dir = session_dir or config.SESSIONS_DIR
        self.clients: Dict[str, Client] = {}
        self._last_client_error: Dict[str, str] = {}  # alias -> last error when get_client failed
        self.ensure_session_dir()
    
    def ensure_session_dir(self):
        """Ensure session directory exists."""
        os.makedirs(self.session_dir, exist_ok=True)
    
    async def release_client(self, alias: str):
        """
        Release a client. Currently does nothing as clients are kept in memory.
        Can be extended for connection pooling or session management.
        """
        # We could potentially stop the client here if we wanted to save resources,
        # but for now we keep them in self.clients for faster reuse.
        pass
    
    async def import_sessions_from_files(self):
        """Import existing session files from directory."""
        await self.db.import_existing_sessions(self.session_dir)
    
    async def load_clients(self):
        """Load all active sessions from DB and create Pyrogram Clients."""
        sessions = await self.db.get_all_sessions()
        for session in sessions:
            # Use session credentials or fallback to config
            api_id = session.api_id or config.API_ID
            api_hash = session.api_hash or config.API_HASH
            
            if session.is_active and api_id and api_hash:
                alias = os.path.basename(session.alias)
                session_dir_abs = os.path.abspath(self.session_dir)
                session_path = os.path.join(session_dir_abs, alias)
                
                if alias not in self.clients:
                    self.clients[alias] = Client(
                        name=session_path,
                        api_id=api_id,
                        api_hash=api_hash,
                        phone_number=session.phone if session.phone else None
                    )
                    logger.info(f"Загружен клиент для сессии: {alias}")
    
    async def add_account(self, alias: str, api_id: int, api_hash: str, phone: str) -> Dict[str, Any]:
        """Add a new account/session."""
        # Check if session already exists
        existing_session = await self.db.get_session_by_alias(alias)
        if existing_session:
            # Delete existing session and its client if it exists
            await self.db.delete_session(alias)
            if alias in self.clients:
                await self.clients[alias].stop()
                del self.clients[alias]

            # Also delete session file if it exists
            session_file = os.path.join(session_dir_abs, f"{alias}.session")
            if os.path.exists(session_file):
                os.remove(session_file)

        session_dir_abs = os.path.abspath(self.session_dir)
        os.makedirs(session_dir_abs, exist_ok=True)

        session = SessionMeta(
            id=0,
            alias=alias,
            api_id=api_id,
            api_hash=api_hash,
            phone=phone,
            session_path=alias,
            is_active=True
        )
        session_id = await self.db.create_session(session)
        
        # Create client
        session_path = os.path.join(session_dir_abs, alias)
        self.clients[alias] = Client(
            name=session_path,
            api_id=api_id,
            api_hash=api_hash,
            phone_number=phone
        )
        
        return {"success": True, "session_id": session_id, "alias": alias}
    
    async def get_all_sessions(self) -> List[SessionMeta]:
        """Get all sessions from database."""
        return await self.db.get_all_sessions()
    
    async def get_sessions_for_task(self, task: str) -> List[SessionMeta]:
        """Get sessions assigned to a specific task."""
        return await self.db.get_sessions_for_task(task)
    
    async def assign_task(self, alias: str, task: str) -> Dict[str, Any]:
        """Assign a session to a task (inviting)."""
        session = await self.db.get_session_by_alias(alias)
        if not session:
            return {"success": False, "error": "Session not found"}
        
        await self.db.add_session_assignment(session.id, task)
        assignments = await self.get_assignments()
        return {"success": True, "alias": alias, "task": task, "assignments": assignments}
    
    async def remove_assignment(self, alias: str, task: str) -> Dict[str, Any]:
        """Remove task assignment from session."""
        session = await self.db.get_session_by_alias(alias)
        if not session:
            return {"success": False, "error": "Session not found"}
        
        await self.db.remove_session_assignment(session.id, task)
        assignments = await self.get_assignments()
        return {"success": True, "alias": alias, "assignments": assignments}
    
    async def get_assignments(self) -> Dict[str, List[str]]:
        """Get all task assignments."""
        return await self.db.get_assignments()
    
    async def delete_session(self, alias: str) -> Dict[str, Any]:
        """Delete a session."""
        session = await self.db.get_session_by_alias(alias)
        if not session:
            return {"success": False, "error": "Session not found"}
        
        # Stop client if running
        if alias in self.clients:
            client = self.clients[alias]
            if client.is_connected:
                try:
                    await client.stop()
                except Exception as e:
                    logger.warning(f"Ошибка остановки клиента {alias}: {e}")
            del self.clients[alias]
        
        # Delete from DB
        await self.db.delete_session(session.id)
        
        # Try to delete session file
        session_path = os.path.join(self.session_dir, alias + ".session")
        if os.path.exists(session_path):
            try:
                os.remove(session_path)
            except Exception as e:
                logger.warning(f"Не удалось удалить файл сессии: {e}")
        
        return {"success": True, "alias": alias}
    
    async def get_proxy_info(self, alias: str, use_proxy: bool = True) -> Optional[str]:
        """Get proxy information string for logging."""
        session = await self.db.get_session_by_alias(alias)
        if not session:
            return None
        
        if use_proxy and session.proxy:
            # Return masked proxy for security (show only host:port)
            proxy_dict = parse_proxy_string(session.proxy)
            if proxy_dict:
                return f"{proxy_dict.get('scheme', 'unknown')}://{proxy_dict.get('hostname', 'unknown')}:{proxy_dict.get('port', 'unknown')}"
            return session.proxy
        return None
    
    async def get_client(self, alias: str, use_proxy: bool = True) -> Optional[Client]:
        """Get a Pyrogram client by alias, creating or restarting if necessary."""
        logger.debug(f"[SESSION_MANAGER] Получение клиента для сессии: {alias} (use_proxy={use_proxy})")
        
        session = await self.db.get_session_by_alias(alias)
        if not session:
            self._last_client_error[alias] = "Сессия не найдена в базе"
            return None
            
        # Determine target proxy configuration
        target_proxy = None
        if use_proxy and session.proxy:
            target_proxy = parse_proxy_string(session.proxy)
            
        # Check existing client
        if alias in self.clients:
            client = self.clients[alias]
            
            # Check if proxy configuration matches
            # Note: Pyrogram stores proxy in client.proxy as a dict
            current_proxy = getattr(client, "proxy", None)
            
            # Simple equality check might fail if types differ slightly or extra fields
            # strict check: scheme, host, port, user, pass
            
            is_match = False
            if current_proxy is None and target_proxy is None:
                is_match = True
            elif current_proxy and target_proxy:
                is_match = (
                    current_proxy.get('scheme') == target_proxy.get('scheme') and
                    current_proxy.get('hostname') == target_proxy.get('hostname') and
                    current_proxy.get('port') == target_proxy.get('port') and
                    current_proxy.get('username') == target_proxy.get('username') and
                    current_proxy.get('password') == target_proxy.get('password')
                )
            
            if not is_match:
                proxy_info = await self.get_proxy_info(alias, use_proxy)
                proxy_str = f" через прокси {proxy_info}" if proxy_info else " без прокси"
                logger.info(f"Конфигурация прокси изменилась для {alias}. Перезапуск клиента...")
                if client.is_connected:
                    await client.stop()
                del self.clients[alias]
        
        if alias not in self.clients:
            # Use session credentials or fallback to config
            api_id = session.api_id or config.API_ID
            api_hash = session.api_hash or config.API_HASH
            
            if session and api_id and api_hash:
                session_dir_abs = os.path.abspath(self.session_dir)
                session_path = os.path.join(session_dir_abs, alias)
                
                client = Client(
                    name=session_path,
                    api_id=api_id,
                    api_hash=api_hash,
                    workdir=session_dir_abs,
                    phone_number=session.phone if session.phone else None,
                    proxy=target_proxy
                )
                self.clients[alias] = client
                proxy_info = await self.get_proxy_info(alias, use_proxy)
                proxy_str = f" с прокси {proxy_info}" if proxy_info else " без прокси"
                logger.info(f"Создан клиент для сессии: {alias}{proxy_str}")
        
        client = self.clients.get(alias)
        if client and not client.is_connected:
            proxy_info = await self.get_proxy_info(alias, use_proxy)
            proxy_str = f" через прокси {proxy_info}" if proxy_info else " без прокси"
            
            try:
                # Apply timeout for connection - especially important for bad proxies
                # Pyrogram can hang indefinitely if proxy is unreachable
                connection_timeout = 30  # 30 seconds timeout
                
                try:
                    await asyncio.wait_for(client.start(), timeout=connection_timeout)
                    logger.info(f"Запущена сессия: {alias}{proxy_str}")
                except asyncio.TimeoutError:
                    # Connection timeout - likely bad proxy
                    error_msg = f"Таймаут подключения ({connection_timeout}с). Прокси недоступен или неверен."
                    logger.error(f"Таймаут подключения ({connection_timeout}с) для сессии {alias}{proxy_str}. Прокси недоступен или неверен.")
                    if alias in self.clients:
                        try:
                            if self.clients[alias].is_connected:
                                await self.clients[alias].stop()
                        except:
                            pass
                        del self.clients[alias]
                    self._last_client_error[alias] = error_msg
                    return None
                    
            except OSError as e:
                error_msg = f"Ошибка сети: {e}"
                logger.error(f"Ошибка сети при подключении сессии {alias}{proxy_str}: {e}")
                if alias in self.clients:
                    del self.clients[alias]
                self._last_client_error[alias] = error_msg
                return None
            except ConnectionError as e:
                error_msg = f"Прокси отклонил подключение: {e}"
                logger.error(f"Ошибка подключения к прокси для сессии {alias}{proxy_str}: {e}")
                if alias in self.clients:
                    del self.clients[alias]
                self._last_client_error[alias] = error_msg
                return None
            except (AuthKeyUnregistered, AuthKeyDuplicated) as e:
                error_msg = "Сессия отозвана или сброшена в Telegram. Нужно заново войти в аккаунт."
                logger.error(f"Сессия {alias}: {e}")
                if alias in self.clients:
                    del self.clients[alias]
                self._last_client_error[alias] = error_msg
                return None
            except Exception as e:
                error_str = str(e).lower()
                if "proxy" in error_str or "socks" in error_str or "connect" in error_str or "network" in error_str:
                    error_msg = f"Прокси/сеть: {e}"
                elif "auth" in error_str or "session" in error_str or "invalid" in error_str:
                    error_msg = f"Сессия или авторизация: {e}"
                else:
                    error_msg = f"Ошибка запуска: {e}"
                logger.error(f"Не удалось запустить сессию {alias}{proxy_str}: {e}")
                if alias in self.clients:
                    del self.clients[alias]
                self._last_client_error[alias] = error_msg
                return None
        
        self._last_client_error.pop(alias, None)
        return client
    
    async def set_session_proxy(self, alias: str, proxy_str: str) -> Dict[str, Any]:
        """Set proxy for a session."""
        session = await self.db.get_session_by_alias(alias)
        if not session:
            return {"success": False, "error": "Session not found"}
            
        # Validate proxy string format
        if proxy_str and proxy_str.lower() != 'none':
            parsed = parse_proxy_string(proxy_str)
            if not parsed:
                return {"success": False, "error": "Invalid proxy format. Use scheme://user:pass@host:port"}
        
        await self.db.update_session(session.id, proxy=proxy_str if proxy_str and proxy_str.lower() != 'none' else None)
        
        # Determine if we need to restart client
        if alias in self.clients:
            client = self.clients[alias]
            if client.is_connected:
                await client.stop()
            del self.clients[alias]
            
        return {"success": True, "alias": alias, "proxy": proxy_str}

    async def _check_ip_address(self, proxy_str: Optional[str] = None) -> Optional[str]:
        """
        Get IP address through connection (with or without proxy).
        
        Supports HTTP, HTTPS, and SOCKS5 proxies.
        For SOCKS5, requires httpx-socks library.
        """
        try:
            # Use multiple IP check services for reliability
            ip_services = [
                "https://api.ipify.org",
                "https://ifconfig.me/ip",
                "https://icanhazip.com",
                "https://api.myip.com",
                "https://checkip.amazonaws.com"
            ]
            
            transport = None
            proxies = None
            
            if proxy_str:
                # Parse proxy string
                proxy_dict = parse_proxy_string(proxy_str)
                if proxy_dict:
                    scheme = (proxy_dict.get("scheme") or "socks5").lower()
                    hostname = proxy_dict.get("hostname")
                    port = proxy_dict.get("port")
                    username = proxy_dict.get("username")
                    password = proxy_dict.get("password")
                    
                    # Handle SOCKS5/SOCKS4 proxy with httpx-socks
                    if scheme in ("socks5", "socks4"):
                        if not SOCKS5_SUPPORT:
                            logger.error("SOCKS5 прокси требует библиотеку httpx-socks. Установите: pip install httpx-socks")
                            return None
                        
                        # Create SOCKS transport - include credentials in URL to avoid duplicate parameter error
                        if username and password:
                            proxy_url = f"{scheme}://{username}:{password}@{hostname}:{port}"
                        else:
                            proxy_url = f"{scheme}://{hostname}:{port}"
                        
                        transport = AsyncProxyTransport.from_url(proxy_url)
                    else:
                        # Handle HTTP/HTTPS proxy (native httpx: один параметр proxy, не proxies)
                        if username and password:
                            proxy_url = f"{scheme}://{username}:{password}@{hostname}:{port}"
                        else:
                            proxy_url = f"{scheme}://{hostname}:{port}"
                        
                        proxies = proxy_url
            
            # Try each IP service
            for service_url in ip_services:
                try:
                    # When using transport (SOCKS5), don't use proxies parameter
                    client_kwargs = {
                        "timeout": 15.0,
                        "follow_redirects": True
                    }
                    
                    if transport:
                        # SOCKS5 proxy - use transport only
                        client_kwargs["transport"] = transport
                        logger.debug(f"Использование SOCKS транспорта для {service_url}")
                    elif proxies:
                        # HTTP/HTTPS proxy - httpx принимает proxy=url (не proxies)
                        client_kwargs["proxy"] = proxies
                        logger.debug(f"Использование HTTP прокси для {service_url}")
                    
                    async with httpx.AsyncClient(**client_kwargs) as http_client:
                        response = await http_client.get(service_url)
                        if response.status_code == 200:
                            ip = response.text.strip()
                            if ip and self._is_valid_ip(ip):
                                logger.info(f"Successfully got IP address: {ip} from {service_url} (proxy: {proxy_str})")
                                return ip
                        else:
                            logger.debug(f"Сервис {service_url} вернул статус {response.status_code}")
                except Exception as e:
                    logger.warning(f"Не удалось получить IP с {service_url} через прокси {proxy_str}: {type(e).__name__}: {e}")
                    continue
            
            logger.warning(f"Не удалось получить IP адрес ни с одного сервиса. Прокси: {proxy_str}")
            return None
        except Exception as e:
            logger.error(f"Ошибка проверки IP адреса с прокси {proxy_str}: {e}", exc_info=True)
            return None

    def _is_valid_ip(self, ip: str) -> bool:
        """Validate IP address format (IPv4 or IPv6)."""
        if not ip or len(ip.strip()) == 0:
            return False
        
        ip = ip.strip()
        
        # Check IPv4 format
        try:
            parts = ip.split('.')
            if len(parts) == 4:
                for part in parts:
                    num = int(part)
                    if num < 0 or num > 255:
                        return False
                return True
        except ValueError:
            pass
        
        # Check IPv6 format (simplified check - contains colons)
        if ':' in ip:
            # Basic IPv6 validation
            parts = ip.split(':')
            if 2 <= len(parts) <= 8:
                return True
        
        return False

    async def _measure_latency(self, client: Client) -> Optional[float]:
        """Measure latency by calling get_me() and timing it."""
        try:
            start_time = time.time()
            await client.get_me()
            latency = (time.time() - start_time) * 1000  # Convert to milliseconds
            return latency
        except Exception as e:
            logger.error(f"Ошибка измерения задержки: {e}")
            return None

    async def test_proxy_connection(self, alias: str, use_proxy: bool = True) -> Dict[str, Any]:
        """Test proxy connection for a session with IP check and latency measurement."""
        session = await self.db.get_session_by_alias(alias)
        if not session:
            return {"success": False, "error": "Session not found"}

        if use_proxy and not session.proxy:
            return {"success": False, "error": "No proxy configured for this session"}

        try:
            # Try to create and start client with proxy settings
            client = await self.get_client(alias, use_proxy=use_proxy)
            if not client:
                return {"success": False, "error": "Failed to create client"}

            # Test basic connectivity by getting user info and measure latency
            start_time = time.time()
            me = await client.get_me()
            latency_ms = (time.time() - start_time) * 1000
            
            if not me:
                return {"success": False, "error": "Failed to get user info"}

            # Get IP address through this connection
            proxy_str = session.proxy if use_proxy else None
            ip_address = await self._check_ip_address(proxy_str)
            
            result = {
                "success": True,
                "message": f"Connection successful {'with proxy' if use_proxy and session.proxy else 'without proxy'}",
                "user_id": me.id,
                "username": me.username,
                "latency_ms": round(latency_ms, 2),
                "ip_address": ip_address
            }

            # Note: Comparison is done in bot layer by calling this function twice
            # (once with use_proxy=True, once with use_proxy=False)
            # No need to create additional client here to avoid unnecessary switching

            return result

        except Exception as e:
            return {"success": False, "error": f"Connection failed: {str(e)}"}


    async def remove_session_proxy(self, alias: str) -> Dict[str, Any]:
        """Remove proxy from a session."""
        return await self.set_session_proxy(alias, None)


    async def copy_proxy_to_session(self, from_alias: str, to_alias: str) -> Dict[str, Any]:
        """Copy proxy configuration from one session to another."""
        from_session = await self.db.get_session_by_alias(from_alias)
        to_session = await self.db.get_session_by_alias(to_alias)

        if not from_session:
            return {"success": False, "error": f"Source session '{from_alias}' not found"}
        if not to_session:
            return {"success": False, "error": f"Target session '{to_alias}' not found"}

        if not from_session.proxy:
            return {"success": False, "error": f"Source session '{from_alias}' has no proxy configured"}

        # Copy proxy to target session
        result = await self.set_session_proxy(to_alias, from_session.proxy)
        if result["success"]:
            result["message"] = f"Proxy copied from '{from_alias}' to '{to_alias}'"

        return result


    async def send_code(self, alias: str, phone: str) -> Dict[str, Any]:
        """Send authentication code to phone."""
        client = self.clients.get(alias)
        if not client:
            logger.error(f"Клиент не найден для сессии: {alias}")
            return {"success": False, "error": "Session not found"}
        
        try:
            await client.connect()
            sent_code = await client.send_code(phone)
            return {
                "success": True,
                "phone_code_hash": sent_code.phone_code_hash
            }
        except Exception as e:
            logger.error(f"Ошибка отправки кода для {alias}: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if client.is_connected:
                await client.disconnect()
    
    async def sign_in(self, alias: str, phone: str, code: str, phone_code_hash: str) -> Dict[str, Any]:
        """Sign in with received code."""
        client = self.clients.get(alias)
        if not client:
            return {"success": False, "error": "Session not found"}
        
        try:
            await client.connect()
            await client.sign_in(
                phone_number=phone,
                phone_code=code,
                phone_code_hash=phone_code_hash
            )
            
            is_authorized = await client.is_user_authorized()
            if is_authorized:
                me = await client.get_me()
                # Update user_id in DB
                session = await self.db.get_session_by_alias(alias)
                if session:
                    await self.db.update_session(session.id, user_id=me.id)
                
                return {
                    "success": True,
                    "user_id": me.id,
                    "first_name": me.first_name,
                    "username": me.username
                }
            else:
                return {"success": False, "error": "Failed to authorize"}
        except SessionPasswordNeeded:
            return {"success": False, "error": "2FA required", "needs_password": True}
        except PhoneCodeInvalid:
            return {"success": False, "error": "Invalid code"}
        except Exception as e:
            logger.error(f"Ошибка входа: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if client.is_connected:
                await client.disconnect()
    
    async def sign_in_with_password(self, alias: str, password: str) -> Dict[str, Any]:
        """Sign in with 2FA password."""
        client = self.clients.get(alias)
        if not client:
            return {"success": False, "error": "Session not found"}
        
        try:
            await client.connect()
            await client.check_password(password)
            
            is_authorized = await client.is_user_authorized()
            if is_authorized:
                me = await client.get_me()
                return {
                    "success": True,
                    "user_id": me.id,
                    "first_name": me.first_name,
                    "username": me.username
                }
            return {"success": False, "error": "Failed to authorize"}
        except Exception as e:
            logger.error(f"Ошибка входа с паролем: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if client.is_connected:
                await client.disconnect()
    
    async def check_group_access(self, alias: str, group_id: int, use_proxy: bool = True) -> Dict[str, Any]:
        """Check if session has access to a group."""
        client = await self.get_client(alias, use_proxy=use_proxy)
        if not client:
            return {"success": False, "error": "Session not available"}
        
        try:
            chat = await client.get_chat(group_id)
            return {
                "success": True,
                "has_access": True,
                "title": chat.title,
                "username": getattr(chat, 'username', None),
                "members_count": getattr(chat, 'members_count', None)
            }
        except Exception as e:
            logger.warning(f"Session {alias} has no access to group {group_id}: {e}")
            return {"success": True, "has_access": False, "error": str(e)}
    
    async def get_group_members(self, alias: str, group_id: int, limit: int = 200, offset: int = 0, username: str = None, use_proxy: bool = True) -> List[Dict]:
        """Get members from a group with offset support."""
        client = await self.get_client(alias, use_proxy=use_proxy)
        if not client:
            return []

        # Убеждаемся, что peer разрешён перед получением участников
        chat = await ensure_peer_resolved(client, group_id, username)
        if not chat:
            logger.error(f"Не удалось разрешить peer для группы {group_id} (username: {username}) в сессии {alias}")
            return None

        # Handle None for limit/offset (e.g. from enhanced_invite_methods with limit=None = fetch all)
        _limit = limit if limit is not None else 10000
        _offset = offset if offset is not None else 0

        members = []
        try:
            # Try to get total members count for logging
            chat_info = await client.get_chat(group_id)
            total_count = getattr(chat_info, 'members_count', 'unknown')
            logger.info(f"Getting members for {group_id}. Total members in chat: {total_count}")

            # Note: client.get_chat_members does not support integer offset natively for pagination
            # We iterate and skip manually.
            current_idx = 0
            async for member in client.get_chat_members(group_id):
                if current_idx < _offset:
                    current_idx += 1
                    continue

                if len(members) >= _limit:
                    break

                if member.user and not member.user.is_bot:
                    members.append({
                        'id': member.user.id,
                        'username': member.user.username,
                        'first_name': member.user.first_name,
                        'last_name': member.user.last_name,
                        # Add status/is_admin if needed for filtering
                        'status': str(member.user.status) if member.user.status else None,
                        'is_admin': member.status in ['administrator', 'creator']
                    })
                
                current_idx += 1
            
            return members

        except Exception as e:
            logger.error(f"Error getting members from {group_id}: {e}")
            return None
    
    async def invite_user(self, alias: str, target_group_id: int, user_id, target_username: str = None, use_proxy: bool = True) -> Dict[str, Any]:
        """
        Invite a single user to a group.
        
        Args:
            alias: Session alias to use
            target_group_id: Target group ID
            user_id: User ID (int) or username (str with '@' prefix) to invite
            target_username: Target group username (optional, for resolving)
            use_proxy: Whether to use proxy
        """
        client = await self.get_client(alias, use_proxy=use_proxy)
        if not client:
            return {"success": False, "error": "Session not available"}

        # Логируем информацию о прокси
        proxy_info = await self.get_proxy_info(alias, use_proxy)
        proxy_str = f" через прокси {proxy_info}" if proxy_info else " без прокси"
        
        # Determine if we're inviting by ID or username
        if isinstance(user_id, str):
            user_target = user_id if user_id.startswith('@') else f"@{user_id}"
            logger.info(f"Приглашение пользователя {user_target} (по username) в группу {target_group_id} (сессия: {alias}{proxy_str})")
        else:
            user_target = user_id
            logger.info(f"Приглашение пользователя {user_id} в группу {target_group_id} (сессия: {alias}{proxy_str})")

        # Убеждаемся, что peer разрешён перед приглашением
        chat = await ensure_peer_resolved(client, target_group_id, target_username)
        if not chat:
            # Не удалось разрешить peer - сессия не имеет доступа к целевой группе
            # Это критическая ошибка, которая требует ротации на другую сессию
            logger.error(f"Не удалось разрешить peer для целевой группы {target_group_id} (сессия: {alias}{proxy_str}). Сессия не имеет доступа к группе.")
            return {"success": False, "error": f"Cannot resolve target group {target_group_id} (username: {target_username}) - session has no access", "fatal": True}

        try:
            await client.add_chat_members(target_group_id, user_target)
            logger.debug(f"Успешно приглашен пользователь {user_target} в группу {target_group_id} (сессия: {alias}{proxy_str})")
            return {"success": True, "user_id": user_id}
        except UserAlreadyParticipant:
            return {"success": True, "user_id": user_id, "already_member": True}
        except FloodWait as e:
            logger.warning(f"FloodWait {e.value} секунд при приглашении пользователя {user_target} (сессия: {alias}{proxy_str})")
            return {"success": False, "error": f"FloodWait: {e.value} seconds", "flood_wait": e.value}
        except UserPrivacyRestricted:
            return {"success": False, "error": "User privacy restricted", "skip": True}
        except UserNotMutualContact:
            return {"success": False, "error": "User not mutual contact", "skip": True}
        except UserChannelsTooMuch:
            return {"success": False, "error": "User in too many channels", "skip": True}
        except (ChatAdminRequired, ChatWriteForbidden):
            logger.error(f"Требуются права администратора для приглашения в группу {target_group_id} (сессия: {alias}{proxy_str})")
            return {"success": False, "error": "Admin rights required (cannot add members)", "fatal": True}
        except PeerFlood:
            logger.error(f"Peer flood - сессия временно заблокирована (сессия: {alias}{proxy_str})")
            return {"success": False, "error": "Peer flood - session temporarily blocked", "fatal": True}
        except Exception as e:
            error_str = str(e)
            # Handle case when username cannot be resolved
            if "USERNAME_NOT_OCCUPIED" in error_str or "USERNAME_INVALID" in error_str:
                logger.warning(f"Username {user_target} не найден или недействителен (сессия: {alias}{proxy_str})")
                return {"success": False, "error": "Username not found or invalid", "skip": True}
            logger.error(f"Ошибка при приглашении пользователя {user_target} (сессия: {alias}{proxy_str}): {e}")
            return {"success": False, "error": str(e)}
    
    async def start_all(self) -> Dict[str, str]:
        """Start all client sessions."""
        results = {}
        for alias, client in self.clients.items():
            try:
                if not client.is_connected:
                    # Добавляем обработчик для подавления ошибок peer_id в обновлениях
                    original_handle_updates = client.handle_updates

                    async def safe_handle_updates(updates):
                        """Безопасная обработка обновлений с подавлением ошибок peer_id."""
                        try:
                            await original_handle_updates(updates)
                        except ValueError as e:
                            if "Peer id invalid" in str(e):
                                # Подавляем ошибку peer_id invalid - это нормально для сессий,
                                # которые не имеют доступа ко всем чатам
                                logger.debug(f"Suppressed peer_id error for session {alias}: {e}")
                            else:
                                # Для других ValueError выбрасываем дальше
                                raise
                        except Exception as e:
                            # Для других исключений логируем и подавляем
                            logger.debug(f"Suppressed update handling error for session {alias}: {e}")

                    # Заменяем обработчик обновлений на безопасный
                    client.handle_updates = safe_handle_updates

                    await client.start()
                    results[alias] = "success"
                else:
                    results[alias] = "already_running"
            except Exception as e:
                logger.error(f"Error starting session {alias}: {e}")
                results[alias] = f"error: {str(e)}"
        return results
    
    async def stop_all(self) -> Dict[str, str]:
        """Stop all client sessions."""
        results = {}
        for alias, client in self.clients.items():
            try:
                if client.is_connected:
                    await client.stop()
                    results[alias] = "success"
                else:
                    results[alias] = "already_stopped"
            except Exception as e:
                logger.error(f"Error stopping session {alias}: {e}")
                results[alias] = f"error: {str(e)}"
        return results

    async def join_chat_if_needed(self, client: Client, chat_id: int, username: str = None) -> (bool, Optional[str]):
        """
        Пытается вступить в чат, если сессия еще не там.
        Returns: (success, error_message)
        """
        try:
            # Проверяем статус участника
            try:
                await client.get_chat_member(chat_id, "me")
                return True, None # Уже в чате
            except UserNotParticipant:
                # Если не участник, пробуем вступить
                chat_input = username if username else chat_id
                logger.info(f"Сессия не в чате {chat_input}, пытаемся вступить...")
                await client.join_chat(chat_input)
                logger.info(f"Сессия успешно вступила в чат {chat_input}")
                return True, None
            except Exception as e:
                error_str = str(e)
                if "ChatAdminRequired" in error_str or "ChannelPrivate" in error_str:
                    pass
                
                # Пробуем вступить в любом случае если получили ошибку
                chat_input = username if username else chat_id
                try:
                    await client.join_chat(chat_input)
                    return True, None
                except Exception as e2:
                    error_msg = f"Не удалось вступить в чат {chat_input}: {e2}"
                    logger.error(error_msg)
                    return False, str(e2)
        except Exception as e:
            error_msg = f"Ошибка при проверке/вступлении в чат {chat_id}: {e}"
            logger.error(error_msg)
            return False, str(e)

    async def validate_session_capability(self, alias: str, source_group_id: int, target_group_id: int, 
                                        source_username: str = None, target_username: str = None, use_proxy: bool = True,
                                        invite_mode: str = 'member_list') -> Dict[str, Any]:
        """
        Validate if a session can perform inviting from source to target.
        Returns detailed error reason if validation fails.
        """
        client = await self.get_client(alias, use_proxy=use_proxy)
        if not client:
            return {"success": False, "reason": "Session not available or invalid"}

        # Skip source validation for file-based invites
        if invite_mode != 'from_file':
            # Check Source Access
            source_chat = await ensure_peer_resolved(client, source_group_id, source_username)
            if not source_chat:
                return {"success": False, "reason": f"No access to source group {source_group_id} (username: {source_username}): peer not resolved"}

            # Пытаемся вступить в источник, чтобы иметь право парсить участников
            joined, error = await self.join_chat_if_needed(client, source_group_id, source_username)
            if not joined:
                return {"success": False, "reason": f"Не удалось вступить в группу-источник: {error}"}

            # ПРОВЕРКА ВИДИМОСТИ УЧАСТНИКОВ
            # Если в группе есть люди, сессия должна их видеть
            source_chat_info = await client.get_chat(source_group_id)
            total_members = getattr(source_chat_info, 'members_count', 0)
            
            if total_members > 5: # Если группа не совсем пустая
                test_members = await self.get_group_members(alias, source_group_id, limit=10)
                if not test_members or len(test_members) < 2:
                    # Если видим слишком мало при большом общем количестве - значит обзор ограничен
                    logger.warning(f"⚠️ Сессия {alias} имеет ограниченный обзор группы {source_group_id}. Видит {len(test_members) if test_members else 0} из {total_members}")
                    return {"success": False, "reason": "Ограниченный обзор участников (возможно, список скрыт для не-админов)"}

        # Check Target Access
        target_chat = await ensure_peer_resolved(client, target_group_id, target_username)
        if not target_chat:
            return {"success": False, "reason": f"No access to target group {target_group_id} (username: {target_username}): peer not resolved"}

        # Пытаемся вступить в цель, чтобы иметь право инвайтить
        joined, error = await self.join_chat_if_needed(client, target_group_id, target_username)
        if not joined:
            return {"success": False, "reason": f"Не удалось вступить в целевую группу: {error}"}

        # Additional check for admin rights if possible (basic check)
        # This is not perfect as get_chat might not return member privileges for everyone,
        # but getting the chat object at least confirms visibility.
        # Real permission check happens during invite action.
        if getattr(target_chat, 'permissions', None):
            # If we can see permissions, check if invite is allowed (for members)
            pass

        return {"success": True}

    async def get_next_inviting_session(self, current_alias: str = None, 
                                        source_group_id: int = None, 
                                        target_group_id: int = None) -> Optional[str]:
        """
        Get the next available session for inviting.
        Checks if the session has access to both source and target groups.
        """
        sessions = await self.get_sessions_for_task("inviting")
        
        if not sessions:
            logger.warning("No sessions assigned for inviting")
            return None
        
        # Find current index
        current_index = -1
        if current_alias:
            for i, session in enumerate(sessions):
                if session.alias == current_alias:
                    current_index = i
                    break
        
        # Try sessions starting from next one
        for i in range(len(sessions)):
            next_index = (current_index + 1 + i) % len(sessions)
            session = sessions[next_index]
            
            # Skip current session
            if session.alias == current_alias and len(sessions) > 1:
                continue
            
            # Check access to both groups if specified
            if source_group_id and target_group_id:
                source_access = await self.check_group_access(session.alias, source_group_id)
                if not source_access.get('has_access'):
                    logger.info(f"Session {session.alias} has no access to source group {source_group_id}")
                    continue
                
                target_access = await self.check_group_access(session.alias, target_group_id)
                if not target_access.get('has_access'):
                    logger.info(f"Session {session.alias} has no access to target group {target_group_id}")
                    continue
            
            logger.info(f"Selected session for inviting: {session.alias}")
            return session.alias
        
        logger.warning("No suitable session found for inviting")
        return None

    async def validate_sessions_for_task(self, task_type: str, task: Any) -> Dict[str, Any]:
        """
        Validate sessions for a task (pre-start check).
        Checks if sessions have access to source/target chats.
        
        Args:
            task_type: 'invite', 'parse', 'post_parse', 'post_monitoring'
            task: Task object with fields: available_sessions, source_id/group_id, etc.
            
        Returns:
            Dict with 'valid': List[str], 'invalid': Dict[str, str] (alias -> error)
        """
        valid = []
        invalid = {}
        
        # Determine sessions to check
        sessions_to_check = task.available_sessions if task.available_sessions else []
        if not sessions_to_check:
             # Try to load if simple string
             if isinstance(task.available_sessions, str):
                 sessions_to_check = task.available_sessions.split(',')
             else:
                 return {"valid": [], "invalid": {"global": "No sessions assigned"}}

        for alias in sessions_to_check:
            if not alias: continue
            
            try:
                # Use proxy from task if available, or default
                use_proxy = getattr(task, 'use_proxy', True)
                client = await self.get_client(alias, use_proxy=use_proxy)
                
                if not client:
                    invalid[alias] = self._last_client_error.get(
                        alias,
                        "Сессия не подключена (не удалось инициализировать)"
                    )
                    continue
                
                # Check connection
                if not client.is_connected:
                    try:
                        # Start with quick timeout just to check
                        # But get_client usually starts it. 
                        # If get_client returned valid object but not connected, try start
                        await client.start()
                    except Exception as e:
                        invalid[alias] = f"Connection failed: {e}"
                        if client.is_connected: # cleanup if partially started
                             try: await client.stop() 
                             except: pass
                        continue
                
                # Validation based on task type
                if task_type == 'invite':
                    # Source check (only if not file mode)
                    if getattr(task, 'invite_mode', 'member_list') != 'from_file':
                         src_id = task.source_group_id
                         src_user = task.source_username
                         src = await ensure_peer_resolved(client, src_id, src_user)
                         if not src:
                             invalid[alias] = f"No access to source group {src_id}"
                             continue
                         joined, error = await self.join_chat_if_needed(client, src_id, src_user)
                         if not joined:
                             invalid[alias] = f"Не удалось вступить в группу-источник: {error}"
                             continue
                    
                    # Target check
                    dst_id = task.target_group_id
                    dst_user = task.target_username
                    dst = await ensure_peer_resolved(client, dst_id, dst_user)
                    if not dst:
                        invalid[alias] = f"No access to target group {dst_id}"
                        continue
                    joined, error = await self.join_chat_if_needed(client, dst_id, dst_user)
                    if not joined:
                        invalid[alias] = f"Не удалось вступить в целевую группу: {error}"
                        continue
                    
                elif task_type == 'parse':
                    # Source check
                    src_id = task.source_group_id
                    src_user = task.source_username
                    src = await ensure_peer_resolved(client, src_id, src_user)
                    if not src:
                        invalid[alias] = f"No access to source group {src_id}"
                        continue
                    joined, error = await self.join_chat_if_needed(client, src_id, src_user)
                    if not joined:
                        invalid[alias] = f"Не удалось вступить в группу-источник: {error}"
                        continue
                    
                    # Check member list visibility for member_list mode
                    if getattr(task, 'parse_mode', 'member_list') == 'member_list':
                        try:
                             # Try to get 1 member to check privileges
                             # Iterating over async generator
                             has_members = False
                             async for _ in client.get_chat_members(src_id, limit=1):
                                 has_members = True
                                 break
                             # If we got here, we have access to members
                        except Exception as e:
                            invalid[alias] = f"Cannot fetch members: {e}"
                            continue

                elif task_type in ['post_parse', 'post_monitoring']:
                    # Source check
                    src_id = task.source_id
                    src_user = task.source_username
                    src = await ensure_peer_resolved(client, src_id, src_user)
                    if not src:
                        invalid[alias] = f"No access to source {src_id}"
                        continue
                    joined, error = await self.join_chat_if_needed(client, src_id, src_user)
                    if not joined:
                        invalid[alias] = f"Не удалось вступить в источник: {error}"
                        continue
                    
                    # Target check
                    dst_id = task.target_id
                    dst_user = task.target_username
                    dst = await ensure_peer_resolved(client, dst_id, dst_user)
                    if not dst:
                        invalid[alias] = f"No access to target {dst_id}"
                        continue
                    joined, error = await self.join_chat_if_needed(client, dst_id, dst_user)
                    if not joined:
                        invalid[alias] = f"Не удалось вступить в цель: {error}"
                        continue

                valid.append(alias)

            except Exception as e:
                invalid[alias] = str(e)
        
        return {"valid": valid, "invalid": invalid}
