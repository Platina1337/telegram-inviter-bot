# -*- coding: utf-8 -*-
"""
Session Manager for Inviter Service.
Manages Telegram client sessions for inviting users.
"""
import os
import logging
import asyncio
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
        self.ensure_session_dir()
    
    def ensure_session_dir(self):
        """Ensure session directory exists."""
        os.makedirs(self.session_dir, exist_ok=True)
    
    async def import_sessions_from_files(self):
        """Import existing session files from directory."""
        await self.db.import_existing_sessions(self.session_dir)
    
    async def load_clients(self):
        """Load all active sessions from DB and create Pyrogram Clients."""
        sessions = await self.db.get_all_sessions()
        for session in sessions:
            if session.is_active and session.api_id and session.api_hash:
                alias = os.path.basename(session.alias)
                session_dir_abs = os.path.abspath(self.session_dir)
                session_path = os.path.join(session_dir_abs, alias)
                
                if alias not in self.clients:
                    self.clients[alias] = Client(
                        name=session_path,
                        api_id=session.api_id,
                        api_hash=session.api_hash,
                        phone_number=session.phone if session.phone else None
                    )
                    logger.info(f"Loaded client for session: {alias}")
    
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
                    logger.warning(f"Error stopping client {alias}: {e}")
            del self.clients[alias]
        
        # Delete from DB
        await self.db.delete_session(session.id)
        
        # Try to delete session file
        session_path = os.path.join(self.session_dir, alias + ".session")
        if os.path.exists(session_path):
            try:
                os.remove(session_path)
            except Exception as e:
                logger.warning(f"Could not delete session file: {e}")
        
        return {"success": True, "alias": alias}
    
    async def get_client(self, alias: str) -> Optional[Client]:
        """Get a Pyrogram client by alias, creating if necessary."""
        logger.debug(f"[SESSION_MANAGER] Getting client for alias: {alias}")
        
        if alias not in self.clients:
            await self.load_clients()
        
        client = self.clients.get(alias)
        
        if not client:
            # Try to create from DB
            session = await self.db.get_session_by_alias(alias)
            if session and session.api_id and session.api_hash:
                session_dir_abs = os.path.abspath(self.session_dir)
                session_path = os.path.join(session_dir_abs, alias)
                
                client = Client(
                    name=session_path,
                    api_id=session.api_id,
                    api_hash=session.api_hash,
                    workdir=session_dir_abs,
                    phone_number=session.phone if session.phone else None
                )
                self.clients[alias] = client
                logger.info(f"Created client for session: {alias}")
        
        if client and not client.is_connected:
            try:
                await client.start()
                logger.info(f"Started session: {alias}")
            except Exception as e:
                logger.error(f"Failed to start session {alias}: {e}")
                return None
        
        return client
    
    async def send_code(self, alias: str, phone: str) -> Dict[str, Any]:
        """Send authentication code to phone."""
        client = self.clients.get(alias)
        if not client:
            logger.error(f"No client found for alias: {alias}")
            return {"success": False, "error": "Session not found"}
        
        try:
            await client.connect()
            sent_code = await client.send_code(phone)
            return {
                "success": True,
                "phone_code_hash": sent_code.phone_code_hash
            }
        except Exception as e:
            logger.error(f"Error sending code for {alias}: {e}")
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
            logger.error(f"Error signing in: {e}")
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
            logger.error(f"Error signing in with password: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if client.is_connected:
                await client.disconnect()
    
    async def check_group_access(self, alias: str, group_id: int) -> Dict[str, Any]:
        """Check if session has access to a group."""
        client = await self.get_client(alias)
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
    
    async def get_group_members(self, alias: str, group_id: int, limit: int = 200, offset: int = 0, username: str = None) -> List[Dict]:
        """Get members from a group with offset support."""
        client = await self.get_client(alias)
        if not client:
            return []

        # Убеждаемся, что peer разрешён перед получением участников
        chat = await ensure_peer_resolved(client, group_id, username)
        if not chat:
            logger.error(f"Не удалось разрешить peer для группы {group_id} (username: {username}) в сессии {alias}")
            return None

        members = []
        current_idx = 0
        try:
            # Note: client.get_chat_members does not support integer offset natively for pagination
            # We iterate and skip manually. This can be slow for very large offsets.
            async for member in client.get_chat_members(group_id):
                if current_idx < offset:
                    current_idx += 1
                    continue

                if len(members) >= limit:
                    break

                if member.user and not member.user.is_bot:
                    members.append({
                        'id': member.user.id,
                        'username': member.user.username,
                        'first_name': member.user.first_name,
                        'last_name': member.user.last_name
                    })

                current_idx += 1
            
            return members

        except Exception as e:
            logger.error(f"Error getting members from {group_id}: {e}")
            return None
    
    async def invite_user(self, alias: str, target_group_id: int, user_id: int, target_username: str = None) -> Dict[str, Any]:
        """Invite a single user to a group."""
        client = await self.get_client(alias)
        if not client:
            return {"success": False, "error": "Session not available"}

        # Убеждаемся, что peer разрешён перед приглашением
        chat = await ensure_peer_resolved(client, target_group_id, target_username)
        if not chat:
            return {"success": False, "error": f"Cannot resolve target group {target_group_id} (username: {target_username})"}

        try:
            await client.add_chat_members(target_group_id, user_id)
            return {"success": True, "user_id": user_id}
        except UserAlreadyParticipant:
            return {"success": True, "user_id": user_id, "already_member": True}
        except FloodWait as e:
            return {"success": False, "error": f"FloodWait: {e.value} seconds", "flood_wait": e.value}
        except UserPrivacyRestricted:
            return {"success": False, "error": "User privacy restricted", "skip": True}
        except UserNotMutualContact:
            return {"success": False, "error": "User not mutual contact", "skip": True}
        except UserChannelsTooMuch:
            return {"success": False, "error": "User in too many channels", "skip": True}
        except (ChatAdminRequired, ChatWriteForbidden):
            return {"success": False, "error": "Admin rights required (cannot add members)", "fatal": True}
        except PeerFlood:
            return {"success": False, "error": "Peer flood - session temporarily blocked", "fatal": True}
        except Exception as e:
            logger.error(f"Error inviting user {user_id}: {e}")
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

    async def join_chat_if_needed(self, client: Client, chat_id: int, username: str = None) -> bool:
        """
        Пытается вступить в чат, если сессия еще не там.
        """
        try:
            # Проверяем статус участника
            try:
                await client.get_chat_member(chat_id, "me")
                return True # Уже в чате
            except UserNotParticipant:
                # Если не участник, пробуем вступить
                chat_input = username if username else chat_id
                logger.info(f"Сессия не в чате {chat_input}, пытаемся вступить...")
                await client.join_chat(chat_input)
                logger.info(f"Сессия успешно вступила в чат {chat_input}")
                return True
            except Exception as e:
                if "ChatAdminRequired" in str(e) or "ChannelPrivate" in str(e):
                    # Для приватных каналов/групп, если мы не там, join_chat может не сработать без ссылки
                    # Но если мы получили ChatAdminRequired на "me", значит мы там, но нет прав админа? 
                    # Обычно это ошибка если запрашивать другого, для "me" должно работать.
                    pass
                
                # Пробуем вступить в любом случае если получили ошибку
                chat_input = username if username else chat_id
                try:
                    await client.join_chat(chat_input)
                    return True
                except Exception as e2:
                    logger.error(f"Не удалось вступить в чат {chat_input}: {e2}")
                    return False
        except Exception as e:
            logger.error(f"Ошибка при проверке/вступлении в чат {chat_id}: {e}")
            return False

    async def validate_session_capability(self, alias: str, source_group_id: int, target_group_id: int, 
                                        source_username: str = None, target_username: str = None) -> Dict[str, Any]:
        """
        Validate if a session can perform inviting from source to target.
        Returns detailed error reason if validation fails.
        """
        client = await self.get_client(alias)
        if not client:
            return {"success": False, "reason": "Session not available or invalid"}

        # Check Source Access
        source_chat = await ensure_peer_resolved(client, source_group_id, source_username)
        if not source_chat:
            return {"success": False, "reason": f"No access to source group {source_group_id} (username: {source_username}): peer not resolved"}

        # Пытаемся вступить в источник, чтобы иметь право парсить участников
        await self.join_chat_if_needed(client, source_group_id, source_username)

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
        await self.join_chat_if_needed(client, target_group_id, target_username)

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
