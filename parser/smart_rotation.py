# -*- coding: utf-8 -*-
"""
Smart session rotation for invite tasks with role separation.
"""
import logging
from typing import List, Optional, Dict, Any
import random

logger = logging.getLogger(__name__)


class SmartSessionRotator:
    """Handles intelligent rotation of sessions based on their roles."""
    
    def __init__(self, db):
        self.db = db
    
    async def get_next_data_fetcher(self, task: Any) -> Optional[str]:
        """Get next session for data fetching (member list or messages)."""
        if not task.data_fetcher_sessions:
            logger.warning(f"🔄 [SMART_ROTATION] Task {task.id}: No data fetcher sessions available")
            return None
        
        current_fetcher = task.current_data_fetcher
        fetchers = task.data_fetcher_sessions
        
        if len(fetchers) == 1:
            # Only one fetcher available
            return fetchers[0]
        
        # Find current index and rotate to next
        try:
            current_index = fetchers.index(current_fetcher) if current_fetcher in fetchers else -1
            next_index = (current_index + 1) % len(fetchers)
            next_fetcher = fetchers[next_index]
            
            logger.info(f"🔄 [SMART_ROTATION] Task {task.id}: Data fetcher rotation: {current_fetcher} → {next_fetcher}")
            
            # Update in database
            await self.db.update_invite_task(task.id, current_data_fetcher=next_fetcher)
            task.current_data_fetcher = next_fetcher
            
            return next_fetcher
            
        except Exception as e:
            logger.error(f"🔄 [SMART_ROTATION] Error rotating data fetcher for task {task.id}: {e}")
            # Fallback to first available
            return fetchers[0]
    
    async def get_next_inviter(self, task: Any, reason: str = "rotation") -> Optional[str]:
        """Get next session for inviting users."""
        if not task.inviter_sessions:
            logger.warning(f"🔄 [SMART_ROTATION] Task {task.id}: No inviter sessions available")
            return None
        
        current_inviter = task.current_inviter
        inviters = task.inviter_sessions
        
        if len(inviters) == 1:
            # Only one inviter available
            return inviters[0]
        
        # Find current index and rotate to next
        try:
            current_index = inviters.index(current_inviter) if current_inviter in inviters else -1
            next_index = (current_index + 1) % len(inviters)
            next_inviter = inviters[next_index]
            
            logger.info(f"🔄 [SMART_ROTATION] Task {task.id}: Inviter rotation ({reason}): {current_inviter} → {next_inviter}")
            
            # Update in database
            await self.db.update_invite_task(task.id, current_inviter=next_inviter)
            task.current_inviter = next_inviter
            
            return next_inviter
            
        except Exception as e:
            logger.error(f"🔄 [SMART_ROTATION] Error rotating inviter for task {task.id}: {e}")
            # Fallback to first available
            return inviters[0]
    
    async def should_rotate_data_fetcher(self, task: Any, requests_made: int) -> bool:
        """Determine if data fetcher should be rotated."""
        if len(task.data_fetcher_sessions) <= 1:
            return False
        
        # Rotate data fetchers less frequently (they don't get rate limited as much)
        # Only rotate every 50-100 requests or on errors
        if requests_made > 0 and requests_made % 75 == 0:
            logger.info(f"🔄 [SMART_ROTATION] Task {task.id}: Data fetcher rotation due to request count ({requests_made})")
            return True
        
        return False
    
    async def should_rotate_inviter(self, task: Any, invites_made: int, 
                                  last_error: str = None) -> bool:
        """Determine if inviter should be rotated."""
        if len(task.inviter_sessions) <= 1:
            return False
        
        # Rotate on error
        if last_error:
            error_lower = last_error.lower()
            if any(keyword in error_lower for keyword in [
                'flood', 'too_many', 'limit', 'banned', 'restricted', 
                'peer_flood', 'channels_too_much'
            ]):
                logger.info(f"🔄 [SMART_ROTATION] Task {task.id}: Inviter rotation due to error: {last_error}")
                return True
        
        # Rotate based on task settings
        if task.rotate_sessions and task.rotate_every > 0:
            if invites_made > 0 and invites_made % task.rotate_every == 0:
                logger.info(f"🔄 [SMART_ROTATION] Task {task.id}: Inviter rotation due to invite count ({invites_made})")
                return True
        
        return False
    
    async def handle_session_error(self, task: Any, session_alias: str, error: str, 
                                 session_type: str = "inviter") -> Optional[str]:
        """Handle session error and potentially rotate to next available session."""
        logger.warning(f"🔄 [SMART_ROTATION] Task {task.id}: Session {session_alias} ({session_type}) error: {error}")
        
        # Determine if this is a critical error that requires immediate rotation
        error_lower = error.lower()
        critical_errors = [
            'session_revoked', 'auth_key', 'banned', 'deleted',
            'flood_wait_x', 'peer_flood', 'user_channels_too_much'
        ]
        
        is_critical = any(keyword in error_lower for keyword in critical_errors)
        
        if not is_critical:
            # Non-critical error, maybe just a temporary issue
            return session_alias
        
        logger.warning(f"🔄 [SMART_ROTATION] Task {task.id}: Critical error detected, rotating {session_type}")
        
        # Remove failed session from appropriate list temporarily
        if session_type == "data_fetcher":
            if session_alias in task.data_fetcher_sessions:
                # Don't permanently remove, just rotate to next
                return await self.get_next_data_fetcher(task)
        elif session_type == "inviter":
            if session_alias in task.inviter_sessions:
                # Don't permanently remove, just rotate to next  
                return await self.get_next_inviter(task, reason="error")
        
        return None
    
    def get_session_role_info(self, task: Any, session_alias: str) -> Dict[str, Any]:
        """Get detailed role information for a session."""
        if not hasattr(task, 'session_roles') or not task.session_roles:
            return {"role": "unknown", "capabilities": {}}
        
        for role_info in task.session_roles:
            if isinstance(role_info, dict) and role_info.get('alias') == session_alias:
                return {
                    "role": role_info.get('role', 'unknown'),
                    "capabilities": {
                        "can_fetch_members": role_info.get('can_fetch_members', False),
                        "can_fetch_messages": role_info.get('can_fetch_messages', False),
                        "can_invite": role_info.get('can_invite', False)
                    },
                    "errors": {
                        "source_error": role_info.get('source_error'),
                        "target_error": role_info.get('target_error')
                    },
                    "priority": role_info.get('priority', 0)
                }
        
        return {"role": "unknown", "capabilities": {}}
    
    def format_rotation_status(self, task: Any) -> str:
        """Format current rotation status for display."""
        status_parts = []
        
        if task.current_data_fetcher:
            fetcher_info = self.get_session_role_info(task, task.current_data_fetcher)
            fetcher_role = fetcher_info.get("role", "unknown")
            status_parts.append(f"📊 Получение данных: {task.current_data_fetcher} ({fetcher_role})")
        
        if task.current_inviter:
            inviter_info = self.get_session_role_info(task, task.current_inviter)
            inviter_role = inviter_info.get("role", "unknown")
            status_parts.append(f"📨 Инвайтинг: {task.current_inviter} ({inviter_role})")
        
        if not status_parts:
            return "❌ Нет активных сессий"
        
        return "\n".join(status_parts)
    
    def format_available_sessions_summary(self, task: Any) -> str:
        """Format summary of available sessions by role."""
        if not hasattr(task, 'session_roles') or not task.session_roles:
            return "Информация о ролях сессий недоступна"
        
        role_counts = {"both": 0, "data_fetcher": 0, "inviter": 0, "invalid": 0}
        
        for role_info in task.session_roles:
            if isinstance(role_info, dict):
                role = role_info.get('role', 'invalid')
                role_counts[role] = role_counts.get(role, 0) + 1
        
        summary_parts = []
        if role_counts["both"] > 0:
            summary_parts.append(f"🔄 Универсальные: {role_counts['both']}")
        if role_counts["data_fetcher"] > 0:
            summary_parts.append(f"📊 Только данные: {role_counts['data_fetcher']}")
        if role_counts["inviter"] > 0:
            summary_parts.append(f"📨 Только инвайты: {role_counts['inviter']}")
        if role_counts["invalid"] > 0:
            summary_parts.append(f"❌ Недоступные: {role_counts['invalid']}")
        
        return " | ".join(summary_parts) if summary_parts else "Нет доступных сессий"