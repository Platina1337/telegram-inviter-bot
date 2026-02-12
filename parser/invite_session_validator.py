# -*- coding: utf-8 -*-
"""
Enhanced session validation specifically for invite tasks.
Implements role-based validation: data fetchers vs inviters.
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from models import SessionCapabilities, SessionRole

logger = logging.getLogger(__name__)


class InviteSessionValidator:
    """Enhanced validator for invite task sessions with role separation."""
    
    def __init__(self, session_manager):
        self.session_manager = session_manager
    
    async def validate_sessions_for_invite_task(self, task: Any) -> Dict[str, Any]:
        """
        Enhanced validation for invite tasks with role-based capabilities.
        
        Returns:
            Dict with:
            - 'session_roles': List[SessionRole] - detailed role assignments
            - 'data_fetcher_sessions': List[str] - sessions that can fetch source data  
            - 'inviter_sessions': List[str] - sessions that can invite to target
            - 'invalid_sessions': Dict[str, str] - sessions with errors
            - 'validation_summary': str - human-readable summary
        """
        logger.info(f"🔍 [ENHANCED_VALIDATION] Starting role-based validation for invite task {task.id}")
        
        session_roles = []
        data_fetcher_sessions = []
        inviter_sessions = []
        invalid_sessions = {}
        
        # Get sessions to validate
        sessions_to_check = task.available_sessions if task.available_sessions else []
        if not sessions_to_check:
            return {
                'session_roles': [],
                'data_fetcher_sessions': [],
                'inviter_sessions': [],
                'invalid_sessions': {'global': 'No sessions assigned'},
                'validation_summary': 'No sessions available for validation'
            }
        
        logger.info(f"🔍 [ENHANCED_VALIDATION] Validating {len(sessions_to_check)} sessions: {sessions_to_check}")
        
        for alias in sessions_to_check:
            if not alias:
                continue
                
            try:
                capabilities = await self._validate_session_capabilities(
                    alias, task.source_group_id, task.target_group_id,
                    task.source_username, task.target_username,
                    task.invite_mode, task.use_proxy
                )
                
                role = self._determine_session_role(capabilities)
                priority = self._calculate_priority(capabilities, alias)
                
                session_role = SessionRole(
                    alias=alias,
                    capabilities=capabilities,
                    role=role,
                    priority=priority
                )
                session_roles.append(session_role)
                
                # Categorize sessions by capabilities
                if role == 'data_fetcher':
                    data_fetcher_sessions.append(alias)
                elif role == 'inviter':
                    inviter_sessions.append(alias)
                elif role == 'both':
                    data_fetcher_sessions.append(alias)
                    inviter_sessions.append(alias)
                elif role == 'invalid':
                    error_msg = capabilities.source_access_error or capabilities.target_access_error or "Unknown error"
                    invalid_sessions[alias] = error_msg
                
                logger.info(f"🔍 [ENHANCED_VALIDATION] Session {alias}: role={role}, "
                          f"source_members={capabilities.can_fetch_source_members}, "
                          f"source_messages={capabilities.can_fetch_source_messages}, "
                          f"target_invite={capabilities.can_invite_to_target}")
                
            except Exception as e:
                logger.error(f"🔍 [ENHANCED_VALIDATION] Error validating session {alias}: {e}")
                invalid_sessions[alias] = str(e)
                session_roles.append(SessionRole(
                    alias=alias,
                    capabilities=SessionCapabilities(),
                    role='invalid',
                    priority=0
                ))
        
        # Sort by priority (higher first)
        session_roles.sort(key=lambda x: x.priority, reverse=True)
        data_fetcher_sessions.sort(key=lambda alias: next(
            (role.priority for role in session_roles if role.alias == alias), 0
        ), reverse=True)
        inviter_sessions.sort(key=lambda alias: next(
            (role.priority for role in session_roles if role.alias == alias), 0
        ), reverse=True)
        
        summary = self._generate_validation_summary(
            session_roles, data_fetcher_sessions, inviter_sessions, invalid_sessions
        )
        
        logger.info(f"🔍 [ENHANCED_VALIDATION] Validation complete: {summary}")
        
        return {
            'session_roles': session_roles,
            'data_fetcher_sessions': data_fetcher_sessions,
            'inviter_sessions': inviter_sessions,
            'invalid_sessions': invalid_sessions,
            'validation_summary': summary
        }
    
    async def _validate_session_capabilities(
        self, alias: str, source_group_id: int, target_group_id: int,
        source_username: str = None, target_username: str = None,
        invite_mode: str = 'member_list', use_proxy: bool = True
    ) -> SessionCapabilities:
        """Validate specific capabilities of a session."""
        capabilities = SessionCapabilities(last_validated=datetime.now().isoformat())
        
        try:
            client = await self.session_manager.get_client(alias, use_proxy=use_proxy)
            if not client or not client.is_connected:
                capabilities.source_access_error = "Session not connected"
                capabilities.target_access_error = "Session not connected"
                return capabilities
            
            # Test source access capabilities
            if invite_mode != 'from_file':
                await self._test_source_access(client, source_group_id, source_username, 
                                             invite_mode, capabilities)
            else:
                # For file mode, we don't need source access
                capabilities.can_fetch_source_members = False
                capabilities.can_fetch_source_messages = False
            
            # Test target access capabilities  
            await self._test_target_access(client, target_group_id, target_username, capabilities, alias)
            
        except Exception as e:
            logger.error(f"Error validating capabilities for {alias}: {e}")
            capabilities.source_access_error = str(e)
            capabilities.target_access_error = str(e)
        
        return capabilities
    
    async def _test_source_access(self, client, source_group_id: int, source_username: str,
                                invite_mode: str, capabilities: SessionCapabilities):
        """Test access to source group for data fetching."""
        try:
            # Test basic peer resolution
            from .session_manager import ensure_peer_resolved
            source_peer = await ensure_peer_resolved(client, source_group_id, source_username)
            if not source_peer:
                capabilities.source_access_error = f"Cannot resolve source group {source_group_id}"
                return
            
            # Test member list access (for member_list mode)
            if invite_mode == 'member_list':
                try:
                    # Try to get a small sample of members
                    member_count = 0
                    async for member in client.get_chat_members(source_group_id, limit=5):
                        member_count += 1
                        break  # Just test if we can get at least one
                    
                    if member_count > 0:
                        capabilities.can_fetch_source_members = True
                    else:
                        # Check if group is empty or we have no access
                        chat_info = await client.get_chat(source_group_id)
                        total_members = getattr(chat_info, 'members_count', 0)
                        if total_members == 0:
                            capabilities.can_fetch_source_members = True  # Empty group is OK
                        else:
                            capabilities.source_access_error = "Cannot fetch members (restricted access)"
                
                except Exception as e:
                    error_str = str(e).lower()
                    if "chat_admin_required" in error_str:
                        capabilities.source_access_error = "Admin rights required to fetch members"
                    elif "channel_private" in error_str:
                        capabilities.source_access_error = "Private channel, cannot fetch members"
                    else:
                        capabilities.source_access_error = f"Member list access error: {e}"
            
            # Test message access (for message_based mode)
            if invite_mode == 'message_based':
                try:
                    # Try to get recent messages
                    message_count = 0
                    async for message in client.get_chat_history(source_group_id, limit=5):
                        message_count += 1
                        break  # Just test if we can get at least one
                    
                    capabilities.can_fetch_source_messages = message_count > 0
                    if not capabilities.can_fetch_source_messages:
                        capabilities.source_access_error = "Cannot fetch messages from source"
                
                except Exception as e:
                    capabilities.source_access_error = f"Message access error: {e}"
        
        except Exception as e:
            capabilities.source_access_error = f"Source access test failed: {e}"
    
    async def _test_target_access(self, client, target_group_id: int, target_username: str,
                                capabilities: SessionCapabilities, alias: str):
        """Test ability to invite users to target group."""
        try:
            # Test basic peer resolution
            from .session_manager import ensure_peer_resolved
            target_peer = await ensure_peer_resolved(client, target_group_id, target_username)
            if not target_peer:
                capabilities.target_access_error = f"Cannot resolve target group {target_group_id}"
                return
            
            # Test if we're a member of target group
            logger.info(f"🔍 [ENHANCED_VALIDATION] Session {alias} checking target group access: {target_group_id}")
            try:
                member_info = await client.get_chat_member(target_group_id, "me")
                member_status = getattr(member_info, 'status', None)
                logger.info(f"🔍 [ENHANCED_VALIDATION] Session {alias} raw member_status: {member_status} (type: {type(member_status)})")
                
                # Normalize status to string (Pyrogram 2.x returns enum)
                status_str = getattr(member_status, "name", str(member_status)).lower()
                logger.info(f"🔍 [ENHANCED_VALIDATION] Session {alias} target group status: {status_str}")
                
                if status_str in ['administrator', 'creator']:
                    capabilities.can_invite_to_target = True
                    logger.info(f"🔍 [ENHANCED_VALIDATION] Session {alias} can invite as {status_str}")
                elif status_str == 'member':
                    # Regular members might be able to invite depending on group settings
                    # We'll assume they can and let the actual invite attempt determine this
                    capabilities.can_invite_to_target = True
                    logger.info(f"🔍 [ENHANCED_VALIDATION] Session {alias} can invite as member")
                else:
                    capabilities.target_access_error = f"Insufficient permissions in target group (status: {status_str})"
                    logger.info(f"🔍 [ENHANCED_VALIDATION] Session {alias} cannot invite, status: {status_str}")
            
            except Exception as e:
                error_str = str(e).lower()
                logger.info(f"🔍 [ENHANCED_VALIDATION] Session {alias} target group access error: {e}")
                if "user_not_participant" in error_str:
                    capabilities.target_access_error = "Not a member of target group"
                elif "chat_admin_required" in error_str:
                    capabilities.target_access_error = "Admin rights required in target group"
                else:
                    capabilities.target_access_error = f"Target access error: {e}"
        
        except Exception as e:
            capabilities.target_access_error = f"Target access test failed: {e}"
    
    def _determine_session_role(self, capabilities: SessionCapabilities) -> str:
        """Determine the role of a session based on its capabilities."""
        can_fetch_data = capabilities.can_fetch_source_members or capabilities.can_fetch_source_messages
        can_invite = capabilities.can_invite_to_target
        
        if can_fetch_data and can_invite:
            return 'both'
        elif can_fetch_data:
            return 'data_fetcher'
        elif can_invite:
            return 'inviter'
        else:
            return 'invalid'
    
    def _calculate_priority(self, capabilities: SessionCapabilities, alias: str) -> int:
        """Calculate priority score for session (higher = better)."""
        priority = 0
        
        # Bonus for multiple capabilities
        if capabilities.can_fetch_source_members:
            priority += 10
        if capabilities.can_fetch_source_messages:
            priority += 8
        if capabilities.can_invite_to_target:
            priority += 15
        
        # Penalty for errors
        if capabilities.source_access_error:
            priority -= 5
        if capabilities.target_access_error:
            priority -= 10
        
        # Small bonus for 'both' capability sessions
        can_fetch = capabilities.can_fetch_source_members or capabilities.can_fetch_source_messages
        if can_fetch and capabilities.can_invite_to_target:
            priority += 5
        
        return max(0, priority)
    
    def _generate_validation_summary(self, session_roles: List[SessionRole], 
                                   data_fetchers: List[str], inviters: List[str], 
                                   invalid: Dict[str, str]) -> str:
        """Generate human-readable validation summary."""
        total = len(session_roles)
        both_count = len([r for r in session_roles if r.role == 'both'])
        data_only = len([r for r in session_roles if r.role == 'data_fetcher'])
        invite_only = len([r for r in session_roles if r.role == 'inviter'])
        invalid_count = len(invalid)
        
        summary_parts = []
        
        if both_count > 0:
            summary_parts.append(f"{both_count} универсальных")
        if data_only > 0:
            summary_parts.append(f"{data_only} только для получения данных")
        if invite_only > 0:
            summary_parts.append(f"{invite_only} только для инвайтинга")
        if invalid_count > 0:
            summary_parts.append(f"{invalid_count} недоступных")
        
        if not summary_parts:
            return "Нет доступных сессий"
        
        return f"Из {total} сессий: " + ", ".join(summary_parts)