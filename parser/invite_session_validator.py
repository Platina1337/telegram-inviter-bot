# -*- coding: utf-8 -*-
"""
Enhanced session validation specifically for invite tasks.
Implements role-based validation: data fetchers vs inviters.
"""
import logging
import asyncio
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
        # Special handling for file-based invites
        if task.invite_mode == 'from_file':
            return await self.validate_sessions_for_file_invite_task(task)
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
    
    async def _load_sample_users_from_file(self, file_source: str, sample_size: int = 10) -> List[Dict[str, Any]]:
        """Load a sample of users from file for PEER_ID validation."""
        if not file_source:
            return []
        
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
            from user_files_manager import UserFilesManager
            
            manager = UserFilesManager()
            file_data = manager.load_users_from_file(file_source)
            users = file_data.get('users', [])
            
            if not users:
                return []
            
            # Take a sample for testing - prefer users with user_id
            users_with_id = [u for u in users if u.get('id')]
            if len(users_with_id) >= sample_size:
                import random
                return random.sample(users_with_id, min(sample_size, len(users_with_id)))
            else:
                # If not enough users with ID, take what we have + some with username
                users_with_username = [u for u in users if u.get('username') and u not in users_with_id]
                combined = users_with_id + users_with_username[:sample_size - len(users_with_id)]
                return combined[:sample_size]
                
        except Exception as e:
            logger.error(f"Error loading sample users from {file_source}: {e}")
            return []
    
    async def _validate_session_for_file_users(
        self, alias: str, target_group_id: int, target_username: str,
        sample_users: List[Dict[str, Any]], use_proxy: bool = True, auto_join: bool = True
    ) -> SessionCapabilities:
        """Validate session capabilities for file-based invites."""
        capabilities = SessionCapabilities(last_validated=datetime.now().isoformat())
        
        try:
            client = await self.session_manager.get_client(alias, use_proxy=use_proxy)
            if not client or not client.is_connected:
                capabilities.target_access_error = "Session not connected"
                capabilities.file_users_error = "Session not connected"
                return capabilities
            
            # Test target access with auto-join option
            await self._test_target_access(client, target_group_id, target_username, capabilities, alias, auto_join)
            
            # Test file users access (PEER_ID validation)
            await self._test_file_users_access(client, sample_users, capabilities, alias)
            
        except Exception as e:
            logger.error(f"Error validating file capabilities for {alias}: {e}")
            capabilities.target_access_error = str(e)
            capabilities.file_users_error = str(e)
        
        return capabilities
    
    async def _test_file_users_access(self, client, sample_users: List[Dict[str, Any]], 
                                    capabilities: SessionCapabilities, alias: str):
        """Test if session can access users from file (PEER_ID validation)."""
        if not sample_users:
            capabilities.file_users_error = "No sample users to test"
            return
        
        accessible_count = 0
        total_tested = 0
        peer_errors = 0
        
        logger.info(f"🔍 [FILE_VALIDATION] Session {alias} testing access to {len(sample_users)} sample users")
        
        for user in sample_users[:5]:  # Test only first 5 to avoid rate limits
            user_id = user.get('id')
            username = user.get('username')
            
            # Skip users without ID or username
            if not user_id and not username:
                continue
                
            total_tested += 1
            
            try:
                # Try to resolve the user peer
                target = user_id if user_id else username
                
                # Test with get_users (lightweight check)
                if user_id:
                    user_info = await client.get_users(user_id)
                elif username:
                    user_info = await client.get_users(username)
                else:
                    continue
                
                if user_info:
                    accessible_count += 1
                    logger.debug(f"🔍 [FILE_VALIDATION] Session {alias} can access user {target}")
                else:
                    logger.debug(f"🔍 [FILE_VALIDATION] Session {alias} cannot resolve user {target}")
                    
            except Exception as e:
                error_str = str(e).lower()
                if "peer_id_invalid" in error_str:
                    peer_errors += 1
                    logger.debug(f"🔍 [FILE_VALIDATION] Session {alias} PEER_ID_INVALID for user {target}")
                elif "user_not_found" in error_str:
                    logger.debug(f"🔍 [FILE_VALIDATION] Session {alias} user not found: {target}")
                else:
                    logger.debug(f"🔍 [FILE_VALIDATION] Session {alias} error accessing user {target}: {e}")
        
        # Determine if session can access file users
        if total_tested == 0:
            capabilities.file_users_error = "No valid users to test"
        elif accessible_count == 0:
            if peer_errors == total_tested:
                capabilities.file_users_error = f"All {peer_errors} tested users have PEER_ID_INVALID (session doesn't know these users)"
            else:
                capabilities.file_users_error = f"Cannot access any of {total_tested} tested users"
        elif accessible_count < total_tested * 0.5:  # Less than 50% accessible
            capabilities.file_users_error = f"Low accessibility: only {accessible_count}/{total_tested} users accessible (many PEER_ID_INVALID)"
            capabilities.can_access_file_users = False  # Mark as problematic
        else:
            capabilities.can_access_file_users = True
            logger.info(f"🔍 [FILE_VALIDATION] Session {alias} can access {accessible_count}/{total_tested} sample users")
    
    async def _validate_sessions_basic_file_mode(self, task: Any, sessions_to_check: List[str]) -> Dict[str, Any]:
        """Fallback validation when file users can't be loaded - only check target access."""
        logger.info(f"🔍 [FILE_VALIDATION] Fallback to basic validation (target access only)")
        
        session_roles = []
        inviter_sessions = []
        invalid_sessions = {}
        
        for alias in sessions_to_check:
            if not alias:
                continue
                
            try:
                capabilities = SessionCapabilities(last_validated=datetime.now().isoformat())
                
                client = await self.session_manager.get_client(alias, use_proxy=task.use_proxy)
                if not client or not client.is_connected:
                    capabilities.target_access_error = "Session not connected"
                    invalid_sessions[alias] = "Session not connected"
                    session_roles.append(SessionRole(
                        alias=alias,
                        capabilities=capabilities,
                        role='invalid',
                        priority=0
                    ))
                    continue
                
                # Test only target access
                await self._test_target_access(client, task.target_group_id, task.target_username, capabilities, alias)
                
                if capabilities.can_invite_to_target:
                    capabilities.can_access_file_users = True  # Assume OK if we can't test
                    role = 'inviter'
                    inviter_sessions.append(alias)
                    priority = 10  # Lower priority since we couldn't test file access
                else:
                    role = 'invalid'
                    invalid_sessions[alias] = capabilities.target_access_error or "Cannot invite to target"
                    priority = 0
                
                session_roles.append(SessionRole(
                    alias=alias,
                    capabilities=capabilities,
                    role=role,
                    priority=priority
                ))
                
            except Exception as e:
                logger.error(f"🔍 [FILE_VALIDATION] Error in basic validation for {alias}: {e}")
                invalid_sessions[alias] = str(e)
                session_roles.append(SessionRole(
                    alias=alias,
                    capabilities=SessionCapabilities(),
                    role='invalid',
                    priority=0
                ))
        
        summary = f"Базовая валидация (без проверки файла): {len(inviter_sessions)} пригодных, {len(invalid_sessions)} недоступных"
        
        return {
            'session_roles': session_roles,
            'data_fetcher_sessions': [],
            'inviter_sessions': inviter_sessions,
            'invalid_sessions': invalid_sessions,
            'validation_summary': summary
        }
    
    def _calculate_file_priority(self, capabilities: SessionCapabilities, alias: str) -> int:
        """Calculate priority for file-based invites."""
        priority = 0
        
        if capabilities.can_invite_to_target:
            priority += 15
        if capabilities.can_access_file_users:
            priority += 10
        
        # Penalties for errors
        if capabilities.target_access_error:
            priority -= 10
        if capabilities.file_users_error:
            priority -= 5
        
        return max(0, priority)
    
    def _generate_file_validation_summary(self, session_roles: List[SessionRole], 
                                        inviters: List[str], invalid: Dict[str, str]) -> str:
        """Generate summary for file-based validation."""
        total = len(session_roles)
        valid_count = len(inviters)
        invalid_count = len(invalid)
        
        # Count specific issues
        peer_issues = len([r for r in session_roles 
                          if r.capabilities.file_users_error and 'peer_id_invalid' in r.capabilities.file_users_error.lower()])
        
        # Count auto-join statistics
        auto_joined = len([r for r in session_roles if getattr(r.capabilities, 'auto_joined_target', False)])
        auto_join_failed = len([r for r in session_roles if getattr(r.capabilities, 'auto_join_error', None)])
        
        summary_parts = []
        if valid_count > 0:
            summary_parts.append(f"{valid_count} пригодных для инвайтинга")
        if auto_joined > 0:
            summary_parts.append(f"{auto_joined} автоприсоединено")
        if peer_issues > 0:
            summary_parts.append(f"{peer_issues} с проблемами PEER_ID")
        if auto_join_failed > 0:
            summary_parts.append(f"{auto_join_failed} не удалось присоединить")
        if invalid_count > 0:
            summary_parts.append(f"{invalid_count} недоступных")
        
        if not summary_parts:
            return "Нет доступных сессий для файлового инвайтинга"
        
        return f"Из {total} сессий: " + ", ".join(summary_parts)
    
    async def _check_target_membership(self, client, target_group_id: int, alias: str) -> Dict[str, Any]:
        """Check if session is a member of target group."""
        try:
            member_info = await client.get_chat_member(target_group_id, "me")
            member_status = getattr(member_info, 'status', None)
            
            # Normalize status to string (Pyrogram 2.x returns enum)
            status_str = getattr(member_status, "name", str(member_status)).lower()
            
            logger.info(f"🔍 [MEMBERSHIP_CHECK] Session {alias} status in target group: {status_str}")
            
            return {
                'is_member': status_str in ['administrator', 'creator', 'member'],
                'status': status_str,
                'raw_status': member_status
            }
            
        except Exception as e:
            error_str = str(e).lower()
            logger.info(f"🔍 [MEMBERSHIP_CHECK] Session {alias} membership check error: {e}")
            
            if "user_not_participant" in error_str:
                return {'is_member': False, 'status': 'not_participant', 'error': 'Not a member'}
            elif "chat_admin_required" in error_str:
                return {'is_member': False, 'status': 'access_denied', 'error': 'Admin rights required'}
            else:
                return {'is_member': False, 'status': 'unknown', 'error': str(e)}
    
    async def _attempt_auto_join(self, client, target_group_id: int, target_username: str, 
                               alias: str, capabilities: SessionCapabilities) -> bool:
        """Attempt to automatically join target group."""
        try:
            logger.info(f"🔍 [AUTO_JOIN] Session {alias} attempting to join target group...")
            
            # Strategy 1: Join by username (if available)
            if target_username and target_username.startswith('@'):
                username_clean = target_username[1:]  # Remove @ prefix
                try:
                    await client.join_chat(username_clean)
                    logger.info(f"✅ [AUTO_JOIN] Session {alias} joined via username @{username_clean}")
                    return True
                except Exception as e:
                    error_str = str(e).lower()
                    logger.debug(f"🔍 [AUTO_JOIN] Username join failed for {alias}: {e}")
                    
                    if "invite_hash_expired" in error_str:
                        capabilities.auto_join_error = f"Invite link expired for @{username_clean}"
                    elif "channels_too_much" in error_str:
                        capabilities.auto_join_error = "Too many channels/groups joined (Telegram limit)"
                    elif "channel_private" in error_str:
                        capabilities.auto_join_error = f"Private channel @{username_clean}, need invite link"
                    elif "flood_wait" in error_str:
                        capabilities.auto_join_error = "Rate limited by Telegram, try again later"
                    else:
                        capabilities.auto_join_error = f"Username join failed: {e}"
            
            # Strategy 2: Join by group ID (fallback)
            try:
                await client.join_chat(target_group_id)
                logger.info(f"✅ [AUTO_JOIN] Session {alias} joined via group ID {target_group_id}")
                return True
            except Exception as e:
                error_str = str(e).lower()
                logger.debug(f"🔍 [AUTO_JOIN] ID join failed for {alias}: {e}")
                
                if "invite_hash_expired" in error_str:
                    capabilities.auto_join_error = "Invite link expired"
                elif "channels_too_much" in error_str:
                    capabilities.auto_join_error = "Too many channels/groups joined (Telegram limit)"
                elif "channel_private" in error_str:
                    capabilities.auto_join_error = "Private channel, need invite link"
                elif "flood_wait" in error_str:
                    capabilities.auto_join_error = "Rate limited by Telegram, try again later"
                elif "peer_id_invalid" in error_str:
                    capabilities.auto_join_error = "Invalid group ID or group not accessible"
                else:
                    capabilities.auto_join_error = f"Auto-join failed: {e}"
            
            logger.warning(f"❌ [AUTO_JOIN] All join strategies failed for session {alias}")
            return False
            
        except Exception as e:
            logger.error(f"❌ [AUTO_JOIN] Unexpected error during auto-join for {alias}: {e}")
            capabilities.auto_join_error = f"Unexpected auto-join error: {e}"
            return False
    
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
            
            # Test target access capabilities (with auto-join if not file mode)
            auto_join_enabled = invite_mode != 'from_file'  # Enable auto-join for non-file modes
            await self._test_target_access(client, target_group_id, target_username, capabilities, alias, auto_join_enabled)
            
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
                                capabilities: SessionCapabilities, alias: str, auto_join: bool = True):
        """Test ability to invite users to target group with optional auto-join."""
        try:
            # Test basic peer resolution
            from .session_manager import ensure_peer_resolved
            target_peer = await ensure_peer_resolved(client, target_group_id, target_username)
            if not target_peer:
                capabilities.target_access_error = f"Cannot resolve target group {target_group_id}"
                return
            
            # Test if we're a member of target group
            logger.info(f"🔍 [ENHANCED_VALIDATION] Session {alias} checking target group access: {target_group_id}")
            
            member_check_result = await self._check_target_membership(client, target_group_id, alias)
            
            if member_check_result['is_member']:
                # Already a member - check permissions
                status_str = member_check_result['status']
                if status_str in ['administrator', 'creator', 'member']:
                    capabilities.can_invite_to_target = True
                    logger.info(f"🔍 [ENHANCED_VALIDATION] Session {alias} can invite as {status_str}")
                else:
                    capabilities.target_access_error = f"Insufficient permissions in target group (status: {status_str})"
            else:
                # Not a member - attempt auto-join if enabled
                if auto_join:
                    logger.info(f"🔍 [AUTO_JOIN] Session {alias} not in target group, attempting auto-join...")
                    
                    join_success = await self._attempt_auto_join(client, target_group_id, target_username, alias, capabilities)
                    
                    if join_success:
                        # Recheck membership after auto-join
                        await asyncio.sleep(2)  # Small delay for Telegram to update
                        recheck_result = await self._check_target_membership(client, target_group_id, alias)
                        
                        if recheck_result['is_member']:
                            status_str = recheck_result['status']
                            if status_str in ['administrator', 'creator', 'member']:
                                capabilities.can_invite_to_target = True
                                capabilities.auto_joined_target = True
                                logger.info(f"✅ [AUTO_JOIN] Session {alias} successfully joined and can invite as {status_str}")
                            else:
                                capabilities.target_access_error = f"Joined but insufficient permissions: {status_str}"
                        else:
                            capabilities.target_access_error = "Auto-joined but membership verification failed"
                    else:
                        # Auto-join failed, error already set in capabilities.auto_join_error
                        capabilities.target_access_error = capabilities.auto_join_error or "Failed to auto-join target group"
                else:
                    capabilities.target_access_error = "Not a member of target group (auto-join disabled)"
        
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
    
    async def validate_sessions_for_file_invite_task(self, task: Any) -> Dict[str, Any]:
        """
        Enhanced validation specifically for file-based invite tasks.
        Tests if sessions can access users from the file (PEER_ID validation).
        """
        logger.info(f"🔍 [FILE_VALIDATION] Starting file-based validation for task {task.id}")
        
        session_roles = []
        inviter_sessions = []
        invalid_sessions = {}
        
        # Get sessions to validate
        sessions_to_check = task.available_sessions if task.available_sessions else []
        if not sessions_to_check:
            return {
                'session_roles': [],
                'data_fetcher_sessions': [],  # Always empty for file mode
                'inviter_sessions': [],
                'invalid_sessions': {'global': 'No sessions assigned'},
                'validation_summary': 'No sessions available for validation'
            }
        
        # Load sample users from file to test PEER_ID access
        sample_users = await self._load_sample_users_from_file(task.file_source)
        if not sample_users:
            logger.warning(f"🔍 [FILE_VALIDATION] No sample users loaded from {task.file_source}")
            # If we can't load users, fall back to basic target validation only
            return await self._validate_sessions_basic_file_mode(task, sessions_to_check)
        
        logger.info(f"🔍 [FILE_VALIDATION] Testing {len(sessions_to_check)} sessions with {len(sample_users)} sample users")
        
        for alias in sessions_to_check:
            if not alias:
                continue
                
            try:
                capabilities = await self._validate_session_for_file_users(
                    alias, task.target_group_id, task.target_username,
                    sample_users, task.use_proxy, getattr(task, 'auto_join_target', True)
                )
                
                role = 'inviter' if capabilities.can_invite_to_target and capabilities.can_access_file_users else 'invalid'
                priority = self._calculate_file_priority(capabilities, alias)
                
                session_role = SessionRole(
                    alias=alias,
                    capabilities=capabilities,
                    role=role,
                    priority=priority
                )
                session_roles.append(session_role)
                
                if role == 'inviter':
                    inviter_sessions.append(alias)
                else:
                    error_msg = capabilities.target_access_error or capabilities.file_users_error or "Unknown error"
                    invalid_sessions[alias] = error_msg
                
                logger.info(f"🔍 [FILE_VALIDATION] Session {alias}: role={role}, "
                          f"target_invite={capabilities.can_invite_to_target}, "
                          f"file_users_access={capabilities.can_access_file_users}")
                
            except Exception as e:
                logger.error(f"🔍 [FILE_VALIDATION] Error validating session {alias}: {e}")
                invalid_sessions[alias] = str(e)
                session_roles.append(SessionRole(
                    alias=alias,
                    capabilities=SessionCapabilities(),
                    role='invalid',
                    priority=0
                ))
        
        # Sort by priority
        session_roles.sort(key=lambda x: x.priority, reverse=True)
        inviter_sessions.sort(key=lambda alias: next(
            (role.priority for role in session_roles if role.alias == alias), 0
        ), reverse=True)
        
        summary = self._generate_file_validation_summary(
            session_roles, inviter_sessions, invalid_sessions
        )
        
        logger.info(f"🔍 [FILE_VALIDATION] Validation complete: {summary}")
        
        return {
            'session_roles': session_roles,
            'data_fetcher_sessions': [],  # Always empty for file mode
            'inviter_sessions': inviter_sessions,
            'invalid_sessions': invalid_sessions,
            'validation_summary': summary
        }