# -*- coding: utf-8 -*-
"""
Enhanced invite methods with smart role-based session rotation.
"""
import logging
import asyncio
import random
from typing import Dict, Any
import re

logger = logging.getLogger(__name__)


class EnhancedInviteMethods:
    """Enhanced methods for invite tasks with role separation."""
    
    def __init__(self, worker):
        self.worker = worker
        self.db = worker.db
        self.session_manager = worker.session_manager
    
    async def run_invite_task_enhanced(self, task_id: int):
        """Enhanced invite task with smart role-based session rotation."""
        from .smart_rotation import SmartSessionRotator
        
        rotator = SmartSessionRotator(self.db)
        data_requests_made = 0
        invites_made = 0
        
        try:
            task = await self.db.get_invite_task(task_id)
            if not task:
                logger.error(f"Task {task_id} not found")
                return
            
            # Ensure we have the enhanced session data
            if not hasattr(task, 'data_fetcher_sessions') or not task.data_fetcher_sessions:
                logger.error(f"🔄 [ENHANCED_TASK] Task {task_id}: No data fetcher sessions available")
                await self.db.update_invite_task(
                    task_id, 
                    status='failed', 
                    error_message="No sessions can fetch source data"
                )
                return
            
            if not hasattr(task, 'inviter_sessions') or not task.inviter_sessions:
                logger.error(f"🔄 [ENHANCED_TASK] Task {task_id}: No inviter sessions available")
                await self.db.update_invite_task(
                    task_id, 
                    status='failed', 
                    error_message="No sessions can invite to target"
                )
                return
            
            logger.info(f"🚀 [ENHANCED_TASK] Starting task {task_id} with enhanced rotation")
            logger.info(f"🚀 [ENHANCED_TASK] Data fetchers: {task.data_fetcher_sessions}")
            logger.info(f"🚀 [ENHANCED_TASK] Inviters: {task.inviter_sessions}")
            
            await self.db.update_invite_task(task_id, worker_phase='fetching_members')
            
            # Get current data fetcher session
            current_data_fetcher = task.current_data_fetcher or task.data_fetcher_sessions[0]
            data_fetcher_client = await self.session_manager.get_client(current_data_fetcher, task.use_proxy)
            
            if not data_fetcher_client:
                logger.error(f"🔄 [ENHANCED_TASK] Cannot get data fetcher client: {current_data_fetcher}")
                await self.db.update_invite_task(
                    task_id, 
                    status='failed', 
                    error_message=f"Data fetcher session {current_data_fetcher} unavailable"
                )
                return
            
            # Get members using data fetcher session
            logger.info(f"📊 [ENHANCED_TASK] Task {task_id}: Getting members with session {current_data_fetcher}")
            members = await self.session_manager.get_group_members(
                current_data_fetcher, 
                task.source_group_id,
                limit=None,
                offset=task.current_offset
            )
            
            if not members:
                logger.info(f"📊 [ENHANCED_TASK] Task {task_id}: No more members to process")
                await self.db.update_invite_task(task_id, status='completed')
                await self.worker._notify_user(task.user_id, f"✅ **Задача завершена**: Приглашено {task.invited_count} пользователей")
                return
            
            data_requests_made += 1
            logger.info(f"📊 [ENHANCED_TASK] Task {task_id}: Fetched {len(members)} members (total requests: {data_requests_made})")
            
            # Process members with inviter sessions
            current_inviter = task.current_inviter or task.inviter_sessions[0]
            processed_in_batch = 0
            session_consecutive_invites = 0
            
            await self.db.update_invite_task(task_id, worker_phase='inviting')
            
            for i, member in enumerate(members):
                if self.worker._stop_flags.get(task_id, False):
                    logger.info(f"🔄 [ENHANCED_TASK] Task {task_id} stopped by user")
                    break
                
                if task.limit and task.invited_count >= task.limit:
                    logger.info(f"🔄 [ENHANCED_TASK] Task {task_id} reached limit: {task.limit}")
                    break
                
                await self.worker._update_heartbeat_if_needed(task_id)
                
                # Check if we should rotate data fetcher (less frequent)
                if await rotator.should_rotate_data_fetcher(task, data_requests_made):
                    new_data_fetcher = await rotator.get_next_data_fetcher(task)
                    if new_data_fetcher and new_data_fetcher != current_data_fetcher:
                        current_data_fetcher = new_data_fetcher
                        data_fetcher_client = await self.session_manager.get_client(current_data_fetcher, task.use_proxy)
                        logger.info(f"🔄 [ENHANCED_TASK] Rotated data fetcher to: {current_data_fetcher}")
                
                # Check if we should rotate inviter
                if await rotator.should_rotate_inviter(task, invites_made):
                    new_inviter = await rotator.get_next_inviter(task, "scheduled_rotation")
                    if new_inviter and new_inviter != current_inviter:
                        current_inviter = new_inviter
                        session_consecutive_invites = 0
                        logger.info(f"🔄 [ENHANCED_TASK] Rotated inviter to: {current_inviter}")
                
                # Process member with current inviter
                result = await self.process_member_enhanced(
                    task_id, task, member, current_inviter, current_data_fetcher
                )
                
                if result.get('success'):
                    invites_made += 1
                    session_consecutive_invites += 1
                    processed_in_batch += 1
                    
                    # Update progress
                    await self.db.update_invite_task(
                        task_id,
                        invited_count=task.invited_count + 1,
                        current_offset=task.current_offset + i + 1,
                        current_inviter=current_inviter,
                        current_data_fetcher=current_data_fetcher
                    )
                    task.invited_count += 1
                    
                    # Standard delay after successful invite
                    if task.invited_count % task.delay_every == 0:
                        min_delay = max(1, int(task.delay_seconds * 0.8))
                        max_delay = int(task.delay_seconds * 1.2)
                        actual_delay = random.randint(min_delay, max_delay)
                        
                        logger.info(f"🔄 [ENHANCED_TASK] Task {task_id}: Delay {actual_delay}s after {task.delay_every} invites")
                        await self.db.update_invite_task(task_id, worker_phase='sleeping')
                        await self.worker._smart_sleep(task_id, actual_delay)
                    else:
                        await self.db.update_invite_task(task_id, worker_phase='sleeping')
                        await self.worker._smart_sleep(task_id, random.randint(2, 5))
                
                elif result.get('flood_wait'):
                    wait_time = result['flood_wait']
                    logger.warning(f"🔄 [ENHANCED_TASK] FloodWait {wait_time}s on inviter {current_inviter}")
                    
                    # Try to rotate to another inviter
                    new_inviter = await rotator.get_next_inviter(task, "flood_wait")
                    if new_inviter and new_inviter != current_inviter:
                        current_inviter = new_inviter
                        session_consecutive_invites = 0
                        logger.info(f"🔄 [ENHANCED_TASK] Rotated to inviter {current_inviter} due to FloodWait")
                        continue  # Retry with new session
                    else:
                        # No other inviter available, wait
                        await self.db.update_invite_task(task_id, worker_phase='sleeping')
                        await asyncio.sleep(min(wait_time, 300))
                
                elif result.get('fatal'):
                    error_detail = result.get('error', 'Unknown error')
                    logger.error(f"🔄 [ENHANCED_TASK] Fatal error with inviter {current_inviter}: {error_detail}")
                    
                    # Try to rotate to another inviter
                    new_inviter = await rotator.handle_session_error(
                        task, current_inviter, error_detail, "inviter"
                    )
                    if new_inviter and new_inviter != current_inviter:
                        current_inviter = new_inviter
                        session_consecutive_invites = 0
                        logger.info(f"🔄 [ENHANCED_TASK] Recovered with inviter {current_inviter}")
                        continue  # Retry with new session
                    else:
                        # No recovery possible
                        error_msg = f"All inviter sessions failed: {error_detail}"
                        await self.db.update_invite_task(task_id, status='failed', error_message=error_msg)
                        await self.worker._notify_user(task.user_id, f"❌ **Задача остановлена**: {error_msg}")
                        return
                
                # Update offset
                await self.db.update_invite_task(task_id, current_offset=task.current_offset + i + 1)
                task.current_offset += i + 1
            
            # Check if we need more members
            if processed_in_batch > 0:
                # Continue with next batch
                await self.run_invite_task_enhanced(task_id)
            else:
                # No more members or all failed
                await self.db.update_invite_task(task_id, status='completed')
                await self.worker._notify_user(task.user_id, f"✅ **Задача завершена**: Приглашено {task.invited_count} пользователей")
        
        except asyncio.CancelledError:
            logger.info(f"🔄 [ENHANCED_TASK] Task {task_id} was cancelled")
        except Exception as e:
            logger.error(f"🔄 [ENHANCED_TASK] Error in enhanced task {task_id}: {e}", exc_info=True)
            await self.db.update_invite_task(
                task_id,
                status='failed',
                error_message=f"Enhanced task error: {str(e)}"
            )
        finally:
            self.worker._stop_flags.pop(task_id, None)
            self.worker.running_tasks.pop(task_id, None)
    
    async def process_member_enhanced(self, task_id: int, task: Any, member: Any, 
                                    inviter_session: str, data_fetcher_session: str) -> Dict[str, Any]:
        """Process a single member with enhanced session handling.
        member can be: dict from get_group_members (id, username, first_name, last_name, ...)
                      or Pyrogram ChatMember object with .user attribute
        """
        try:
            # Normalize member: get_group_members returns dict, legacy code expected .user object
            if isinstance(member, dict):
                user_id = member.get('id')
                username = member.get('username')
                first_name = member.get('first_name') or ''
                last_name = member.get('last_name') or ''
            else:
                user_id = member.user.id
                username = getattr(member.user, 'username', None)
                first_name = getattr(member.user, 'first_name', '') or ''
                last_name = getattr(member.user, 'last_name', '') or ''
            full_name = f"{first_name} {last_name}".strip()
            if user_id is None:
                logger.warning(f"📨 [ENHANCED_PROCESS] Task {task_id}: member has no id, skipping")
                return {"success": False, "reason": "no_user_id"}
            
            # Get last_online_date using data fetcher session (if different from inviter)
            last_online_date = None
            if data_fetcher_session != inviter_session:
                try:
                    logger.info(f"📊 [ENHANCED_PROCESS] Task {task_id}: Getting last_online for user {full_name} @{username} (ID: {user_id}) via data fetcher {data_fetcher_session}")
                    user_info = await self.session_manager.get_client(data_fetcher_session, task.use_proxy)
                    if user_info:
                        users = await user_info.get_users([user_id])
                        if users:
                            user = users[0]
                            if hasattr(user, 'last_online_date') and user.last_online_date:
                                last_online_date = user.last_online_date
                                logger.info(f"📊 [ENHANCED_PROCESS] Task {task_id}: Got last_online for user {full_name} @{username} (ID: {user_id}): {last_online_date}")
                            else:
                                logger.info(f"📊 [ENHANCED_PROCESS] Task {task_id}: last_online unavailable for user {full_name} @{username} (ID: {user_id})")
                except Exception as e:
                    logger.warning(f"📊 [ENHANCED_PROCESS] Task {task_id}: Could not get last_online for user {user_id}: {e}")
            else:
                # Same session for both operations, use existing logic
                try:
                    logger.info(f"📊 [ENHANCED_PROCESS] Task {task_id}: Getting last_online for user {full_name} @{username} (ID: {user_id}) via unified session {inviter_session}")
                    client = await self.session_manager.get_client(inviter_session, task.use_proxy)
                    if client:
                        users = await client.get_users([user_id])
                        if users:
                            user = users[0]
                            if hasattr(user, 'last_online_date') and user.last_online_date:
                                last_online_date = user.last_online_date
                                logger.info(f"📊 [ENHANCED_PROCESS] Task {task_id}: Got last_online for user {full_name} @{username} (ID: {user_id}): {last_online_date}")
                            else:
                                logger.info(f"📊 [ENHANCED_PROCESS] Task {task_id}: last_online unavailable for user {full_name} @{username} (ID: {user_id})")
                except Exception as e:
                    logger.warning(f"📊 [ENHANCED_PROCESS] Task {task_id}: Could not get last_online for user {user_id}: {e}")
            
            # Apply filtering using data fetcher's client (needs source group access for admin check)
            client = await self.session_manager.get_client(data_fetcher_session, task.use_proxy)
            if client:
                skip_reason = await self.worker._should_skip_user(
                    task, client, user_id, last_online_date, username, first_name
                )
                if skip_reason:
                    return {"success": False, "reason": "filtered"}
            
            # Check if already in target
            if await self.is_user_in_target(task, user_id, inviter_session):
                logger.info(f"📨 [ENHANCED_PROCESS] Task {task_id}: User {full_name} @{username} (ID: {user_id}) already in target group. Skip.")
                return {"success": False, "reason": "already_member"}
            
            # Perform invite using inviter session
            logger.info(f"📨 [ENHANCED_PROCESS] Task {task_id}: Inviting user {full_name} @{username} (ID: {user_id}) via inviter {inviter_session}")
            invite_result = await self.session_manager.invite_user(
                inviter_session, task.target_group_id, user_id,
                target_username=task.target_username, use_proxy=task.use_proxy
            )
            
            if invite_result.get('success'):
                logger.info(f"📨 [ENHANCED_PROCESS] Task {task_id}: Successfully invited user {full_name} @{username} (ID: {user_id}) via {inviter_session}")
                return {"success": True}
            else:
                error = invite_result.get('error', 'Unknown error')
                logger.warning(f"📨 [ENHANCED_PROCESS] Task {task_id}: Failed to invite user {user_id} via {inviter_session}: {error}")
                
                # Categorize error
                if 'flood' in error.lower() or 'wait' in error.lower():
                    # Extract wait time if possible
                    wait_match = re.search(r'(\d+)', error)
                    wait_time = int(wait_match.group(1)) if wait_match else 60
                    return {"success": False, "flood_wait": wait_time, "error": error}
                elif any(keyword in error.lower() for keyword in ['session_revoked', 'auth_key', 'banned']):
                    return {"success": False, "fatal": True, "error": error}
                else:
                    return {"success": False, "error": error}
        
        except Exception as e:
            logger.error(f"📨 [ENHANCED_PROCESS] Error processing member in task {task_id}: {e}")
            return {"success": False, "error": str(e)}
    
    async def is_user_in_target(self, task: Any, user_id: int, session_alias: str) -> bool:
        """Check if user is already in target group using specified session."""
        try:
            client = await self.session_manager.get_client(session_alias, task.use_proxy)
            if not client:
                return False
            
            member_info = await client.get_chat_member(task.target_group_id, user_id)
            status = getattr(member_info, 'status', None)
            return status in ['member', 'administrator', 'creator']
        
        except Exception:
            # If we can't check, assume not in target (better to try invite than skip)
            return False