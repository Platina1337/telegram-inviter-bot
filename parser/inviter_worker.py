# -*- coding: utf-8 -*-
"""
Inviter Worker - handles the actual invite logic.
"""
import asyncio
import logging
import random
import httpx
from typing import Dict, Any, Optional
from datetime import datetime

from .database import Database
from .session_manager import SessionManager
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from models import InviteTask

logger = logging.getLogger(__name__)


class InviterWorker:
    """Worker that performs the actual inviting."""
    
    def __init__(self, db: Database, session_manager: SessionManager):
        self.db = db
        self.session_manager = session_manager
        self.running_tasks: Dict[int, asyncio.Task] = {}
        self._stop_flags: Dict[int, bool] = {}
    
    async def start_invite_task(self, task_id: int) -> Dict[str, Any]:
        """Start an invite task."""
        task = await self.db.get_invite_task(task_id)
        if not task:
            return {"success": False, "error": "Task not found"}
        
        if task_id in self.running_tasks and not self.running_tasks[task_id].done():
            return {"success": False, "error": "Task is already running"}
        
        # Update task status
        await self.db.update_invite_task(task_id, status='running')
        self._stop_flags[task_id] = False
        
        # Start the task in background - choose method based on invite_mode
        if task.invite_mode == 'message_based':
            self.running_tasks[task_id] = asyncio.create_task(
                self._run_message_based_invite_task(task_id)
            )
            logger.info(f"Started message-based invite task {task_id}")
        else:
            self.running_tasks[task_id] = asyncio.create_task(
                self._run_invite_task(task_id)
            )
            logger.info(f"Started member-list invite task {task_id}")
        
        return {"success": True, "task_id": task_id, "status": "running"}
    
    async def stop_invite_task(self, task_id: int) -> Dict[str, Any]:
        """Stop an invite task."""
        self._stop_flags[task_id] = True
        
        if task_id in self.running_tasks:
            task = self.running_tasks[task_id]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        await self.db.update_invite_task(task_id, status='paused')
        logger.info(f"Stopped invite task {task_id}")
        return {"success": True, "task_id": task_id, "status": "paused"}
    
    async def _notify_user(self, user_id: int, message: str):
        """Send notification to user via Telegram Bot API."""
        from .config import config
        if not config.BOT_TOKEN:
            logger.warning("Cannot send notification: BOT_TOKEN not set")
            return

        url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/sendMessage"
        try:
            async with httpx.AsyncClient() as client:
                await client.post(url, json={
                    "chat_id": user_id,
                    "text": message,
                    "parse_mode": "Markdown"
                })
        except Exception as e:
            logger.error(f"Failed to send notification to user {user_id}: {e}")

    async def _run_invite_task(self, task_id: int):
        """Main invite logic."""
        session_consecutive_invites = 0
        try:
            while not self._stop_flags.get(task_id, False):
                task = await self.db.get_invite_task(task_id)
                if not task:
                    logger.error(f"Task {task_id} not found")
                    break
                
                # Check if limit reached
                if task.limit and task.invited_count >= task.limit:
                    logger.info(f"Task {task_id} reached limit: {task.invited_count}/{task.limit}")
                    await self.db.update_invite_task(task_id, status='completed')
                    break
                
                # Log rotation settings at start of each iteration
                logger.debug(f"Task {task_id}: rotate_sessions={task.rotate_sessions}, rotate_every={task.rotate_every}, session_consecutive_invites={session_consecutive_invites}")

                # Get session client
                client = await self.session_manager.get_client(task.session_alias)
                if not client:
                    logger.error(f"Failed to get client for session {task.session_alias}")

                    # Try to rotate session if enabled (auto-rotate on failure)
                    if task.rotate_sessions and task.available_sessions:
                        logger.info(f"Session {task.session_alias} failed, attempting rotation...")
                        new_session = await self._rotate_session(task)
                        if new_session:
                            session_consecutive_invites = 0
                            continue

                    # If rotation failed or disabled, stop task
                    error_msg = f"Session {task.session_alias} unavailable"
                    if task.rotate_sessions:
                        error_msg += " (Rotation failed: no suitable sessions)"

                    await self.db.update_invite_task(
                        task_id,
                        status='failed',
                        error_message=error_msg
                    )
                    break
                
                # Get already invited users
                invited_ids = await self.db.get_invited_user_ids(
                    task.source_group_id, 
                    task.target_group_id
                )
                
                # Get members from source group
                members = await self.session_manager.get_group_members(
                    task.session_alias,
                    task.source_group_id,
                    limit=50,
                    offset=task.current_offset,
                    username=task.source_username
                )
                
                if members is None:
                    logger.error(f"Task {task_id}: Failed to get members with session {task.session_alias}")
                    
                    if task.rotate_sessions and task.available_sessions:
                        logger.info(f"Attempting rotation due to member fetch error...")
                        new_session = await self._rotate_session(task)
                        if new_session:
                            session_consecutive_invites = 0
                            continue
                            
                    error_msg = f"Ошибка доступа к группе-источнику (сессия {task.session_alias})"
                    await self.db.update_invite_task(task_id, status='failed', error_message=error_msg)
                    
                    # Notify user
                    notify_text = (
                        f"❌ **Задача инвайтинга остановлена**\n\n"
                        f"Группа-источник: {task.source_group_title}\n"
                        f"Ошибка: Не удалось получить список участников через сессию `{task.session_alias}`.\n"
                        f"Возможно, сессия была исключена из группы или группа стала недоступна."
                    )
                    await self._notify_user(task.user_id, notify_text)
                    break

                if not members:
                    # Проверяем, действительно ли участники закончились, или сессия "слепая"
                    source_info = await self.session_manager.check_group_access(task.session_alias, task.source_group_id)
                    has_access = source_info.get('has_access', False)
                    total_in_group = source_info.get('members_count')

                    if not has_access:
                        logger.warning(f"Task {task_id}: Session {task.session_alias} has no access to source group")
                        if task.rotate_sessions and task.available_sessions and len(task.available_sessions) > 1:
                            current_blind = task.session_alias
                            task.available_sessions = [s for s in task.available_sessions if s != current_blind]
                            await self.db.update_invite_task(task_id, available_sessions=task.available_sessions)

                            notify_text = (
                                f"⚠️ **Сессия `{current_blind}` без доступа к группе-источнику**\n"
                                f"Пробую другую сессию..."
                            )
                            await self._notify_user(task.user_id, notify_text)

                            new_session = await self._rotate_session(task)
                            if new_session:
                                session_consecutive_invites = 0
                                continue

                        error_msg = f"Нет доступа к группе-источнику через сессию {task.session_alias}"
                        await self.db.update_invite_task(task_id, status='failed', error_message=error_msg)
                        await self._notify_user(task.user_id, f"❌ **Задача остановлена**\n\n{error_msg}")
                        break

                    # Если количество участников неизвестно, не считаем задачу завершенной
                    if total_in_group is None:
                        logger.warning(f"Task {task_id}: Session {task.session_alias} sees NO members and members_count is unknown")
                        if task.rotate_sessions and task.available_sessions and len(task.available_sessions) > 1:
                            current_blind = task.session_alias
                            task.available_sessions = [s for s in task.available_sessions if s != current_blind]
                            await self.db.update_invite_task(task_id, available_sessions=task.available_sessions)

                            notify_text = (
                                f"⚠️ **Сессия `{current_blind}` не видит список участников**\n"
                                f"Возможно, список скрыт. Пробую другую сессию..."
                            )
                            await self._notify_user(task.user_id, notify_text)

                            new_session = await self._rotate_session(task)
                            if new_session:
                                session_consecutive_invites = 0
                                continue

                        error_msg = f"Сессия {task.session_alias} не видит участников группы-источника"
                        await self.db.update_invite_task(task_id, status='failed', error_message=error_msg)
                        await self._notify_user(task.user_id, f"❌ **Задача остановлена**\n\n{error_msg}")
                        break

                    # Если в группе участников больше, чем текущий offset, это не конец
                    if total_in_group > task.current_offset:
                        logger.warning(
                            f"Task {task_id}: Session {task.session_alias} sees 0 members at offset {task.current_offset}, "
                            f"but group has ~{total_in_group}. Treating session as blind."
                        )
                        if task.rotate_sessions and task.available_sessions and len(task.available_sessions) > 1:
                            current_blind = task.session_alias
                            task.available_sessions = [s for s in task.available_sessions if s != current_blind]
                            await self.db.update_invite_task(task_id, available_sessions=task.available_sessions)

                            notify_text = (
                                f"⚠️ **Сессия `{current_blind}` не видит участников**\n"
                                f"Пробую другую сессию..."
                            )
                            await self._notify_user(task.user_id, notify_text)

                            new_session = await self._rotate_session(task)
                            if new_session:
                                session_consecutive_invites = 0
                                continue

                        error_msg = f"Сессия {task.session_alias} не видит участников (ожидалось ~{total_in_group})"
                        await self.db.update_invite_task(task_id, status='failed', error_message=error_msg)
                        await self._notify_user(task.user_id, f"❌ **Задача остановлена**\n\n{error_msg}")
                        break

                    # Реально конец списка
                    logger.info(f"Task {task_id}: No more members to invite (verified)")
                    await self.db.update_invite_task(task_id, status='completed')
                    break
                
                invited_in_batch = 0
                processed_in_batch = 0
                for member in members:
                    processed_in_batch += 1
                    if self._stop_flags.get(task_id, False):
                        # Update offset before stopping
                        await self.db.update_invite_task(
                            task_id,
                            current_offset=task.current_offset + processed_in_batch
                        )
                        break
                    
                    user_id = member['id']
                    
                    # Skip already invited (local DB check)
                    if user_id in invited_ids:
                        continue
                    
                    # PRE-CHECK: Check if user is ALREADY in target group (API check)
                    # The checking is bound to the current task.target_group_id. 
                    # If target group changes, this check applies to the NEW group.
                    try:
                        target_member = await client.get_chat_member(task.target_group_id, user_id)
                        
                        # Get status safely (handles both string and Enum)
                        raw_status = target_member.status
                        status = getattr(raw_status, "name", str(raw_status)).upper()
                        
                        # Allow only 'LEFT' users to be re-invited.
                        # Everyone else (Member, Admin, Restricted, Banned) is "occupied".
                        if status != 'LEFT':
                            logger.info(f"Task {task_id}: User {user_id} is {status} in target group {task.target_group_id}. Skipping.")
                            
                            # Distinguish ban vs member
                            status_code = 'banned_in_target' if status in ['KICKED', 'BANNED'] else 'already_in_target'
                            
                            await self.db.add_invite_record(
                                task_id, user_id,
                                username=member.get('username'),
                                first_name=member.get('first_name'),
                                status=status_code
                            )
                            # Small sleep to prevent API flood during skips
                            await asyncio.sleep(0.1)
                            continue

                    except Exception:
                        # UserNotParticipant (400) or other error usually means 
                        # user is NOT in the group, so we proceed to invite.
                        pass

                    # Check limit
                    if task.limit and task.invited_count >= task.limit:
                        await self.db.update_invite_task(task_id, status='completed')
                        return
                    
                    # Check session rotation limit
                    if task.rotate_sessions and task.rotate_every > 0 and session_consecutive_invites >= task.rotate_every:
                        logger.info(f"Task {task_id}: Session {task.session_alias} reached {session_consecutive_invites} invites. Rotating...")
                        
                        # Update offset BEFORE rotation to save progress accurately.
                        # IMPORTANT: processed_in_batch already includes current member,
                        # but we didn't process it yet. So save offset up to previous member.
                        offset_to_save = task.current_offset + max(processed_in_batch - 1, 0)
                        await self.db.update_invite_task(
                            task_id,
                            current_offset=offset_to_save
                        )
                        task.current_offset = offset_to_save
                        
                        new_session = await self._rotate_session(task)
                        if new_session:
                            session_consecutive_invites = 0
                            # Mark that we updated offset and should skip the final update
                            processed_in_batch = -1 
                            break
                    
                    # Invite the user
                    result = await self.session_manager.invite_user(
                        task.session_alias,
                        task.target_group_id,
                        user_id,
                        target_username=task.target_username
                    )
                    
                    if result.get('success'):
                        # Record success
                        await self.db.add_invite_record(
                            task_id, user_id,
                            username=member.get('username'),
                            first_name=member.get('first_name'),
                            status='success'
                        )
                        await self.db.update_invite_task(
                            task_id,
                            invited_count=task.invited_count + 1
                        )
                        task.invited_count += 1
                        session_consecutive_invites += 1
                        invited_in_batch += 1
                        logger.info(f"Task {task_id}: Invited user {user_id} ({task.invited_count}/{task.limit or '∞'})")
                        
                        # Delay based on frequency and randomization
                        if task.invited_count % task.delay_every == 0:
                            # Add randomization: 80% to 120% of the base delay
                            min_delay = max(1, int(task.delay_seconds * 0.8))
                            max_delay = int(task.delay_seconds * 1.2)
                            actual_delay = random.randint(min_delay, max_delay)
                            
                            logger.info(f"Task {task_id}: Waiting {actual_delay}s after {task.delay_every} invites (base delay: {task.delay_seconds}s)")
                            await asyncio.sleep(actual_delay)
                        else:
                            # Small fixed delay between invites if no major delay is scheduled
                            await asyncio.sleep(random.randint(2, 5))
                    
                    elif result.get('flood_wait'):
                        # FloodWait - pause and maybe rotate session
                        wait_time = result['flood_wait']
                        logger.warning(f"FloodWait {wait_time}s for session {task.session_alias}")
                        
                        if task.rotate_sessions and task.available_sessions:
                            logger.info(f"FloodWait on {task.session_alias}, attempting rotation...")
                            new_session = await self._rotate_session(task)
                            if new_session:
                                logger.info(f"Rotated to session {new_session} due to FloodWait")
                                session_consecutive_invites = 0
                                break
                        
                        # Wait out the flood
                        await asyncio.sleep(min(wait_time, 300))
                    
                    elif result.get('fatal'):
                        # Fatal error - stop with this session
                        error_detail = result.get('error', 'Unknown error')
                        logger.error(f"Fatal error with session {task.session_alias}: {error_detail}")
                        
                        # NEW: Notify user that session is being rotated due to fatal error
                        notify_text = (
                            f"⚠️ **Проблема с сессией `{task.session_alias}`**\n\n"
                            f"Группа: {task.target_group_title}\n"
                            f"Ошибка: `{error_detail}`\n"
                        )
                        
                        if task.rotate_sessions and task.available_sessions and len(task.available_sessions) > 1:
                            logger.info(f"Fatal error on {task.session_alias}, attempting rotation...")
                            
                            # Remove failing session from available list for this run
                            current_failing = task.session_alias
                            task.available_sessions = [s for s in task.available_sessions if s != current_failing]
                            await self.db.update_invite_task(task_id, available_sessions=task.available_sessions)
                            
                            notify_text += f"🔄 Сессия исключена из текущей задачи. Пробую переключиться на другую..."
                            await self._notify_user(task.user_id, notify_text)
                            
                            # IMPORTANT: Don't skip this user! Decrement processed count
                            processed_in_batch -= 1
                            
                            new_session = await self._rotate_session(task)
                            if new_session:
                                session_consecutive_invites = 0
                                break
                        
                        # If rotation failed or disabled, stop task
                        error_msg = f"Критическая ошибка: {error_detail}"
                        if task.rotate_sessions:
                             error_msg += " (Ротация не удалась или нет других сессий)"

                        await self.db.update_invite_task(
                            task_id,
                            status='failed',
                            error_message=error_msg
                        )
                        
                        notify_text += f"\n❌ Задача остановлена."
                        await self._notify_user(task.user_id, notify_text)
                        return
                    
                    elif result.get('skip'):
                        # Skip this user
                        await self.db.add_invite_record(
                            task_id, user_id,
                            username=member.get('username'),
                            first_name=member.get('first_name'),
                            status='skipped',
                            error_message=result.get('error')
                        )
                    
                    else:
                        # Other error
                        await self.db.add_invite_record(
                            task_id, user_id,
                            username=member.get('username'),
                            first_name=member.get('first_name'),
                            status='failed',
                            error_message=result.get('error')
                        )
                
                # Update offset only if we finished the full batch and didn't rotate
                if processed_in_batch > 0:
                    await self.db.update_invite_task(
                        task_id,
                        current_offset=task.current_offset + processed_in_batch
                    )
            
            # Task finished or stopped
            task = await self.db.get_invite_task(task_id)
            if task and task.status == 'running':
                await self.db.update_invite_task(task_id, status='paused')
        
        except asyncio.CancelledError:
            logger.info(f"Task {task_id} was cancelled")
        except Exception as e:
            logger.error(f"Error in invite task {task_id}: {e}", exc_info=True)
            await self.db.update_invite_task(
                task_id,
                status='failed',
                error_message=str(e)
            )
        finally:
            self._stop_flags.pop(task_id, None)
            self.running_tasks.pop(task_id, None)
    
    async def _run_message_based_invite_task(self, task_id: int):
        """Message-based invite logic - iterates through chat history and invites message authors."""
        session_consecutive_invites = 0
        
        try:
            task = await self.db.get_invite_task(task_id)
            if not task:
                logger.error(f"Task {task_id} not found")
                return
            
            # Validate initial session capability
            logger.info(f"Task {task_id}: Validating session {task.session_alias} capability...")
            validation = await self.session_manager.validate_session_capability(
                task.session_alias,
                task.source_group_id,
                task.target_group_id,
                source_username=task.source_username,
                target_username=task.target_username
            )
            
            if not validation.get('success'):
                logger.warning(f"Task {task_id}: Initial session {task.session_alias} validation failed: {validation.get('reason')}")
                
                # Try to rotate if enabled
                if task.rotate_sessions and task.available_sessions and len(task.available_sessions) > 1:
                    logger.info(f"Task {task_id}: Attempting to find suitable session...")
                    new_session = await self._rotate_session(task)
                    if new_session:
                        logger.info(f"Task {task_id}: Rotated to session {new_session}")
                        task = await self.db.get_invite_task(task_id)  # Reload task with new session
                    else:
                        error_msg = f"Не удалось найти подходящую сессию. {validation.get('reason')}"
                        await self.db.update_invite_task(
                            task_id,
                            status='failed',
                            error_message=error_msg
                        )
                        await self._notify_user(
                            task.user_id,
                            f"❌ **Задача остановлена**\n\n{error_msg}"
                        )
                        return
                else:
                    error_msg = f"Сессия {task.session_alias} не подходит: {validation.get('reason')}"
                    await self.db.update_invite_task(
                        task_id,
                        status='failed',
                        error_message=error_msg
                    )
                    await self._notify_user(
                        task.user_id,
                        f"❌ **Задача остановлена**\n\n{error_msg}"
                    )
                    return
            
            # Get session client
            client = await self.session_manager.get_client(task.session_alias)
            if not client:
                logger.error(f"Failed to get client for session {task.session_alias}")
                
                # Try to rotate if enabled
                if task.rotate_sessions and task.available_sessions:
                    logger.info(f"Task {task_id}: Client unavailable, attempting rotation...")
                    new_session = await self._rotate_session(task)
                    if new_session:
                        client = await self.session_manager.get_client(new_session)
                        if client:
                            task = await self.db.get_invite_task(task_id)
                        else:
                            await self.db.update_invite_task(
                                task_id,
                                status='failed',
                                error_message=f"Session {new_session} unavailable after rotation"
                            )
                            return
                    else:
                        await self.db.update_invite_task(
                            task_id,
                            status='failed',
                            error_message=f"Session {task.session_alias} unavailable and rotation failed"
                        )
                        return
                else:
                    await self.db.update_invite_task(
                        task_id,
                        status='failed',
                        error_message=f"Session {task.session_alias} unavailable"
                    )
                    return
            
            # Get already invited users for this source->target pair
            invited_ids = await self.db.get_invited_user_ids(
                task.source_group_id, 
                task.target_group_id
            )
            
            logger.info(f"Task {task_id}: Starting message-based inviting. Already invited: {len(invited_ids)} users")
            
            # Join source group if needed
            await self.session_manager.join_chat_if_needed(
                client, 
                task.source_group_id, 
                task.source_username
            )
            
            # Notify user that processing started
            await self._notify_user(
                task.user_id,
                f"✅ **Обработка сообщений запущена**\n\n"
                f"Группа-источник: {task.source_group_title}\n"
                f"Группа-цель: {task.target_group_title}\n"
                f"Режим: Добавление по сообщениям\n"
                f"Сессия: {task.session_alias}\n"
                f"Лимит: {task.limit or '∞'}"
            )
            
            # Track unique users we've seen
            seen_users = set()
            processed_messages = 0
            
            # Iterate through chat history
            logger.info(f"Task {task_id}: Starting to iterate through chat history")
            
            try:
                async for message in client.get_chat_history(task.source_group_id):
                    # Check stop flag
                    if self._stop_flags.get(task_id, False):
                        logger.info(f"Task {task_id}: Stop flag set, breaking")
                        break
                    
                    # Reload task to get fresh data
                    task = await self.db.get_invite_task(task_id)
                    if not task:
                        break
                    
                    # Check if limit reached
                    if task.limit and task.invited_count >= task.limit:
                        logger.info(f"Task {task_id} reached limit: {task.invited_count}/{task.limit}")
                        await self.db.update_invite_task(task_id, status='completed')
                        break
                    
                    processed_messages += 1
                    
                    # Get message author
                    user = message.from_user
                    if not user or user.is_bot:
                        continue
                    
                    user_id = user.id
                    
                    # Skip if we already processed this user in this run
                    if user_id in seen_users:
                        continue
                    
                    seen_users.add(user_id)
                    
                    # Reload invited_ids to check latest state
                    current_invited_ids = await self.db.get_invited_user_ids(
                        task.source_group_id, 
                        task.target_group_id
                    )
                    
                    # Skip if already invited
                    if user_id in current_invited_ids:
                        logger.debug(f"Task {task_id}: User {user_id} already invited, skipping")
                        continue
                    
                    # PRE-CHECK: Check if user is ALREADY in target group
                    try:
                        current_client = await self.session_manager.get_client(task.session_alias)
                        if not current_client:
                            logger.warning(f"Task {task_id}: Client unavailable for pre-check")
                            continue
                        
                        target_member = await current_client.get_chat_member(task.target_group_id, user_id)
                        
                        # Get status safely
                        raw_status = target_member.status
                        status = getattr(raw_status, "name", str(raw_status)).upper()
                        
                        # Skip if user is already in target (not LEFT)
                        if status != 'LEFT':
                            logger.info(f"Task {task_id}: User {user_id} is {status} in target group. Skipping.")
                            
                            status_code = 'banned_in_target' if status in ['KICKED', 'BANNED'] else 'already_in_target'
                            
                            await self.db.add_invite_record(
                                task_id, user_id,
                                username=user.username,
                                first_name=user.first_name,
                                status=status_code
                            )
                            await asyncio.sleep(0.1)
                            continue
                    
                    except Exception:
                        # User not in group, proceed to invite
                        pass
                    
                    # Check session rotation limit
                    if task.rotate_sessions and task.rotate_every > 0 and session_consecutive_invites >= task.rotate_every:
                        logger.info(f"Task {task_id}: Session {task.session_alias} reached {session_consecutive_invites} invites. Rotating...")
                        
                        new_session = await self._rotate_session(task)
                        if new_session:
                            session_consecutive_invites = 0
                        else:
                            logger.warning(f"Task {task_id}: Rotation failed, continuing with current session")
                    
                    # Invite the user
                    result = await self.session_manager.invite_user(
                        task.session_alias,
                        task.target_group_id,
                        user_id,
                        target_username=task.target_username
                    )
                    
                    if result.get('success'):
                        # Record success
                        await self.db.add_invite_record(
                            task_id, user_id,
                            username=user.username,
                            first_name=user.first_name,
                            status='success'
                        )
                        await self.db.update_invite_task(
                            task_id,
                            invited_count=task.invited_count + 1
                        )
                        task.invited_count += 1
                        session_consecutive_invites += 1
                        logger.info(f"Task {task_id}: Invited user {user_id} (@{user.username or 'no_username'}) ({task.invited_count}/{task.limit or '∞'})")
                        
                        # Delay based on frequency
                        if task.invited_count % task.delay_every == 0:
                            min_delay = max(1, int(task.delay_seconds * 0.8))
                            max_delay = int(task.delay_seconds * 1.2)
                            actual_delay = random.randint(min_delay, max_delay)
                            
                            logger.info(f"Task {task_id}: Waiting {actual_delay}s after {task.delay_every} invites")
                            await asyncio.sleep(actual_delay)
                        else:
                            await asyncio.sleep(random.randint(2, 5))
                    
                    elif result.get('flood_wait'):
                        # FloodWait - pause and maybe rotate session
                        wait_time = result['flood_wait']
                        logger.warning(f"FloodWait {wait_time}s for session {task.session_alias}")
                        
                        if task.rotate_sessions and task.available_sessions:
                            logger.info(f"FloodWait on {task.session_alias}, attempting rotation...")
                            new_session = await self._rotate_session(task)
                            if new_session:
                                logger.info(f"Rotated to session {new_session} due to FloodWait")
                                session_consecutive_invites = 0
                                continue
                        
                        # Wait out the flood
                        await asyncio.sleep(min(wait_time, 300))
                    
                    elif result.get('fatal'):
                        # Fatal error - try to rotate session
                        error_detail = result.get('error', 'Unknown error')
                        logger.error(f"Fatal error with session {task.session_alias}: {error_detail}")
                        
                        # If rotation is enabled, try to rotate and continue
                        if task.rotate_sessions and task.available_sessions:
                            logger.info(f"Fatal error on {task.session_alias}, attempting rotation...")
                            
                            notify_text = (
                                f"⚠️ **Проблема с сессией `{task.session_alias}`**\n\n"
                                f"Группа: {task.target_group_title}\n"
                                f"Ошибка: `{error_detail}`\n"
                            )
                            
                            # Remove failing session from available list
                            if len(task.available_sessions) > 1:
                                current_failing = task.session_alias
                                task.available_sessions = [s for s in task.available_sessions if s != current_failing]
                                await self.db.update_invite_task(task_id, available_sessions=task.available_sessions)
                                
                                notify_text += f"🔄 Сессия `{current_failing}` исключена из задачи. Пробую другую..."
                                await self._notify_user(task.user_id, notify_text)
                            
                            new_session = await self._rotate_session(task)
                            if new_session:
                                logger.info(f"Task {task_id}: Successfully rotated to {new_session}, continuing...")
                                session_consecutive_invites = 0
                                task = await self.db.get_invite_task(task_id)  # Reload task
                                # Get new client
                                client = await self.session_manager.get_client(task.session_alias)
                                if not client:
                                    logger.error(f"Task {task_id}: Failed to get new client after rotation")
                                    # Try one more rotation
                                    new_session = await self._rotate_session(task)
                                    if new_session:
                                        client = await self.session_manager.get_client(new_session)
                                        task = await self.db.get_invite_task(task_id)
                                    if not client:
                                        # Give up
                                        error_msg = "Не удалось получить рабочую сессию после ротации"
                                        await self.db.update_invite_task(
                                            task_id,
                                            status='failed',
                                            error_message=error_msg
                                        )
                                        await self._notify_user(task.user_id, f"❌ **Задача остановлена**\n\n{error_msg}")
                                        break
                                # Continue with next user
                                continue
                            else:
                                logger.warning(f"Task {task_id}: Rotation failed, but will continue trying with current session")
                                # Don't break - continue trying with current session
                                await asyncio.sleep(5)  # Small delay before retry
                                continue
                        else:
                            # Rotation not enabled - stop task
                            error_msg = f"Критическая ошибка: {error_detail}"
                            await self.db.update_invite_task(
                                task_id,
                                status='failed',
                                error_message=error_msg
                            )
                            
                            notify_text = (
                                f"❌ **Задача остановлена**\n\n"
                                f"Сессия: `{task.session_alias}`\n"
                                f"Ошибка: `{error_detail}`\n\n"
                                f"Включите ротацию сессий для автоматического переключения при ошибках."
                            )
                            await self._notify_user(task.user_id, notify_text)
                            break
                    
                    elif result.get('skip'):
                        # Skip this user
                        await self.db.add_invite_record(
                            task_id, user_id,
                            username=user.username,
                            first_name=user.first_name,
                            status='skipped',
                            error_message=result.get('error')
                        )
                    
                    else:
                        # Other error
                        await self.db.add_invite_record(
                            task_id, user_id,
                            username=user.username,
                            first_name=user.first_name,
                            status='failed',
                            error_message=result.get('error')
                        )
                
                logger.info(f"Task {task_id}: Finished processing chat history. Processed {processed_messages} messages, found {len(seen_users)} unique users")
                
            except Exception as e:
                logger.error(f"Task {task_id}: Error iterating chat history: {e}", exc_info=True)
                await self.db.update_invite_task(
                    task_id,
                    status='failed',
                    error_message=f"Ошибка при обработке истории: {str(e)}"
                )
                await self._notify_user(
                    task.user_id,
                    f"❌ **Ошибка при обработке сообщений**\n\n"
                    f"Группа: {task.source_group_title}\n"
                    f"Ошибка: {str(e)}"
                )
                return
            
            # Task finished
            task = await self.db.get_invite_task(task_id)
            if task and task.status == 'running':
                await self.db.update_invite_task(task_id, status='completed')
                await self._notify_user(
                    task.user_id,
                    f"✅ **Обработка завершена**\n\n"
                    f"Группа-источник: {task.source_group_title}\n"
                    f"Группа-цель: {task.target_group_title}\n"
                    f"Обработано сообщений: {processed_messages}\n"
                    f"Найдено уникальных пользователей: {len(seen_users)}\n"
                    f"Добавлено: {task.invited_count}/{task.limit or '∞'}"
                )
        
        except asyncio.CancelledError:
            logger.info(f"Task {task_id} was cancelled")
            task = await self.db.get_invite_task(task_id)
            if task and task.status == 'running':
                await self.db.update_invite_task(task_id, status='paused')
        except Exception as e:
            logger.error(f"Error in message-based invite task {task_id}: {e}", exc_info=True)
            await self.db.update_invite_task(
                task_id,
                status='failed',
                error_message=str(e)
            )
        finally:
            self._stop_flags.pop(task_id, None)
            self.running_tasks.pop(task_id, None)

    
    async def _rotate_session(self, task: InviteTask) -> Optional[str]:
        """
        Rotate to next available session.
        Checks for capability (group access) and reports detailed errors if rotation fails.
        """
        logger.warning(f"🔄 SESSION ROTATION: Task {task.id} - Starting rotation from session '{task.session_alias}'")
        logger.info(f"🔄 SESSION ROTATION: Task {task.id} - Available sessions: {task.available_sessions}")
        logger.info(f"🔄 SESSION ROTATION: Task {task.id} - Groups: source={task.source_group_id}, target={task.target_group_id}")

        if not task.available_sessions:
            logger.error(f"🔄 SESSION ROTATION: Task {task.id} - FAILED: No available sessions for rotation")
            return None

        if len(task.available_sessions) == 1:
            logger.warning(f"🔄 SESSION ROTATION: Task {task.id} - Only one session available, cannot rotate")
            return None

        current_index = -1
        try:
            current_index = task.available_sessions.index(task.session_alias)
            logger.info(f"🔄 SESSION ROTATION: Task {task.id} - Current session index: {current_index}")
        except ValueError:
            logger.warning(f"🔄 SESSION ROTATION: Task {task.id} - Current session '{task.session_alias}' not found in available list")
            pass  # Current session might not be in the list anymore

        # We will collect errors for all candidates to report if rotation fails completely
        rotation_errors = []
        checked_sessions = []

        # Try next sessions
        for i in range(len(task.available_sessions)):
            next_index = (current_index + 1 + i) % len(task.available_sessions)
            candidate_alias = task.available_sessions[next_index]

            if candidate_alias == task.session_alias and len(task.available_sessions) > 1:
                logger.debug(f"🔄 SESSION ROTATION: Task {task.id} - Skipping current session '{candidate_alias}'")
                continue

            logger.info(f"🔄 SESSION ROTATION: Task {task.id} - Checking candidate session '{candidate_alias}'")
            checked_sessions.append(candidate_alias)

            # Validate capability using the new granular check
            validation = await self.session_manager.validate_session_capability(
                candidate_alias,
                task.source_group_id,
                task.target_group_id,
                source_username=task.source_username,
                target_username=task.target_username
            )

            if validation.get('success'):
                # Success! Rotate to this session
                await self.db.update_invite_task(task.id, session_alias=candidate_alias)
                logger.warning(f"✅ SESSION ROTATION SUCCESS: Task {task.id} - Rotated from '{task.session_alias}' to '{candidate_alias}'")
                logger.info(f"✅ SESSION ROTATION SUCCESS: Task {task.id} - Session '{candidate_alias}' validated and ready")
                return candidate_alias
            else:
                # Failed, record reason
                reason = validation.get('reason', 'Unknown error')
                rotation_errors.append(f"{candidate_alias}: {reason}")
                logger.warning(f"❌ SESSION ROTATION: Task {task.id} - Candidate '{candidate_alias}' rejected: {reason}")

        # If we get here, no suitable session was found
        logger.error(f"🚫 SESSION ROTATION FAILED: Task {task.id} - No suitable sessions found")
        logger.error(f"🚫 SESSION ROTATION FAILED: Task {task.id} - Checked sessions: {checked_sessions}")
        logger.error(f"🚫 SESSION ROTATION FAILED: Task {task.id} - Total candidates: {len(task.available_sessions)}, suitable: 0")

        # We should update the task with a meaningful error message
        error_summary = " | ".join(rotation_errors[:3]) # Limit length
        if len(rotation_errors) > 3:
            error_summary += "..."

        full_error = f"Rotation failed. Candidates unavailable: {error_summary}"

        logger.error(f"🚫 SESSION ROTATION FINAL: Task {task.id} - {full_error}")
        # Note: We don't stop the task here, the caller (inviter loop) handles the stop
        # based on the None return value. But we can update the task error message now
        # so it's ready when the task stops.
        await self.db.update_invite_task(task.id, error_message=full_error)

        return None
    
    async def get_task_status(self, task_id: int) -> Dict[str, Any]:
        """Get current status of a task."""
        task = await self.db.get_invite_task(task_id)
        if not task:
            return {"success": False, "error": "Task not found"}
        
        return {
            "success": True,
            "task_id": task.id,
            "status": task.status,
            "source_group": task.source_group_title,
            "target_group": task.target_group_title,
            "session": task.session_alias,
            "invited_count": task.invited_count,
            "limit": task.limit,
            "delay_seconds": task.delay_seconds,
            "delay_every": task.delay_every,
            "rotate_sessions": task.rotate_sessions,
            "rotate_every": task.rotate_every,
            "error_message": task.error_message,
            "created_at": task.created_at,
            "updated_at": task.updated_at
        }
    
    async def get_all_running_tasks(self) -> list:
        """Get all running tasks."""
        tasks = await self.db.get_running_tasks()
        return [await self.get_task_status(t.id) for t in tasks]
