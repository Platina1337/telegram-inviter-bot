# -*- coding: utf-8 -*-
"""
Inviter Worker - handles the actual invite logic.
"""
import asyncio
import logging
import random
import httpx
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

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
        
        # Логируем информацию о прокси
        proxy_info = await self.session_manager.get_proxy_info(task.session_alias, task.use_proxy)
        proxy_str = f" через прокси {proxy_info}" if proxy_info else " без прокси"
        
        # Start the task in background - choose method based on invite_mode
        if task.invite_mode == 'message_based':
            self.running_tasks[task_id] = asyncio.create_task(
                self._run_message_based_invite_task(task_id)
            )
            logger.info(f"Запущена задача инвайтинга по сообщениям {task_id} (сессия: {task.session_alias}{proxy_str})")
        elif task.invite_mode == 'from_file':
            self.running_tasks[task_id] = asyncio.create_task(
                self._run_from_file_invite_task(task_id)
            )
            logger.info(f"Запущена задача инвайтинга из файла {task_id} (файл: {task.file_source}, сессия: {task.session_alias}{proxy_str})")
        else:
            self.running_tasks[task_id] = asyncio.create_task(
                self._run_invite_task(task_id)
            )
            logger.info(f"Запущена задача инвайтинга по списку участников {task_id} (сессия: {task.session_alias}{proxy_str})")
        
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
        logger.info(f"Остановлена задача инвайтинга {task_id}")
        return {"success": True, "task_id": task_id, "status": "paused"}
    
    async def _notify_user(self, user_id: int, message: str):
        """Send notification to user via Telegram Bot API."""
        from .config import config
        if not config.BOT_TOKEN:
            logger.warning("Не удалось отправить уведомление: BOT_TOKEN не установлен")
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
            logger.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")

    def _format_user_info(self, user_id: int, username: Optional[str] = None, first_name: Optional[str] = None) -> str:
        """Format user information for logging."""
        parts = []
        if first_name:
            parts.append(first_name)
        if username:
            parts.append(f"@{username}")
        parts.append(f"(ID: {user_id})")
        return " ".join(parts)
    
    async def _should_skip_user(self, task: InviteTask, client: Any, user_id: int, last_online: Optional[datetime], username: Optional[str] = None, first_name: Optional[str] = None) -> Optional[str]:
        """Check if a user should be skipped based on task filter settings."""
        user_info = self._format_user_info(user_id, username, first_name)
        logger.info(f"🔍 [FILTER] Задача {task.id}: Начало проверки фильтрации для пользователя {user_info}")
        logger.info(f"🔍 [FILTER] Задача {task.id}: Режим фильтрации: {task.filter_mode}, Порог неактивности: {task.inactive_threshold_days} дней")
        
        if task.filter_mode == "all":
            logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - режим 'all', фильтрация отключена. Пользователь подходит.")
            return None

        skip_reason = None

        # Check for admin status
        if task.filter_mode in ["exclude_admins", "exclude_admins_and_inactive"]:
            logger.info(f"🔍 [FILTER] Задача {task.id}: Проверка администраторского статуса пользователя {user_info} в исходной группе {task.source_group_id}")
            try:
                source_member = await client.get_chat_member(task.source_group_id, user_id)
                raw_status = source_member.status
                status = getattr(raw_status, "name", str(raw_status)).upper()
                logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} имеет статус '{status}' в исходной группе")
                
                if status in ['ADMINISTRATOR', 'CREATOR', 'OWNER']:
                    logger.warning(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} является администратором/владельцем в исходной группе (статус: {status}). ПРОПУСК по фильтру админов.")
                    skip_reason = "admin_in_source"
                else:
                    logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} не является администратором в исходной группе (статус: {status}). Проверка админа пройдена.")
            except Exception as e:
                # If we can't get member status, assume not admin or not in group.
                # Log a debug message, but don't stop the invite process.
                logger.warning(f"🔍 [FILTER] Задача {task.id}: Не удалось проверить статус администратора для пользователя {user_info} в исходной группе {task.source_group_id}: {e}. Считаем, что пользователь не админ.")
                logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - проверка админа не удалась, но продолжаем (считаем не админом).")

        # Check for inactivity
        if task.filter_mode in ["exclude_inactive", "exclude_admins_and_inactive"] and task.inactive_threshold_days is not None:
            logger.info(f"🔍 [FILTER] Задача {task.id}: Проверка неактивности пользователя {user_info}. Порог: {task.inactive_threshold_days} дней")
            
            if last_online is None:
                # Если дата последней активности недоступна, считаем пользователя активным
                # (не пропускаем его из-за неактивности)
                logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - last_online_date недоступна. Считаем активным (не пропускаем).")
                if skip_reason:
                    logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - ФИНАЛЬНОЕ РЕШЕНИЕ: ПРОПУСК по причине '{skip_reason}' (неактивность неизвестна, но есть другая причина пропуска)")
                    return skip_reason
                else:
                    logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - ФИНАЛЬНОЕ РЕШЕНИЕ: ПОДХОДИТ (неактивность неизвестна, но других причин для пропуска нет)")
                    return None
            
            # Convert last_online to datetime object if it's a string
            if isinstance(last_online, str):
                try:
                    last_online = datetime.fromisoformat(last_online)
                    logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - last_online_date преобразована из строки: {last_online}")
                except ValueError:
                    logger.warning(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - не удалось распарсить last_online timestamp: {last_online}. Считаем активным.")
                    if skip_reason:
                        logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - ФИНАЛЬНОЕ РЕШЕНИЕ: ПРОПУСК по причине '{skip_reason}' (неактивность не определена, но есть другая причина пропуска)")
                        return skip_reason
                    else:
                        logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - ФИНАЛЬНОЕ РЕШЕНИЕ: ПОДХОДИТ (неактивность не определена, но других причин для пропуска нет)")
                        return None

            # Log last online info
            logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - последний раз был онлайн: {last_online}")
            
            # Compare with threshold
            threshold_date = datetime.now() - timedelta(days=task.inactive_threshold_days)
            days_since_online = (datetime.now() - last_online).days
            logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - порог неактивности: {task.inactive_threshold_days} дней (дата порога: {threshold_date})")
            logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - дней с последнего онлайна: {days_since_online}")
            
            if last_online < threshold_date:
                logger.warning(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - НЕАКТИВЕН ({days_since_online} дней > {task.inactive_threshold_days} дней). ПРОПУСК по фильтру неактивности.")
                if skip_reason:
                    logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - ФИНАЛЬНОЕ РЕШЕНИЕ: ПРОПУСК по причине '{skip_reason}' и неактивности")
                    return skip_reason
                else:
                    return "inactive"
            else:
                logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - АКТИВЕН ({days_since_online} дней <= {task.inactive_threshold_days} дней). Проверка неактивности пройдена.")

        # Final decision
        if skip_reason:
            logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - ФИНАЛЬНОЕ РЕШЕНИЕ: ПРОПУСК по причине '{skip_reason}'")
            return skip_reason
        else:
            logger.info(f"🔍 [FILTER] Задача {task.id}: Пользователь {user_info} - ФИНАЛЬНОЕ РЕШЕНИЕ: ПОДХОДИТ (все проверки пройдены)")
            return None

    async def _run_invite_task(self, task_id: int):
        """Main invite logic."""
        session_consecutive_invites = 0
        try:
            while not self._stop_flags.get(task_id, False):
                task = await self.db.get_invite_task(task_id)
                if not task:
                    logger.error(f"Задача {task_id} не найдена")
                    break
                
                # Check if limit reached
                if task.limit and task.invited_count >= task.limit:
                    logger.info(f"Задача {task_id} достигла лимита: {task.invited_count}/{task.limit}")
                    # Не меняем статус здесь - он будет установлен в конце с уведомлением
                    break
                
                # Log rotation settings at start of each iteration
                logger.debug(f"Задача {task_id}: rotate_sessions={task.rotate_sessions}, rotate_every={task.rotate_every}, session_consecutive_invites={session_consecutive_invites}")

                # Get session client
                proxy_info = await self.session_manager.get_proxy_info(task.session_alias, task.use_proxy)
                proxy_str = f" через прокси {proxy_info}" if proxy_info else " без прокси"
                
                client = await self.session_manager.get_client(task.session_alias, use_proxy=task.use_proxy)
                if not client:
                    logger.error(f"Не удалось получить клиент для сессии {task.session_alias}{proxy_str}")

                    # Try to rotate session if enabled (auto-rotate on failure)
                    if task.rotate_sessions and task.available_sessions:
                        logger.info(f"Сессия {task.session_alias} недоступна, попытка ротации...")
                        new_session = await self._rotate_session(task)
                        if new_session:
                            session_consecutive_invites = 0
                            continue

                    # If rotation failed or disabled, stop task
                    error_msg = f"Сессия {task.session_alias} недоступна"
                    if task.rotate_sessions:
                        error_msg += " (Ротация не удалась: нет подходящих сессий)"

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
                    username=task.source_username,
                    use_proxy=task.use_proxy
                )
                
                if members is None:
                    logger.error(f"Задача {task_id}: Не удалось получить участников с сессией {task.session_alias}{proxy_str}")
                    
                    if task.rotate_sessions and task.available_sessions:
                        logger.info(f"Попытка ротации из-за ошибки получения участников...")
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
                    source_info = await self.session_manager.check_group_access(task.session_alias, task.source_group_id, use_proxy=task.use_proxy)
                    has_access = source_info.get('has_access', False)
                    total_in_group = source_info.get('members_count')

                    if not has_access:
                        logger.warning(f"Задача {task_id}: Сессия {task.session_alias} не имеет доступа к группе-источнику")
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
                        logger.warning(f"Задача {task_id}: Сессия {task.session_alias} не видит участников, количество участников неизвестно")
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
                            f"Задача {task_id}: Сессия {task.session_alias} видит 0 участников на offset {task.current_offset}, "
                            f"но в группе ~{total_in_group}. Сессия считается 'слепой'."
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
                    logger.info(f"Задача {task_id}: Больше нет участников для приглашения (проверено)")
                    # Не меняем статус здесь - он будет установлен в конце с уведомлением
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
                    
                    # Try to get last_online_date for inactivity filter
                    # In "member_list" mode, last_online_date is not available from get_group_members
                    # We need to fetch it separately using get_users
                    user_last_online = None
                    user_username = member.get('username')
                    user_first_name = member.get('first_name')
                    user_info = self._format_user_info(user_id, user_username, user_first_name)
                    
                    if task.filter_mode in ["exclude_inactive", "exclude_admins_and_inactive"] and task.inactive_threshold_days is not None:
                        logger.info(f"Задача {task_id}: Попытка получить last_online_date для пользователя {user_info} через get_users")
                        try:
                            users = await client.get_users([user_id])
                            if users and len(users) > 0:
                                user_obj = users[0]
                                user_last_online = getattr(user_obj, 'last_online_date', None)
                                if user_last_online is not None:
                                    logger.info(f"Задача {task_id}: Получена last_online_date для пользователя {user_info}: {user_last_online}")
                                else:
                                    logger.info(f"Задача {task_id}: last_online_date недоступна для пользователя {user_info} (не в контактах или скрыта)")
                            else:
                                logger.warning(f"Задача {task_id}: Не удалось получить информацию о пользователе {user_info} через get_users")
                        except Exception as e:
                            logger.warning(f"Задача {task_id}: Ошибка при получении last_online_date для пользователя {user_info}: {e}")
                            user_last_online = None
                    
                    # New filter logic
                    logger.info(f"Задача {task_id}: Запуск проверки фильтрации для пользователя {user_info} (режим: по списку участников)")
                    skip_reason = await self._should_skip_user(task, client, user_id, user_last_online, user_username, user_first_name)
                    if skip_reason:
                        logger.warning(f"Задача {task_id}: Пользователь {user_info} ПРОПУЩЕН по причине фильтрации: {skip_reason}")
                        await self.db.add_invite_record(
                            task_id, user_id,
                            username=user_username,
                            first_name=user_first_name,
                            status='skipped_by_filter',
                            error_message=skip_reason
                        )
                        continue
                    else:
                        logger.info(f"Задача {task_id}: Пользователь {user_info} ПРОШЕЛ проверку фильтрации, будет приглашен")
                    
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
                            logger.info(f"Задача {task_id}: Пользователь {user_info} имеет статус {status} в целевой группе {task.target_group_id}. Пропуск.")
                            
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
                        logger.info(f"Задача {task_id} достигла лимита: {task.invited_count}/{task.limit}")
                        # Не меняем статус здесь - он будет установлен в конце с уведомлением
                        break
                    
                    # Check session rotation limit
                    if task.rotate_sessions and task.rotate_every > 0 and session_consecutive_invites >= task.rotate_every:
                        logger.info(f"Задача {task_id}: Сессия {task.session_alias} достигла {session_consecutive_invites} приглашений. Ротация...")
                        
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
                        else:
                            # Ротация не удалась (нет доступных сессий или они не прошли валидацию)
                            # Сбрасываем счетчик, чтобы не пытаться ротировать на каждом инвайте
                            # и продолжаем работу с текущей сессией
                            logger.warning(f"Задача {task_id}: Ротация по счетчику не удалась. Продолжаем с текущей сессией {task.session_alias}")
                            session_consecutive_invites = 0  # Сбрасываем счетчик, чтобы не пытаться ротировать постоянно
                    
                    # Invite the user
                    result = await self.session_manager.invite_user(
                        task.session_alias,
                        task.target_group_id,
                        user_id,
                        target_username=task.target_username,
                        use_proxy=task.use_proxy
                    )
                    
                    if result.get('success'):
                        # Record success
                        await self.db.add_invite_record(
                            task_id, user_id,
                            username=user_username,
                            first_name=user_first_name,
                            status='success'
                        )
                        await self.db.update_invite_task(
                            task_id,
                            invited_count=task.invited_count + 1
                        )
                        task.invited_count += 1
                        session_consecutive_invites += 1
                        invited_in_batch += 1
                        logger.info(f"Задача {task_id}: Приглашен пользователь {user_info} ({task.invited_count}/{task.limit or '∞'}) (сессия: {task.session_alias}{proxy_str})")
                        
                        # Delay based on frequency and randomization
                        if task.invited_count % task.delay_every == 0:
                            # Add randomization: 80% to 120% of the base delay
                            min_delay = max(1, int(task.delay_seconds * 0.8))
                            max_delay = int(task.delay_seconds * 1.2)
                            actual_delay = random.randint(min_delay, max_delay)
                            
                            logger.info(f"Задача {task_id}: Ожидание {actual_delay}с после {task.delay_every} приглашений (базовая задержка: {task.delay_seconds}с)")
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
                        # Fatal error - mark session as failed and try to rotate
                        error_detail = result.get('error', 'Unknown error')
                        logger.error(f"Критическая ошибка с сессией {task.session_alias}{proxy_str}: {error_detail}")
                        
                        current_failing = task.session_alias
                        
                        # Добавляем сессию в список проблемных для этой задачи
                        if current_failing not in task.failed_sessions:
                            task.failed_sessions.append(current_failing)
                            await self.db.update_invite_task(task_id, failed_sessions=task.failed_sessions)
                            logger.info(f"Сессия {current_failing} добавлена в список проблемных для задачи {task_id}")
                        
                        notify_text = (
                            f"⚠️ **Проблема с сессией `{current_failing}`**\n\n"
                            f"Группа: {task.target_group_title}\n"
                            f"Ошибка: `{error_detail}`\n"
                        )
                        
                        # Если ротация включена, пробуем другую сессию
                        if task.rotate_sessions:
                            if task.available_sessions:
                                logger.info(f"Критическая ошибка на {current_failing}, попытка ротации...")
                                
                                notify_text += f"🔄 Сессия `{current_failing}` исключена из задачи. Пробую другую..."
                                await self._notify_user(task.user_id, notify_text)
                                
                                # IMPORTANT: Don't skip this user! Decrement processed count
                                processed_in_batch -= 1
                                
                                new_session = await self._rotate_session(task)
                                if new_session:
                                    logger.info(f"Успешно переключено на сессию {new_session}, продолжаем задачу")
                                    session_consecutive_invites = 0
                                    break
                                else:
                                    # Ротация не удалась - проверяем, есть ли еще доступные сессии
                                    # Перезагружаем задачу из БД, чтобы получить актуальный список
                                    task = await self.db.get_invite_task(task_id)
                                    if not task:
                                        logger.warning(f"Задача {task_id} не найдена после неудачной ротации")
                                        break
                                    
                                    available_count = len([s for s in task.available_sessions if s and s not in task.failed_sessions])
                                    logger.info(f"Задача {task_id}: После неудачной ротации. Доступных сессий: {available_count}, failed_sessions: {task.failed_sessions}, available_sessions: {task.available_sessions}")
                                    
                                    if available_count == 0:
                                        error_msg = f"Критическая ошибка: {error_detail}. Все сессии исключены из задачи."
                                        logger.error(f"Задача {task_id}: Остановка задачи - нет доступных сессий")
                                        await self.db.update_invite_task(
                                            task_id,
                                            status='failed',
                                            error_message=error_msg
                                        )
                                        notify_text += f"\n❌ Задача остановлена: нет доступных сессий."
                                        await self._notify_user(task.user_id, notify_text)
                                        return
                                    else:
                                        # Есть доступные сессии, но ротация не удалась (возможно, они не прошли валидацию)
                                        # Останавливаем задачу с информативным сообщением
                                        error_msg = f"Критическая ошибка: {error_detail}. Ротация не удалась: доступные сессии не прошли валидацию."
                                        logger.error(f"Задача {task_id}: Остановка задачи - доступные сессии не могут выполнить задачу. available_count={available_count}")
                                        await self.db.update_invite_task(
                                            task_id,
                                            status='failed',
                                            error_message=error_msg
                                        )
                                        notify_text += f"\n❌ Задача остановлена: доступные сессии не могут выполнить задачу."
                                        await self._notify_user(task.user_id, notify_text)
                                        return
                            else:
                                # Ротация включена, но список доступных сессий пуст
                                error_msg = f"Критическая ошибка: {error_detail}. Список доступных сессий пуст."
                                await self.db.update_invite_task(task_id, status='failed', error_message=error_msg)
                                notify_text += f"\n❌ Задача остановлена: не на чем продолжать."
                                await self._notify_user(task.user_id, notify_text)
                                return
                        
                        # Если ротация выключена, останавливаем задачу
                        else:
                            error_msg = f"Критическая ошибка: {error_detail}"
                            await self.db.update_invite_task(
                                task_id,
                                status='failed',
                                error_message=error_msg
                            )
                            notify_text += f"\n❌ Задача остановлена. Включите ротацию сессий для автоматического переключения."
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
            if task:
                # Если задача все еще в статусе running, значит она завершилась успешно
                if task.status == 'running':
                    logger.info(f"🔄 Задача {task_id} завершена корректно")
                    await self.db.update_invite_task(task_id, status='completed')

                    await self._notify_user(
                        task.user_id,
                        f"✅ **Задача инвайтинга завершена**\n\n"
                        f"📊 **Итоговые результаты:**\n"
                        f"• Группа-источник: {task.source_group_title}\n"
                        f"• Группа-цель: {task.target_group_title}\n"
                        f"• Всего добавлено: {task.invited_count} участников\n"
                        f"• Лимит: {task.limit or 'Неограничен'}\n\n"
                        f"🎯 **Задача выполнена успешно!**"
                    )
                elif task.status == 'completed':
                    # Задача уже помечена как завершенная, но уведомление могло не отправиться
                    # Отправляем уведомление, если оно еще не было отправлено
                    logger.info(f"📢 Задача {task_id} уже завершена, отправляем итоговое уведомление")
                    await self._notify_user(
                        task.user_id,
                        f"✅ **Задача инвайтинга завершена**\n\n"
                        f"📊 **Итоговые результаты:**\n"
                        f"• Группа-источник: {task.source_group_title}\n"
                        f"• Группа-цель: {task.target_group_title}\n"
                        f"• Всего добавлено: {task.invited_count} участников\n"
                        f"• Лимит: {task.limit or 'Неограничен'}\n\n"
                        f"🎯 **Задача выполнена успешно!**"
                    )
        
        except asyncio.CancelledError:
            logger.info(f"Задача {task_id} была отменена")
        except Exception as e:
            logger.error(f"Ошибка в задаче инвайтинга {task_id}: {e}", exc_info=True)
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
                logger.error(f"Задача {task_id} не найдена")
                return

            logger.info("=" * 80)
            logger.info(f"🚀 НАЧАЛО ИСПОЛНЕНИЯ ЗАДАЧИ {task_id}")
            logger.info("=" * 80)
            logger.info(f"📊 ОСНОВНЫЕ НАСТРОЙКИ:")
            logger.info(f"   Режим инвайтинга: по сообщениям")
            logger.info(f"   Исходная группа: {task.source_group_title} (ID: {task.source_group_id}, username: {task.source_username or 'не указан'})")
            logger.info(f"   Целевая группа: {task.target_group_title} (ID: {task.target_group_id}, username: {task.target_username or 'не указан'})")
            logger.info(f"   Текущая сессия: {task.session_alias}")
            logger.info(f"   Доступные сессии: {task.available_sessions}")
            logger.info(f"   Проблемные сессии: {task.failed_sessions}")

            logger.info(f"⚙️ ТЕХНИЧЕСКИЕ НАСТРОЙКИ:")
            logger.info(f"   Ротация сессий: {'включена' if task.rotate_sessions else 'выключена'}")
            logger.info(f"   Ротация после N инвайтов: {task.rotate_every if task.rotate_every > 0 else 'только при ошибках'}")
            logger.info(f"   Использование прокси: {'включено' if task.use_proxy else 'выключено'}")
            logger.info(f"   Задержка между инвайтами: {task.delay_seconds} сек")
            logger.info(f"   Применять задержку каждые: {task.delay_every} инвайтов")
            logger.info(f"   Лимит инвайтов: {task.limit if task.limit else 'не ограничен'}")
            logger.info(f"   Текущее количество инвайтов: {task.invited_count}")

            logger.info(f"🎯 ПЛАН РАБОТЫ:")
            logger.info(f"   1. Подключение к исходной группе для чтения истории сообщений")
            logger.info(f"   2. Извлечение авторов сообщений из истории чата")
            logger.info(f"   3. Проверка статуса пользователей в целевой группе")
            logger.info(f"   4. Приглашение пользователей с обработкой ошибок и ротацией сессий")
            logger.info(f"   5. Ожидание между инвайтами и ротация сессий по необходимости")
            logger.info("=" * 80)
            
            # Validate initial session capability
            proxy_info = await self.session_manager.get_proxy_info(task.session_alias, task.use_proxy)
            proxy_str = f" через прокси {proxy_info}" if proxy_info else " без прокси"
            logger.info(f"Задача {task_id}: Проверка возможностей сессии {task.session_alias}{proxy_str}...")
            validation = await self.session_manager.validate_session_capability(
                task.session_alias,
                task.source_group_id,
                task.target_group_id,
                source_username=task.source_username,
                target_username=task.target_username,
                use_proxy=task.use_proxy
            )
            
            if not validation.get('success'):
                logger.warning(f"Задача {task_id}: Проверка начальной сессии {task.session_alias}{proxy_str} не прошла: {validation.get('reason')}")
                
                # Try to rotate if enabled
                if task.rotate_sessions and task.available_sessions and len(task.available_sessions) > 1:
                    logger.info(f"Задача {task_id}: Попытка найти подходящую сессию...")
                    new_session = await self._rotate_session(task)
                    if new_session:
                        logger.info(f"Задача {task_id}: Ротация на сессию {new_session}")
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
            proxy_info = await self.session_manager.get_proxy_info(task.session_alias, task.use_proxy)
            proxy_str = f" через прокси {proxy_info}" if proxy_info else " без прокси"
            
            client = await self.session_manager.get_client(task.session_alias, use_proxy=task.use_proxy)
            if not client:
                logger.error(f"Не удалось получить клиент для сессии {task.session_alias}{proxy_str}")
                
                # Try to rotate if enabled
                if task.rotate_sessions and task.available_sessions:
                    logger.info(f"Задача {task_id}: Клиент недоступен, попытка ротации...")
                    new_session = await self._rotate_session(task)
                    if new_session:
                        client = await self.session_manager.get_client(new_session, use_proxy=task.use_proxy)
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
            
            logger.info(f"Задача {task_id}: Запуск инвайтинга по сообщениям. Уже приглашено: {len(invited_ids)} пользователей (сессия: {task.session_alias}{proxy_str})")
            
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
                f"Прокси: {'Да' if proxy_info else 'Нет'}\n"
                f"Лимит: {task.limit or '∞'}"
            )
            
            # Track unique users we've seen
            seen_users = set()
            processed_messages = 0
            
            # Iterate through chat history
            logger.info(f"Задача {task_id}: Начало итерации по истории чата (сессия: {task.session_alias}{proxy_str})")
            
            try:
                async for message in client.get_chat_history(task.source_group_id):
                    # Check stop flag
                    if self._stop_flags.get(task_id, False):
                        logger.info(f"Задача {task_id}: Установлен флаг остановки, прерывание")
                        break
                    
                    # Reload task to get fresh data
                    task = await self.db.get_invite_task(task_id)
                    if not task:
                        break
                    
                    # Check if limit reached
                    if task.limit and task.invited_count >= task.limit:
                        logger.info(f"Задача {task_id} достигла лимита: {task.invited_count}/{task.limit}")
                        await self.db.update_invite_task(task_id, status='completed')
                        break
                    
                    processed_messages += 1
                    
                    # Get message author
                    user = message.from_user
                    if not user or user.is_bot:
                        continue
                    
                    user_id = user.id
                    user_username = user.username
                    user_first_name = user.first_name
                    user_info = self._format_user_info(user_id, user_username, user_first_name)
                    
                    # Получаем last_online_date, если доступно
                    user_last_online = getattr(user, 'last_online_date', None)
                    
                    if user_last_online is not None:
                        logger.info(f"Задача {task_id}: Пользователь {user_info} - найден last_online_date: {user_last_online} (тип: {type(user_last_online).__name__})")
                    else:
                        logger.info(f"Задача {task_id}: Пользователь {user_info} - last_online_date недоступна (None)")
                    
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
                        logger.debug(f"Задача {task_id}: Пользователь {user_info} уже приглашен, пропуск")
                        continue
                    
                    # New filter logic
                    logger.info(f"Задача {task_id}: Запуск проверки фильтрации для пользователя {user_info} (режим: по сообщениям)")
                    skip_reason = await self._should_skip_user(task, client, user_id, user_last_online, user_username, user_first_name)
                    if skip_reason:
                        logger.warning(f"Задача {task_id}: Пользователь {user_info} ПРОПУЩЕН по причине фильтрации: {skip_reason}")
                        await self.db.add_invite_record(
                            task_id, user_id,
                            username=user_username,
                            first_name=user_first_name,
                            status='skipped_by_filter',
                            error_message=skip_reason
                        )
                        continue
                    else:
                        logger.info(f"Задача {task_id}: Пользователь {user_info} ПРОШЕЛ проверку фильтрации, будет приглашен")
                    
                    # PRE-CHECK: Check if user is ALREADY in target group
                    try:
                        current_client = await self.session_manager.get_client(task.session_alias, use_proxy=task.use_proxy)
                        if not current_client:
                            logger.warning(f"Задача {task_id}: Клиент недоступен для предварительной проверки")
                            continue
                        
                        target_member = await current_client.get_chat_member(task.target_group_id, user_id)
                        
                        # Get status safely
                        raw_status = target_member.status
                        status = getattr(raw_status, "name", str(raw_status)).upper()
                        
                        # Skip if user is already in target (not LEFT)
                        if status != 'LEFT':
                            logger.info(f"Задача {task_id}: Пользователь {user_info} имеет статус {status} в целевой группе. Пропуск.")
                            
                            status_code = 'banned_in_target' if status in ['KICKED', 'BANNED'] else 'already_in_target'
                            
                            await self.db.add_invite_record(
                                task_id, user_id,
                                username=user_username,
                                first_name=user_first_name,
                                status=status_code
                            )
                            await asyncio.sleep(0.1)
                            continue
                    
                    except Exception:
                        # User not in group, proceed to invite
                        pass
                    
                    # Check session rotation limit
                    if task.rotate_sessions and task.rotate_every > 0 and session_consecutive_invites >= task.rotate_every:
                        logger.info(f"Задача {task_id}: Сессия {task.session_alias} достигла {session_consecutive_invites} приглашений. Ротация...")
                        
                        new_session = await self._rotate_session(task)
                        if new_session:
                            session_consecutive_invites = 0
                            # Выходим из цикла обработки сообщений, чтобы перезагрузить задачу и создать новый итератор с новой сессией
                            break
                        else:
                            # Ротация не удалась (нет доступных сессий или они не прошли валидацию)
                            # Сбрасываем счетчик, чтобы не пытаться ротировать на каждом инвайте
                            # и продолжаем работу с текущей сессией
                            logger.warning(f"Задача {task_id}: Ротация по счетчику не удалась. Продолжаем с текущей сессией {task.session_alias}")
                            session_consecutive_invites = 0  # Сбрасываем счетчик, чтобы не пытаться ротировать постоянно
                    
                    # Invite the user
                    result = await self.session_manager.invite_user(
                        task.session_alias,
                        task.target_group_id,
                        user_id,
                        target_username=task.target_username,
                        use_proxy=task.use_proxy
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
                        logger.info(f"Задача {task_id}: Приглашен пользователь {user_info} ({task.invited_count}/{task.limit or '∞'}) (сессия: {task.session_alias}{proxy_str})")
                        
                        # Delay based on frequency
                        if task.invited_count % task.delay_every == 0:
                            min_delay = max(1, int(task.delay_seconds * 0.8))
                            max_delay = int(task.delay_seconds * 1.2)
                            actual_delay = random.randint(min_delay, max_delay)
                            
                            logger.info(f"Задача {task_id}: Ожидание {actual_delay}с после {task.delay_every} приглашений")
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
                        # Fatal error - mark session as failed and try to rotate
                        error_detail = result.get('error', 'Unknown error')
                        logger.error(f"Критическая ошибка с сессией {task.session_alias}{proxy_str}: {error_detail}")
                        
                        current_failing = task.session_alias
                        
                        # Добавляем сессию в список проблемных для этой задачи
                        if current_failing not in task.failed_sessions:
                            task.failed_sessions.append(current_failing)
                            await self.db.update_invite_task(task_id, failed_sessions=task.failed_sessions)
                            logger.info(f"Сессия {current_failing} добавлена в список проблемных для задачи {task_id}")
                        
                        notify_text = (
                            f"⚠️ **Проблема с сессией `{current_failing}`**\n\n"
                            f"Группа: {task.target_group_title}\n"
                            f"Ошибка: `{error_detail}`\n"
                        )
                        
                        # Если ротация включена, пробуем другую сессию
                        if task.rotate_sessions:
                            if task.available_sessions:
                                logger.info(f"Критическая ошибка на {current_failing}, попытка ротации...")
                                
                                notify_text += f"🔄 Сессия `{current_failing}` исключена из задачи. Пробую другую..."
                                await self._notify_user(task.user_id, notify_text)
                                
                                new_session = await self._rotate_session(task)
                                if new_session:
                                    logger.info(f"Task {task_id}: Successfully rotated to {new_session}, continuing...")
                                    session_consecutive_invites = 0
                                    # Выходим из цикла обработки сообщений, чтобы перезагрузить задачу и создать новый итератор с новой сессией
                                    break
                                else:
                                    # Ротация не удалась - перезагружаем задачу и проверяем доступные сессии
                                    task = await self.db.get_invite_task(task_id)
                                    if not task:
                                        break
                                    
                                    available_count = len([s for s in task.available_sessions if s and s not in task.failed_sessions])
                                    if available_count == 0:
                                        error_msg = f"Критическая ошибка: {error_detail}. Все сессии исключены из задачи."
                                        await self.db.update_invite_task(
                                            task_id,
                                            status='failed',
                                            error_message=error_msg
                                        )
                                        notify_text += f"\n❌ Задача остановлена: нет доступных сессий."
                                        await self._notify_user(task.user_id, notify_text)
                                        return
                                    else:
                                        # Есть доступные сессии, но ротация не удалась (не прошли валидацию)
                                        error_msg = f"Критическая ошибка: {error_detail}. Ротация не удалась: доступные сессии не прошли валидацию."
                                        await self.db.update_invite_task(
                                            task_id,
                                            status='failed',
                                            error_message=error_msg
                                        )
                                        notify_text += f"\n❌ Задача остановлена: доступные сессии не могут выполнить задачу."
                                        await self._notify_user(task.user_id, notify_text)
                                        return
                            else:
                                # Ротация включена, но список доступных сессий пуст
                                error_msg = f"Критическая ошибка: {error_detail}. Список доступных сессий пуст."
                                await self.db.update_invite_task(task_id, status='failed', error_message=error_msg)
                                notify_text += f"\n❌ Задача остановлена: не на чем продолжать."
                                await self._notify_user(task.user_id, notify_text)
                                return
                        
                        # Если ротация выключена, останавливаем задачу
                        else:
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
                            return
                    
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
                
                logger.info(f"Задача {task_id}: Завершена обработка истории чата. Обработано {processed_messages} сообщений, найдено {len(seen_users)} уникальных пользователей (сессия: {task.session_alias}{proxy_str})")
                
            except Exception as e:
                logger.error(f"Задача {task_id}: Ошибка при итерации по истории чата: {e}", exc_info=True)
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
                    f"📊 **Результаты:**\n"
                    f"• Группа-источник: {task.source_group_title}\n"
                    f"• Группа-цель: {task.target_group_title}\n"
                    f"• Обработано сообщений: {processed_messages}\n"
                    f"• Найдено уникальных пользователей: {len(seen_users)}\n"
                    f"• Добавлено участников: {task.invited_count}/{task.limit or 'Неограничен'}\n\n"
                    f"🎉 **Задача выполнена успешно!**"
                )
        
        except asyncio.CancelledError:
            logger.info(f"⏹️ Задача {task_id} была отменена пользователем")
            task = await self.db.get_invite_task(task_id)
            if task and task.status == 'running':
                await self.db.update_invite_task(task_id, status='paused')

                await self._notify_user(
                    task.user_id,
                    f"⏹️ **Задача инвайтинга остановлена**\n\n"
                    f"📊 **Текущий прогресс:**\n"
                    f"• Группа-источник: {task.source_group_title}\n"
                    f"• Группа-цель: {task.target_group_title}\n"
                    f"• Добавлено участников: {task.invited_count}\n"
                    f"• Лимит: {task.limit or 'Неограничен'}\n\n"
                    f"ℹ️ **Статус:** Остановлено пользователем"
                )
        except Exception as e:
            logger.error(f"💥 Критическая ошибка в задаче {task_id}: {e}", exc_info=True)
            await self.db.update_invite_task(
                task_id,
                status='failed',
                error_message=str(e)
            )

            # Notify user about critical error
            try:
                task = await self.db.get_invite_task(task_id)
                if task:
                    await self._notify_user(
                        task.user_id,
                        f"❌ **Критическая ошибка в задаче инвайтинга**\n\n"
                        f"📊 **Информация о задаче:**\n"
                        f"• ID задачи: {task_id}\n"
                        f"• Группа-источник: {task.source_group_title}\n"
                        f"• Группа-цель: {task.target_group_title}\n"
                        f"• Добавлено участников: {task.invited_count}\n\n"
                        f"🚨 **Ошибка:** {str(e)}\n\n"
                        f"🔧 **Рекомендации:**\n"
                        f"• Проверьте подключение к интернету\n"
                        f"• Убедитесь, что сессии активны\n"
                        f"• Проверьте права администратора в группах"
                    )
            except Exception as notify_error:
                logger.error(f"Не удалось отправить уведомление об ошибке: {notify_error}")
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
        logger.info(f"🔄 SESSION ROTATION: Task {task.id} - Failed sessions: {task.failed_sessions}")
        logger.info(f"🔄 SESSION ROTATION: Task {task.id} - Groups: source={task.source_group_id}, target={task.target_group_id}")

        # Фильтруем пустые строки из available_sessions
        available_sessions = [s for s in task.available_sessions if s]
        
        if not available_sessions:
            logger.error(f"🔄 SESSION ROTATION: Task {task.id} - FAILED: No available sessions for rotation (available_sessions is empty or contains only empty strings)")
            return None

        if len(available_sessions) == 1:
            logger.warning(f"🔄 SESSION ROTATION: Task {task.id} - Only one session available ({available_sessions[0]}), cannot rotate")
            return None

        current_index = -1
        try:
            current_index = available_sessions.index(task.session_alias)
            logger.info(f"🔄 SESSION ROTATION: Task {task.id} - Current session index: {current_index}")
        except ValueError:
            logger.warning(f"🔄 SESSION ROTATION: Task {task.id} - Current session '{task.session_alias}' not found in available list")
            pass  # Current session might not be in the list anymore

        # We will collect errors for all candidates to report if rotation fails completely
        rotation_errors = []
        checked_sessions = []

        # Try next sessions
        for i in range(len(available_sessions)):
            next_index = (current_index + 1 + i) % len(available_sessions)
            candidate_alias = available_sessions[next_index]

            if candidate_alias == task.session_alias and len(available_sessions) > 1:
                logger.debug(f"🔄 SESSION ROTATION: Task {task.id} - Skipping current session '{candidate_alias}'")
                continue

            # Пропускаем сессии, которые уже были помечены как проблемные для этой задачи
            if candidate_alias in task.failed_sessions:
                logger.debug(f"🔄 SESSION ROTATION: Task {task.id} - Skipping failed session '{candidate_alias}'")
                continue

            logger.info(f"🔄 SESSION ROTATION: Task {task.id} - Checking candidate session '{candidate_alias}'")
            checked_sessions.append(candidate_alias)

            # Validate capability using the new granular check
            validation = await self.session_manager.validate_session_capability(
                candidate_alias,
                task.source_group_id,
                task.target_group_id,
                source_username=task.source_username,
                target_username=task.target_username,
                use_proxy=task.use_proxy,
                invite_mode=task.invite_mode
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
            "use_proxy": task.use_proxy,
            "error_message": task.error_message,
            "created_at": task.created_at,
            "updated_at": task.updated_at,
            "available_sessions": task.available_sessions,
            "filter_mode": task.filter_mode,
            "inactive_threshold_days": task.inactive_threshold_days
        }
    
    async def get_all_running_tasks(self) -> list:
        """Get all running tasks."""
        tasks = await self.db.get_running_tasks()
        return [await self.get_task_status(t.id) for t in tasks]
    
    async def _run_from_file_invite_task(self, task_id: int):
        """Invite users from a file."""
        session_consecutive_invites = 0
        try:
            task = await self.db.get_invite_task(task_id)
            if not task:
                logger.error(f"Задача {task_id} не найдена")
                return
            
            if not task.file_source:
                logger.error(f"Задача {task_id}: file_source не указан")
                await self.db.update_invite_task(
                    task_id,
                    status='failed',
                    error_message="Файл-источник не указан"
                )
                return
            
            # Load users from file
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
            from user_files_manager import UserFilesManager
            
            manager = UserFilesManager()
            
            try:
                file_data = manager.load_users_from_file(task.file_source)
                users = file_data['users']
                metadata = file_data.get('metadata', {})
                
                logger.info(f"Задача {task_id}: Загружено {len(users)} пользователей из файла {task.file_source}")
                logger.info(f"Задача {task_id}: Метаданные файла: {metadata}")
            except FileNotFoundError:
                error_msg = f"Файл {task.file_source} не найден"
                logger.error(f"Задача {task_id}: {error_msg}")
                await self.db.update_invite_task(
                    task_id,
                    status='failed',
                    error_message=error_msg
                )
                await self._notify_user(task.user_id, f"❌ **Ошибка**: {error_msg}")
                return
            except Exception as e:
                error_msg = f"Ошибка при загрузке файла: {e}"
                logger.error(f"Задача {task_id}: {error_msg}")
                await self.db.update_invite_task(
                    task_id,
                    status='failed',
                    error_message=error_msg
                )
                await self._notify_user(task.user_id, f"❌ **Ошибка**: {error_msg}")
                return
            
            # Get already invited users
            invited_ids = await self.db.get_invited_user_ids(
                -1,  # Special source_group_id for file-based invites
                task.target_group_id
            )
            
            # Process users from file
            current_index = task.current_offset
            
            while current_index < len(users) and not self._stop_flags.get(task_id, False):
                task = await self.db.get_invite_task(task_id)
                if not task:
                    logger.error(f"Задача {task_id} не найдена")
                    break
                
                # Check if limit reached
                if task.limit and task.invited_count >= task.limit:
                    logger.info(f"Задача {task_id} достигла лимита: {task.invited_count}/{task.limit}")
                    break
                
                # Get session client
                proxy_info = await self.session_manager.get_proxy_info(task.session_alias, task.use_proxy)
                proxy_str = f" через прокси {proxy_info}" if proxy_info else " без прокси"
                
                client = await self.session_manager.get_client(task.session_alias, use_proxy=task.use_proxy)
                if not client:
                    logger.error(f"Не удалось получить клиент для сессии {task.session_alias}{proxy_str}")
                    
                    if task.rotate_sessions and task.available_sessions:
                        logger.info(f"Сессия {task.session_alias} недоступна, попытка ротации...")
                        new_session = await self._rotate_session(task)
                        if new_session:
                            session_consecutive_invites = 0
                            continue
                    
                    error_msg = f"Сессия {task.session_alias} недоступна"
                    await self.db.update_invite_task(
                        task_id,
                        status='failed',
                        error_message=error_msg
                    )
                    break
                
                # Process batch of users
                batch_size = min(50, len(users) - current_index)
                batch_users = users[current_index:current_index + batch_size]
                
                processed_in_batch = 0
                for user in batch_users:
                    processed_in_batch += 1
                    if self._stop_flags.get(task_id, False):
                        await self.db.update_invite_task(
                            task_id,
                            current_offset=current_index + processed_in_batch
                        )
                        break
                    
                    user_id = user.get('id')
                    user_username = user.get('username')
                    
                    # Need at least user_id or username to invite
                    if not user_id and not user_username:
                        continue
                    
                    # Skip already invited (only if we have user_id)
                    if user_id and user_id in invited_ids:
                        continue
                    
                    user_first_name = user.get('first_name')
                    # Use user_id for invite, or fall back to username
                    invite_target = user_id if user_id else user_username
                    user_info = self._format_user_info(user_id, user_username, user_first_name)
                    
                    # PRE-CHECK: Check if user is ALREADY in target group
                    # Only works reliably with user_id, skip for username-only cases
                    if user_id:
                        try:
                            target_member = await client.get_chat_member(task.target_group_id, user_id)
                            raw_status = target_member.status
                            status = getattr(raw_status, "name", str(raw_status)).upper()
                            
                            if status != 'LEFT':
                                logger.info(f"Задача {task_id}: Пользователь {user_info} имеет статус {status} в целевой группе. Пропуск.")
                                
                                status_code = 'banned_in_target' if status in ['KICKED', 'BANNED'] else 'already_in_target'
                                
                                await self.db.add_invite_record(
                                    task_id, user_id,
                                    username=user_username,
                                    first_name=user_first_name,
                                    status=status_code
                                )
                                await asyncio.sleep(0.1)
                                continue
                        except Exception:
                            pass
                    
                    # Check limit
                    if task.limit and task.invited_count >= task.limit:
                        logger.info(f"Задача {task_id} достигла лимита: {task.invited_count}/{task.limit}")
                        break
                    
                    # Check session rotation limit
                    if task.rotate_sessions and task.rotate_every > 0 and session_consecutive_invites >= task.rotate_every:
                        logger.info(f"Задача {task_id}: Сессия {task.session_alias} достигла {session_consecutive_invites} приглашений. Ротация...")
                        
                        offset_to_save = current_index + max(processed_in_batch - 1, 0)
                        await self.db.update_invite_task(
                            task_id,
                            current_offset=offset_to_save
                        )
                        current_index = offset_to_save
                        
                        new_session = await self._rotate_session(task)
                        if new_session:
                            session_consecutive_invites = 0
                            processed_in_batch = -1
                            break
                        else:
                            logger.warning(f"Задача {task_id}: Ротация по счетчику не удалась. Продолжаем с текущей сессией {task.session_alias}")
                            session_consecutive_invites = 0
                    
                    # Invite the user
                    result = await self.session_manager.invite_user(
                        task.session_alias,
                        task.target_group_id,
                        invite_target,
                        target_username=task.target_username,
                        use_proxy=task.use_proxy
                    )
                    
                    if result.get('success'):
                        # Use user_id if available, otherwise use username as identifier
                        record_user_id = user_id if user_id else f"@{user_username}"
                        await self.db.add_invite_record(
                            task_id, record_user_id,
                            username=user_username,
                            first_name=user_first_name,
                            status='success'
                        )
                        await self.db.update_invite_task(
                            task_id,
                            invited_count=task.invited_count + 1
                        )
                        task.invited_count += 1
                        session_consecutive_invites += 1
                        logger.info(f"Задача {task_id}: Приглашен пользователь {user_info} ({task.invited_count}/{task.limit or '∞'}) (сессия: {task.session_alias}{proxy_str})")
                        
                        # Delay
                        if task.invited_count % task.delay_every == 0:
                            min_delay = max(1, int(task.delay_seconds * 0.8))
                            max_delay = int(task.delay_seconds * 1.2)
                            actual_delay = random.randint(min_delay, max_delay)
                            
                            logger.info(f"Задача {task_id}: Ожидание {actual_delay}с после {task.delay_every} приглашений")
                            await asyncio.sleep(actual_delay)
                        else:
                            await asyncio.sleep(random.randint(2, 5))
                    
                    elif result.get('flood_wait'):
                        wait_time = result['flood_wait']
                        logger.warning(f"FloodWait {wait_time}s for session {task.session_alias}")
                        
                        if task.rotate_sessions and task.available_sessions:
                            logger.info(f"FloodWait on {task.session_alias}, attempting rotation...")
                            new_session = await self._rotate_session(task)
                            if new_session:
                                logger.info(f"Rotated to session {new_session} due to FloodWait")
                                session_consecutive_invites = 0
                                break
                        
                        await asyncio.sleep(min(wait_time, 300))
                    
                    elif result.get('fatal'):
                        error_detail = result.get('error', 'Unknown error')
                        logger.error(f"Критическая ошибка с сессией {task.session_alias}{proxy_str}: {error_detail}")
                        
                        current_failing = task.session_alias
                        
                        if current_failing not in task.failed_sessions:
                            task.failed_sessions.append(current_failing)
                            await self.db.update_invite_task(task_id, failed_sessions=task.failed_sessions)
                        
                        if task.rotate_sessions and task.available_sessions:
                            new_session = await self._rotate_session(task)
                            if new_session:
                                session_consecutive_invites = 0
                                processed_in_batch -= 1
                                break
                            else:
                                error_msg = f"Критическая ошибка: {error_detail}. Ротация не удалась."
                                await self.db.update_invite_task(task_id, status='failed', error_message=error_msg)
                                await self._notify_user(task.user_id, f"❌ **Задача остановлена**: {error_msg}")
                                return
                        else:
                            error_msg = f"Критическая ошибка: {error_detail}"
                            await self.db.update_invite_task(task_id, status='failed', error_message=error_msg)
                            await self._notify_user(task.user_id, f"❌ **Задача остановлена**: {error_msg}")
                            return
                    
                    elif result.get('skip'):
                        await self.db.add_invite_record(
                            task_id, user_id,
                            username=user_username,
                            first_name=user_first_name,
                            status='skipped',
                            error_message=result.get('error')
                        )
                    
                    else:
                        await self.db.add_invite_record(
                            task_id, user_id,
                            username=user_username,
                            first_name=user_first_name,
                            status='failed',
                            error_message=result.get('error')
                        )
                
                # Update offset
                if processed_in_batch > 0:
                    current_index += processed_in_batch
                    await self.db.update_invite_task(
                        task_id,
                        current_offset=current_index
                    )
            
            # Task finished
            task = await self.db.get_invite_task(task_id)
            if task and task.status == 'running':
                logger.info(f"Задача {task_id} завершена корректно")
                await self.db.update_invite_task(task_id, status='completed')
                
                await self._notify_user(
                    task.user_id,
                    f"✅ **Задача инвайтинга из файла завершена**\n\n"
                    f"📊 **Итоговые результаты:**\n"
                    f"• Файл-источник: {task.file_source}\n"
                    f"• Группа-цель: {task.target_group_title}\n"
                    f"• Всего добавлено: {task.invited_count} участников\n"
                    f"• Лимит: {task.limit or 'Неограничен'}\n\n"
                    f"🎯 **Задача выполнена успешно!**"
                )
        
        except asyncio.CancelledError:
            logger.info(f"Задача {task_id} была отменена")
        except Exception as e:
            logger.error(f"Ошибка в задаче инвайтинга из файла {task_id}: {e}", exc_info=True)
            await self.db.update_invite_task(
                task_id,
                status='failed',
                error_message=str(e)
            )
        finally:
            self._stop_flags.pop(task_id, None)
            self.running_tasks.pop(task_id, None)

