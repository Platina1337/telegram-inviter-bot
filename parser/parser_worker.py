# -*- coding: utf-8 -*-
"""
Parser worker for executing parse tasks.
"""
import logging
import asyncio
import httpx
from typing import Dict, List, Optional
from datetime import datetime

from parser.database import Database
from parser.session_manager import SessionManager
from shared.models import ParseTask
from shared.user_files_manager import UserFilesManager

logger = logging.getLogger(__name__)


class ParserWorker:
    """Worker for executing parse tasks."""
    
    def __init__(self, db: Database, session_manager: SessionManager):
        self.db = db
        self.session_manager = session_manager
        self.running_tasks: Dict[int, asyncio.Task] = {}
        self.user_files_manager = UserFilesManager()
        self.http_client = httpx.AsyncClient(timeout=10.0)
        # Store unsaved members for each task (for saving on stop)
        self.task_unsaved_members: Dict[int, List[Dict]] = {}
        self.task_metadata: Dict[int, Dict] = {}
        # Track last heartbeat update time to avoid too frequent DB writes
        self._last_heartbeat: Dict[int, float] = {}
    
    async def start_parse_task(self, task_id: int) -> Dict:
        """Start a parse task."""
        if task_id in self.running_tasks:
            return {"success": False, "error": "Task already running"}
        
        task = await self.db.get_parse_task(task_id)
        if not task:
            return {"success": False, "error": "Task not found"}
        
        # Update status
        await self.db.update_parse_task(task_id, status='running')
        
        # Initialize unsaved members storage
        self.task_unsaved_members[task_id] = []
        
        # Start task in background
        async_task = asyncio.create_task(self._run_parse_task(task))
        self.running_tasks[task_id] = async_task
        
        # Start heartbeat tracking (will be updated during task execution)
        self._last_heartbeat[task_id] = 0  # Force first heartbeat update
        
        logger.info(f"Started parse task {task_id}")
        return {"success": True}
    
    async def _update_heartbeat_if_needed(self, task_id: int):
        """Update heartbeat if 60+ seconds passed since last update. Returns True if updated."""
        import time
        now = time.time()
        last = self._last_heartbeat.get(task_id, 0)
        if now - last >= 60:  # Update every 60 seconds minimum
            await self.db.update_parse_task(task_id, last_heartbeat=datetime.now().isoformat())
            self._last_heartbeat[task_id] = now
            return True
        return False
    
    async def stop_parse_task(self, task_id: int) -> Dict:
        """Stop a parse task and save any unsaved data."""
        # Clean up heartbeat tracking
        if task_id in self._last_heartbeat:
            del self._last_heartbeat[task_id]
        
        task = await self.db.get_parse_task(task_id)
        
        # Save unsaved members before stopping
        if task_id in self.task_unsaved_members and self.task_unsaved_members[task_id]:
            unsaved = self.task_unsaved_members[task_id]
            if task and unsaved:
                metadata = self.task_metadata.get(task_id, {
                    'source_group_id': task.source_group_id,
                    'source_group_title': task.source_group_title,
                    'filter_admins': task.filter_admins,
                    'filter_inactive': task.filter_inactive,
                    'inactive_days': task.inactive_threshold_days,
                    'parsed_at': datetime.now().isoformat()
                })
                
                logger.info(f"💾 Сохранение {len(unsaved)} несохраненных пользователей перед остановкой задачи {task_id}")
                filepath, total = self.user_files_manager.append_users_to_file(
                    task.file_name,
                    unsaved,
                    metadata
                )
                
                # Update saved count
                await self.db.update_parse_task(task_id, saved_count=total)
                
                # Clear unsaved
                self.task_unsaved_members[task_id] = []
                
                # Notify user
                await self._notify_user(
                    task.user_id,
                    f"⏸️ **Парсинг приостановлен**\n\n"
                    f"📝 Файл: `{task.file_name}`\n"
                    f"💾 Сохранено в файл: `{total}` пользователей\n"
                    f"👥 Всего спаршено: `{task.parsed_count}` пользователей"
                )
        
        if task_id not in self.running_tasks:
            await self.db.update_parse_task(task_id, status='paused')
            return {"success": True, "message": "Task was not running, data saved"}
        
        # Cancel the task
        self.running_tasks[task_id].cancel()
        del self.running_tasks[task_id]
        
        await self.db.update_parse_task(task_id, status='paused')
        logger.info(f"Stopped parse task {task_id}")
        return {"success": True}
    
    async def _save_users_incremental(self, task: ParseTask, users: List[Dict]) -> int:
        """Save users to file incrementally. Returns total count in file."""
        if not users:
            return task.saved_count
        
        metadata = {
            'source_group_id': task.source_group_id,
            'source_group_title': task.source_group_title,
            'filter_admins': task.filter_admins,
            'filter_inactive': task.filter_inactive,
            'inactive_days': task.inactive_threshold_days,
            'parsed_at': datetime.now().isoformat()
        }
        
        self.task_metadata[task.id] = metadata
        
        filepath, total = self.user_files_manager.append_users_to_file(
            task.file_name,
            users,
            metadata
        )
        
        logger.info(f"💾 Задача {task.id} - сохранено {len(users)} пользователей в файл (всего в файле: {total})")
        
        return total
    
    async def _run_parse_task(self, task: ParseTask):
        """Execute the parse task."""
        task_id = task.id
        
        # Check source type - if channel, use channel comments parsing
        if task.source_type == 'channel':
            await self._run_channel_comments_parse_task(task)
            return
        
        # Check parse mode and delegate to appropriate method
        if task.parse_mode == 'message_based':
            await self._run_message_based_parse_task(task)
            return
        
        # Default: member_list mode
        await self._run_member_list_parse_task(task)

    async def _run_member_list_parse_task(self, task: ParseTask):
        """Execute member list based parse task."""
        task_id = task.id
        user_id = task.user_id
        session_consecutive_parsed = 0  # Track parsed users for session rotation
        unsaved_members: List[Dict] = []  # Track unsaved members for incremental saving
        
        try:
            logger.info(f"🚀 Запуск задачи парсинга {task_id}: {task.file_name}")
            
            # Get available sessions
            available_sessions = task.available_sessions.copy() if task.available_sessions else []
            logger.info(f"📋 Задача {task_id} - доступные сессии из настроек: {available_sessions}")
            
            # If no sessions in task settings, use sessions assigned to "parsing" task
            if not available_sessions:
                logger.info(f"⚠️ Задача {task_id} - сессии не указаны в настройках, используем сессии назначенные на задачу 'parsing'")
                parsing_sessions = await self.db.get_sessions_for_task("parsing")
                available_sessions = [s.alias for s in parsing_sessions if s.alias]
                
                if not available_sessions:
                    logger.warning(f"⚠️ Задача {task_id} - нет сессий назначенных на 'parsing', используем все доступные")
                    all_sessions = await self.session_manager.list_sessions()
                    available_sessions = [s['alias'] for s in all_sessions if s.get('alias')]
                
                if not available_sessions:
                    raise Exception("No sessions available for parsing")
                
                # Update task with available sessions
                await self.db.update_parse_task(task_id, available_sessions=available_sessions)
                logger.info(f"✅ Задача {task_id} - установлены сессии: {available_sessions}")
            
            # Remove failed sessions
            available_sessions = [s for s in available_sessions if s and s not in task.failed_sessions]
            if not available_sessions:
                raise Exception("All sessions have failed")
            
            logger.info(f"✅ Задача {task_id} - активные сессии (после исключения проблемных): {available_sessions}")
            
            # Use current session or first available
            current_session = task.session_alias if task.session_alias in available_sessions else available_sessions[0]
            logger.info(f"🔐 Задача {task_id} - текущая сессия: {current_session}")
            
            # Get proxy info for logging
            proxy_info = await self.session_manager.get_proxy_info(current_session, task.use_proxy)
            proxy_str = f" через прокси {proxy_info}" if proxy_info else " без прокси"
            logger.info(f"🌐 Задача {task_id} - прокси: {proxy_str}")
            
            # Get client
            client = await self.session_manager.get_client(current_session, use_proxy=task.use_proxy)
            if not client:
                raise Exception(f"Session {current_session} not available")
            
            logger.info(f"✅ Задача {task_id} - клиент получен для сессии {current_session}{proxy_str}")
            
            # Get group members
            offset = task.current_offset
            limit_per_request = 200
            total_parsed = task.parsed_count
            saved_count = task.saved_count
            
            from datetime import datetime, timedelta, timezone
            
            save_every = task.save_every if task.save_every > 0 else 0
            
            # Load already saved user IDs to skip them when resuming
            already_saved_ids = self.user_files_manager.get_saved_user_ids(task.file_name)
            if already_saved_ids:
                logger.info(f"📂 Задача {task_id} - загружено {len(already_saved_ids)} уже сохранённых ID из файла (пропустим при парсинге)")
            
            logger.info(f"📊 Задача {task_id} - начальные параметры: offset={offset}, уже спаршено={total_parsed}, сохранено={saved_count}, лимит={task.limit or 'без лимита'}")
            logger.info(f"⚙️ Задача {task_id} - настройки ротации: включена={task.rotate_sessions}, каждые {task.rotate_every} польз.")
            logger.info(f"⏱️ Задача {task_id} - задержка: {task.delay_seconds} сек каждые {task.delay_every} польз.")
            logger.info(f"💾 Задача {task_id} - сохранение в файл каждые: {save_every or 'в конце'} польз.")
            logger.info(f"🚫 Задача {task_id} - фильтры: исключать админов={'Да' if task.filter_admins else 'Нет'}, исключать неактивных={'Да' if task.filter_inactive else 'Нет'} (> {task.inactive_threshold_days} дн.)")
            
            while True:
                # Check if task was cancelled
                if task_id not in self.running_tasks:
                    logger.info(f"⏹️ Задача {task_id} была отменена")
                    break
                
                # Check limit
                if task.limit and total_parsed >= task.limit:
                    logger.info(f"✅ Задача {task_id} достигла лимита: {task.limit}")
                    await self.db.update_parse_task(task_id, status='completed')
                    break
                
                try:
                    # Get members batch
                    batch_limit = limit_per_request
                    
                    if task.limit:
                        remaining = task.limit - total_parsed
                        # We might need to fetch more because of filtering, but let's stick to simple logic
                        batch_limit = min(batch_limit, remaining + 50) # fetch a bit more than needed to account for filters
                    
                    logger.info(f"📥 Задача {task_id} - запрос участников из '{task.source_group_title}': offset={offset}, limit={batch_limit} (сессия: {current_session}{proxy_str})")
                    
                    batch = await self.session_manager.get_group_members(
                        current_session,
                        task.source_group_id,
                        limit=batch_limit,
                        offset=offset,
                        username=task.source_username
                    )
                    
                    if not batch:
                        logger.info(f"✅ Задача {task_id} - больше нет участников для парсинга")
                        await self.db.update_parse_task(task_id, status='completed')
                        break
                    
                    logger.info(f"📦 Задача {task_id} - получено {len(batch)} участников из API")
                    
                    # Process members one by one (like inviter does)
                    for member in batch:
                        # Check if task was cancelled
                        if task_id not in self.running_tasks:
                            logger.info(f"⏹️ Задача {task_id} была отменена во время обработки")
                            break
                        
                        # Check limit
                        if task.limit and total_parsed >= task.limit:
                            logger.info(f"✅ Задача {task_id} достигла лимита: {task.limit}")
                            break
                        
                        member_id = member.get('id')
                        user_info = f"{member.get('username') or member.get('id')}"
                        
                        # 1. Filter admins - делаем отдельный запрос как в инвайтинге
                        if task.filter_admins:
                            logger.debug(f"🔍 Задача {task_id} - проверка админа для {user_info}")
                            try:
                                source_member = await client.get_chat_member(task.source_group_id, member_id)
                                raw_status = source_member.status
                                status_str = getattr(raw_status, "name", str(raw_status)).upper()
                                
                                if status_str in ['ADMINISTRATOR', 'CREATOR', 'OWNER']:
                                    logger.info(f"🔍 Задача {task_id} - пропуск админа: {user_info} (статус в группе: {status_str})")
                                    continue
                                else:
                                    logger.debug(f"🔍 Задача {task_id} - пользователь {user_info} не админ (статус: {status_str})")
                            except Exception as e:
                                # Если не удалось проверить статус, считаем что не админ (как в инвайтинге)
                                logger.warning(f"🔍 Задача {task_id} - не удалось проверить статус админа для {user_info}: {e}. Считаем не админом.")
                        
                        # 2. Filter inactive - делаем отдельный запрос для получения last_online_date как в инвайтинге
                        if task.filter_inactive and task.inactive_threshold_days is not None:
                            logger.debug(f"🔍 Задача {task_id} - проверка неактивности для {user_info} (порог: {task.inactive_threshold_days} дн.)")
                            user_last_online = None
                            try:
                                # Получаем last_online_date через get_users (как в инвайтинге)
                                users = await client.get_users([member_id])
                                if users:
                                    user_obj = users[0]
                                    user_last_online = getattr(user_obj, 'last_online_date', None)
                            except Exception as e:
                                logger.warning(f"🔍 Задача {task_id} - не удалось получить last_online_date для {user_info}: {e}. Считаем активным (как в инвайтинге).")
                            
                            # Если last_online_date недоступна, считаем пользователя активным (как в инвайтинге)
                            if user_last_online is None:
                                logger.debug(f"🔍 Задача {task_id} - пользователь {user_info}: last_online_date недоступна. Считаем активным (не пропускаем).")
                            else:
                                # Если получили last_online_date, проверяем по дате (как в инвайтинге)
                                from datetime import datetime, timedelta
                                threshold_date = datetime.now() - timedelta(days=task.inactive_threshold_days)
                                days_since_online = (datetime.now() - user_last_online).days
                                
                                logger.debug(f"🔍 Задача {task_id} - пользователь {user_info}: был онлайн {days_since_online} дн. назад (порог: {task.inactive_threshold_days} дн.)")
                                
                                if user_last_online < threshold_date:
                                    logger.info(f"🔍 Задача {task_id} - пропуск неактивного: {user_info} (был онлайн {days_since_online} дн. назад, порог: {task.inactive_threshold_days} дн.)")
                                    continue
                                else:
                                    logger.debug(f"🔍 Задача {task_id} - пользователь {user_info} активен ({days_since_online} дн. <= {task.inactive_threshold_days} дн.)")
                        
                        # 3. Skip already saved users (for resume support)
                        if member_id in already_saved_ids:
                            logger.debug(f"🔍 Задача {task_id} - пропуск уже сохранённого: {member.get('username') or member_id}")
                            continue
                        
                        # Add user to list
                        user_data = {
                            'id': member_id,
                            'username': member['username'],
                            'first_name': member['first_name'],
                            'last_name': member['last_name']
                        }
                        unsaved_members.append(user_data)
                        self.task_unsaved_members[task_id] = unsaved_members
                        already_saved_ids.add(member_id)  # Mark as processed to avoid duplicates in same run
                        total_parsed += 1
                        session_consecutive_parsed += 1
                        
                        logger.info(f"✅ Задача {task_id} - добавлен пользователь #{total_parsed}: {user_data.get('username') or user_data.get('id')} (сессия: {current_session})")
                        
                        # Update progress in DB
                        await self.db.update_parse_task(
                            task_id,
                            parsed_count=total_parsed
                        )
                        
                        # Check for incremental save
                        if save_every > 0 and len(unsaved_members) >= save_every:
                            saved_count = await self._save_users_incremental(task, unsaved_members)
                            await self.db.update_parse_task(task_id, saved_count=saved_count)
                            unsaved_members = []  # Clear after save
                            self.task_unsaved_members[task_id] = unsaved_members
                            logger.info(f"💾 Задача {task_id} - инкрементальное сохранение: {saved_count} польз. в файле")
                        
                        # Check for session rotation based on parsed count
                        if task.rotate_sessions and task.rotate_every > 0 and session_consecutive_parsed >= task.rotate_every:
                            if available_sessions and len(available_sessions) > 1:
                                logger.info(f"🔄 Задача {task_id} - сессия {current_session} обработала {session_consecutive_parsed} пользователей. Ротация...")
                                
                                try:
                                    current_index = available_sessions.index(current_session)
                                except ValueError:
                                    current_index = -1
                                    
                                next_index = (current_index + 1) % len(available_sessions)
                                next_session = available_sessions[next_index]
                                
                                if next_session != current_session:
                                    # Get proxy info for new session
                                    new_proxy_info = await self.session_manager.get_proxy_info(next_session, task.use_proxy)
                                    new_proxy_str = f" через прокси {new_proxy_info}" if new_proxy_info else " без прокси"
                                    
                                    logger.info(f"🔄 Задача {task_id} - ротация сессии: {current_session}{proxy_str} -> {next_session}{new_proxy_str}")
                                    current_session = next_session
                                    proxy_str = new_proxy_str
                                    session_consecutive_parsed = 0  # Reset counter
                                    
                                    await self.db.update_parse_task(task_id, session_alias=current_session)
                                    
                                    # Update client for next iteration
                                    client = await self.session_manager.get_client(current_session, use_proxy=task.use_proxy)
                                    if not client:
                                        logger.error(f"❌ Задача {task_id} - не удалось получить клиент для сессии {current_session}")
                                        raise Exception(f"Failed to get client for session {current_session}")
                                    
                                    logger.info(f"✅ Задача {task_id} - клиент обновлен для сессии {current_session}{proxy_str}")
                            else:
                                logger.warning(f"⚠️ Задача {task_id} - ротация включена, но доступна только одна сессия")
                        
                        # Apply delay after every N parsed users
                        if task.delay_seconds > 0 and task.delay_every > 0:
                            if total_parsed % task.delay_every == 0:
                                logger.info(f"⏱️ Задача {task_id} - задержка {task.delay_seconds} сек после {total_parsed} обработанных пользователей")
                                await asyncio.sleep(task.delay_seconds)
                    
                    # Update offset for next batch
                    offset += len(batch)
                    await self.db.update_parse_task(task_id, current_offset=offset)
                    
                    # If batch was smaller than limit, we've reached the end
                    if len(batch) < batch_limit:
                        logger.info(f"✅ Задача {task_id} - достигнут конец списка участников (получено {len(batch)} < {batch_limit})")
                        await self.db.update_parse_task(task_id, status='completed')
                        break
                    
                    # Also stop if we reached the limit
                    if task.limit and total_parsed >= task.limit:
                        logger.info(f"✅ Задача {task_id} - достигнут лимит парсинга: {total_parsed}/{task.limit}")
                        await self.db.update_parse_task(task_id, status='completed')
                        break
                
                except Exception as e:
                    logger.error(f"❌ Задача {task_id} - ошибка при получении участников: {e}", exc_info=True)
                    
                    # Try rotating session on error
                    if task.rotate_sessions and len(available_sessions) > 1:
                        logger.info(f"🔄 Задача {task_id} - попытка ротации сессии из-за ошибки...")
                        
                        try:
                            current_index = available_sessions.index(current_session)
                        except ValueError:
                            current_index = -1
                            
                        next_index = (current_index + 1) % len(available_sessions)
                        next_session = available_sessions[next_index]
                        
                        # Get proxy info for new session
                        new_proxy_info = await self.session_manager.get_proxy_info(next_session, task.use_proxy)
                        new_proxy_str = f" через прокси {new_proxy_info}" if new_proxy_info else " без прокси"
                        
                        logger.info(f"🔄 Задача {task_id} - ротация из-за ошибки: {current_session}{proxy_str} -> {next_session}{new_proxy_str}")
                        current_session = next_session
                        proxy_str = new_proxy_str
                        session_consecutive_parsed = 0  # Reset counter
                        
                        client = await self.session_manager.get_client(current_session, use_proxy=task.use_proxy)
                        if not client:
                            logger.error(f"❌ Задача {task_id} - не удалось ротировать на сессию {current_session}")
                            raise Exception(f"Failed to rotate to session {current_session}")
                        
                        await self.db.update_parse_task(task_id, session_alias=current_session)
                        logger.info(f"✅ Задача {task_id} - успешная ротация на сессию {current_session}{proxy_str}, продолжаем...")
                        continue
                    else:
                        logger.error(f"❌ Задача {task_id} - ротация недоступна, прерывание задачи")
                        raise
            
            # Save remaining users to file at the end
            if unsaved_members:
                saved_count = await self._save_users_incremental(task, unsaved_members)
                await self.db.update_parse_task(task_id, saved_count=saved_count)
                unsaved_members = []
                self.task_unsaved_members[task_id] = unsaved_members
                logger.info(f"💾 Задача {task_id} - финальное сохранение: {saved_count} польз. в файле")
            
            if saved_count > 0:
                logger.info(f"✅ Задача {task_id} завершена: сохранено {saved_count} пользователей")
                
                # Notify user
                await self._notify_user(
                    user_id, 
                    f"✅ **Парсинг завершен!**\n\n"
                    f"📝 Файл: `{task.file_name}`\n"
                    f"👥 Сохранено: `{saved_count}` пользователей.\n"
                    f"📂 Файл сохранен в `user_files/`"
                )
            else:
                logger.warning(f"⚠️ Задача {task_id} - не найдено пользователей для сохранения")
                await self._notify_user(
                    user_id, 
                    f"⚠️ **Парсинг завершен, но участники не найдены.**\n\n"
                    f"Возможно, список участников скрыт или применены слишком строгие фильтры."
                )
            
            # Mark as completed
            await self.db.update_parse_task(task_id, status='completed')
            logger.info(f"🎉 Задача {task_id} успешно завершена")
            
        except asyncio.CancelledError:
            logger.info(f"⏹️ Задача {task_id} была отменена пользователем")
            # Save any unsaved data on cancel
            if unsaved_members:
                saved_count = await self._save_users_incremental(task, unsaved_members)
                await self.db.update_parse_task(task_id, saved_count=saved_count)
                logger.info(f"💾 Задача {task_id} - сохранено {len(unsaved_members)} польз. при отмене (всего: {saved_count})")
            await self.db.update_parse_task(task_id, status='paused')
        except Exception as e:
            logger.error(f"💥 Задача {task_id} завершилась с ошибкой: {e}", exc_info=True)
            # Save any unsaved data on error
            if unsaved_members:
                try:
                    saved_count = await self._save_users_incremental(task, unsaved_members)
                    await self.db.update_parse_task(task_id, saved_count=saved_count)
                    logger.info(f"💾 Задача {task_id} - сохранено {len(unsaved_members)} польз. при ошибке (всего: {saved_count})")
                except Exception as save_error:
                    logger.error(f"❌ Не удалось сохранить данные при ошибке: {save_error}")
            await self.db.update_parse_task(
                task_id,
                status='failed',
                error_message=str(e)
            )
            await self._notify_user(user_id, f"❌ **Ошибка парсинга!**\n\nЗадача: `{task.file_name}`\nОшибка: `{str(e)}`")
        finally:
            # Remove from running tasks
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
                logger.info(f"🧹 Задача {task_id} удалена из списка активных задач")
            # Clean up heartbeat tracking
            if task_id in self._last_heartbeat:
                del self._last_heartbeat[task_id]
            # Clean up unsaved members storage
            if task_id in self.task_unsaved_members:
                del self.task_unsaved_members[task_id]
            if task_id in self.task_metadata:
                del self.task_metadata[task_id]


    async def _notify_user(self, user_id: int, text: str):
        """Send a notification to user via bot (using HTTP API)."""
        from parser.config import config
        
        if not config.BOT_TOKEN:
            return
            
        try:
            url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": user_id,
                "text": text,
                "parse_mode": "Markdown"
            }
            
            response = await self.http_client.post(url, json=payload)
            if response.status_code != 200:
                logger.error(f"Failed to send notification: {response.text}")
        except Exception as e:
            logger.error(f"Failed to send notification to user {user_id}: {e}")

    async def _run_message_based_parse_task(self, task: ParseTask):
        """Execute message-based parse task - parses users from chat history based on their messages.
        
        This method works differently from member_list mode:
        1. Iterates through chat message history (not member list)
        2. For each message, checks if author matches keyword/exclude filters
        3. Saves matching users incrementally to file (not in memory)
        4. All settings (delay, rotation, save_every) apply to API requests/found users on stage 1
        """
        task_id = task.id
        user_id = task.user_id
        unsaved_members: List[Dict] = []
        
        try:
            logger.info(f"🚀 Запуск задачи парсинга по сообщениям {task_id}: {task.file_name}")
            logger.info(f"📋 Режим: по сообщениям")
            logger.info(f"🔑 Ключевые слова: {task.keyword_filter if task.keyword_filter else 'нет'}")
            logger.info(f"🚫 Слова для исключения: {task.exclude_keywords if task.exclude_keywords else 'нет'}")
            logger.info(f"📊 Лимит сообщений: {task.messages_limit if task.messages_limit else 'без лимита'}")
            logger.info(f"💾 Сохранять каждые: {task.save_every_users} уникальных пользователей" if task.save_every_users > 0 else "💾 Сохранение: в конце")
            logger.info(f"⏱️ Задержка: {task.delay_seconds} сек каждые {task.delay_every_requests} запросов")
            logger.info(f"🔄 Ротация: каждые {task.rotate_every_requests} запросов" if task.rotate_every_requests > 0 else "🔄 Ротация: только при ошибках")
            
            # Get available sessions
            available_sessions = task.available_sessions.copy() if task.available_sessions else []
            
            if not available_sessions:
                parsing_sessions = await self.db.get_sessions_for_task("parsing")
                available_sessions = [s.alias for s in parsing_sessions if s.alias]
                
                if not available_sessions:
                    all_sessions = await self.session_manager.list_sessions()
                    available_sessions = [s['alias'] for s in all_sessions if s.get('alias')]
                
                if not available_sessions:
                    raise Exception("No sessions available for parsing")
                
                await self.db.update_parse_task(task_id, available_sessions=available_sessions)
            
            # Remove failed sessions
            available_sessions = [s for s in available_sessions if s and s not in task.failed_sessions]
            if not available_sessions:
                raise Exception("All sessions have failed")
            
            # Use current session or first available
            current_session = task.session_alias if task.session_alias in available_sessions else available_sessions[0]
            
            # Get proxy info
            proxy_info = await self.session_manager.get_proxy_info(current_session, task.use_proxy)
            proxy_str = f" через прокси {proxy_info}" if proxy_info else " без прокси"
            
            # Get client
            client = await self.session_manager.get_client(current_session, use_proxy=task.use_proxy)
            if not client:
                raise Exception(f"Session {current_session} not available")
            
            logger.info(f"✅ Задача {task_id} - клиент получен для сессии {current_session}{proxy_str}")
            
            # Initialize counters
            total_unique_users = 0  # Total unique users found (matching keywords/exclude criteria)
            saved_count = task.saved_count
            processed_messages = task.messages_offset  # Resume from where we left off
            api_requests_count = 0  # Count API requests for delay/rotation
            
            # Load already saved user IDs to skip duplicates
            already_saved_ids = self.user_files_manager.get_saved_user_ids(task.file_name)
            if already_saved_ids:
                logger.info(f"📂 Задача {task_id} - загружено {len(already_saved_ids)} уже сохранённых ID из файла (пропустим)")
            
            # Track seen users in this run to avoid duplicates within same run
            seen_user_ids = set()
            
            logger.info(f"📊 Задача {task_id} - начало итерации по истории чата {task.source_group_title}")
            if processed_messages > 0:
                logger.info(f"🔄 Задача {task_id} - продолжение с сообщения #{processed_messages}")
            
            # Join source group if needed
            await self.session_manager.join_chat_if_needed(
                client, 
                task.source_group_id, 
                task.source_username
            )
            
            # Iterate through chat history
            async for message in client.get_chat_history(task.source_group_id, offset=task.messages_offset):
                # Check if task was cancelled
                if task_id not in self.running_tasks:
                    logger.info(f"⏹️ Задача {task_id} была отменена")
                    break
                
                processed_messages += 1
                
                # Log progress every 100 messages
                if processed_messages % 100 == 0:
                    logger.info(
                        f"📨 Задача {task_id} - обработано {processed_messages} сообщений, "
                        f"найдено {total_unique_users} уникальных пользователей, "
                        f"сохранено в файл: {saved_count}"
                    )
                
                # Check messages limit
                if task.messages_limit and processed_messages >= task.messages_limit:
                    logger.info(f"✅ Задача {task_id} - достигнут лимит по сообщениям: {task.messages_limit}")
                    break
                
                # Count API requests (approximately every 100 messages = 1 request)
                if processed_messages % 100 == 0:
                    api_requests_count += 1
                    
                    # Check for delay after N requests
                    if task.delay_every_requests > 0 and api_requests_count % task.delay_every_requests == 0:
                        logger.info(f"⏱️ Задача {task_id} - задержка {task.delay_seconds} сек после {api_requests_count} запросов")
                        await asyncio.sleep(task.delay_seconds)
                    
                    # Check for session rotation after N requests
                    if task.rotate_sessions and task.rotate_every_requests > 0 and api_requests_count % task.rotate_every_requests == 0:
                        if len(available_sessions) > 1:
                            logger.info(f"🔄 Задача {task_id} - ротация сессии после {api_requests_count} запросов...")
                            
                            try:
                                current_index = available_sessions.index(current_session)
                            except ValueError:
                                current_index = -1
                            
                            next_index = (current_index + 1) % len(available_sessions)
                            next_session = available_sessions[next_index]
                            
                            if next_session != current_session:
                                new_proxy_info = await self.session_manager.get_proxy_info(next_session, task.use_proxy)
                                new_proxy_str = f" через прокси {new_proxy_info}" if new_proxy_info else " без прокси"
                                
                                logger.info(f"🔄 Задача {task_id} - ротация: {current_session}{proxy_str} -> {next_session}{new_proxy_str}")
                                current_session = next_session
                                proxy_str = new_proxy_str
                                
                                await self.db.update_parse_task(task_id, session_alias=current_session)
                                
                                client = await self.session_manager.get_client(current_session, use_proxy=task.use_proxy)
                                if not client:
                                    raise Exception(f"Failed to get client for session {current_session}")
                
                # Get message author
                user = message.from_user
                if not user or user.is_bot:
                    continue
                
                msg_user_id = user.id
                
                # Skip if already saved or seen in this run
                if msg_user_id in already_saved_ids or msg_user_id in seen_user_ids:
                    continue
                
                # Get message text
                msg_text = message.text or message.caption or ""
                msg_text_lower = msg_text.lower()
                
                # Check for keywords
                has_keyword = False
                if task.keyword_filter:
                    for keyword in task.keyword_filter:
                        if keyword.lower() in msg_text_lower:
                            has_keyword = True
                            break
                else:
                    # No keyword filter = all match
                    has_keyword = True
                
                if not has_keyword:
                    continue
                
                # Check for exclude keywords
                has_exclude = False
                if task.exclude_keywords:
                    for exclude_word in task.exclude_keywords:
                        if exclude_word.lower() in msg_text_lower:
                            has_exclude = True
                            break
                
                if has_exclude:
                    continue
                
                # User matches keyword criteria - mark as seen and add to unique count
                seen_user_ids.add(msg_user_id)
                total_unique_users += 1
                
                user_data = {
                    'id': msg_user_id,
                    'username': user.username,
                    'first_name': user.first_name,
                    'last_name': user.last_name
                }
                user_info = f"{user_data.get('username') or user_data.get('id')}"
                
                # Apply admin filter if enabled
                if task.filter_admins:
                    try:
                        source_member = await client.get_chat_member(task.source_group_id, msg_user_id)
                        raw_status = source_member.status
                        status_str = getattr(raw_status, "name", str(raw_status)).upper()
                        
                        if status_str in ['ADMINISTRATOR', 'CREATOR', 'OWNER']:
                            logger.info(f"🔍 Задача {task_id} - пропуск админа: {user_info}")
                            continue
                    except Exception as e:
                        logger.warning(f"🔍 Задача {task_id} - не удалось проверить статус админа для {user_info}: {e}")
                
                # Apply inactive filter if enabled
                if task.filter_inactive and task.inactive_threshold_days is not None:
                    try:
                        users = await client.get_users([msg_user_id])
                        if users:
                            user_obj = users[0]
                            user_last_online = getattr(user_obj, 'last_online_date', None)
                            
                            if user_last_online is not None:
                                from datetime import datetime, timedelta
                                threshold_date = datetime.now() - timedelta(days=task.inactive_threshold_days)
                                
                                if user_last_online < threshold_date:
                                    days_since_online = (datetime.now() - user_last_online).days
                                    logger.info(f"🔍 Задача {task_id} - пропуск неактивного: {user_info} ({days_since_online} дн.)")
                                    continue
                    except Exception as e:
                        logger.warning(f"🔍 Задача {task_id} - не удалось проверить активность для {user_info}: {e}")
                
                # User passed all filters - add to unsaved list
                unsaved_members.append(user_data)
                self.task_unsaved_members[task_id] = unsaved_members
                already_saved_ids.add(msg_user_id)
                
                logger.info(f"✅ Задача {task_id} - найден пользователь #{total_unique_users}: {user_info}")
                
                # Update messages offset in DB for resume capability
                await self.db.update_parse_task(task_id, messages_offset=processed_messages, parsed_count=total_unique_users)
                
                # Check for incremental save by unique users found
                if task.save_every_users > 0 and len(unsaved_members) >= task.save_every_users:
                    saved_count = await self._save_users_incremental(task, unsaved_members)
                    await self.db.update_parse_task(task_id, saved_count=saved_count)
                    unsaved_members = []
                    self.task_unsaved_members[task_id] = unsaved_members
                    logger.info(f"💾 Задача {task_id} - сохранено {len(unsaved_members)} польз., всего в файле: {saved_count}")
            
            # Save remaining users
            if unsaved_members:
                saved_count = await self._save_users_incremental(task, unsaved_members)
                await self.db.update_parse_task(task_id, saved_count=saved_count)
                logger.info(f"💾 Задача {task_id} - финальное сохранение: всего в файле {saved_count} польз.")
                unsaved_members = []
                self.task_unsaved_members[task_id] = unsaved_members
            
            # Mark as completed
            await self.db.update_parse_task(task_id, status='completed')
            logger.info(f"🎉 Задача {task_id} завершена: обработано {processed_messages} сообщений, найдено {total_unique_users} пользователей.")
            
            # Notify user
            await self._notify_user(
                user_id,
                f"✅ **Парсинг завершен!**\n\n"
                f"📝 Файл: `{task.file_name}`\n"
                f"📤 Группа: {task.source_group_title}\n"
                f"📋 Режим: по сообщениям\n"
                f"📨 Обработано сообщений: {processed_messages}\n"
                f"👥 Найдено пользователей: {total_unique_users}\n"
                f"💾 Сохранено в файл: {saved_count}"
            )
            
        except asyncio.CancelledError:
            logger.info(f"⏹️ Задача {task_id} была отменена пользователем")
            # Save unsaved data
            if unsaved_members:
                saved_count = await self._save_users_incremental(task, unsaved_members)
                await self.db.update_parse_task(task_id, saved_count=saved_count)
                logger.info(f"💾 Задача {task_id} - сохранено {len(unsaved_members)} польз. при остановке, всего: {saved_count}")
            await self.db.update_parse_task(task_id, status='paused')
            
            # Notify user
            await self._notify_user(
                user_id,
                f"⏸️ **Парсинг приостановлен**\n\n"
                f"📝 Файл: `{task.file_name}`\n"
                f"💾 Сохранено в файл: `{saved_count}` пользователей"
            )
        except Exception as e:
            error_str = str(e)
            logger.error(f"💥 Задача {task_id} завершилась с ошибкой: {error_str}", exc_info=True)
            
            # Check if it's a flood wait error and we can rotate
            is_flood_wait = 'FloodWait' in error_str or 'flood' in error_str.lower()
            
            if is_flood_wait and task.rotate_sessions and len(available_sessions) > 1:
                logger.warning(f"⚠️ Задача {task_id} - обнаружен FloodWait, попытка ротации сессии...")
                
                # Save unsaved data first
                if unsaved_members:
                    try:
                        saved_count = await self._save_users_incremental(task, unsaved_members)
                        await self.db.update_parse_task(task_id, saved_count=saved_count)
                        logger.info(f"💾 Задача {task_id} - сохранено {len(unsaved_members)} польз. перед ротацией")
                    except Exception as save_error:
                        logger.error(f"❌ Не удалось сохранить данные: {save_error}")
                
                # Try rotating to next session
                try:
                    current_index = available_sessions.index(current_session)
                except ValueError:
                    current_index = -1
                
                next_index = (current_index + 1) % len(available_sessions)
                next_session = available_sessions[next_index]
                
                if next_session != current_session:
                    logger.info(f"🔄 Задача {task_id} - ротация из-за FloodWait: {current_session} -> {next_session}")
                    await self.db.update_parse_task(task_id, session_alias=next_session, status='paused')
                    await self._notify_user(
                        user_id,
                        f"⚠️ **FloodWait на сессии {current_session}**\n\n"
                        f"Парсинг приостановлен. Сессия переключена на `{next_session}`.\n"
                        f"Нажмите 'Продолжить' чтобы возобновить парсинг."
                    )
                    return
            
            # Save unsaved data on error
            if unsaved_members:
                try:
                    saved_count = await self._save_users_incremental(task, unsaved_members)
                    await self.db.update_parse_task(task_id, saved_count=saved_count)
                    logger.info(f"💾 Задача {task_id} - сохранено {len(unsaved_members)} польз. при ошибке, всего: {saved_count}")
                except Exception as save_error:
                    logger.error(f"❌ Не удалось сохранить данные при ошибке: {save_error}")
            
            await self.db.update_parse_task(
                task_id,
                status='failed',
                error_message=error_str
            )
            await self._notify_user(
                user_id,
                f"❌ **Ошибка парсинга!**\n\n"
                f"Задача: `{task.file_name}`\n"
                f"Ошибка: `{error_str}`"
            )
        finally:
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
            if task_id in self.task_unsaved_members:
                del self.task_unsaved_members[task_id]
            if task_id in self.task_metadata:
                del self.task_metadata[task_id]

    async def _run_channel_comments_parse_task(self, task: ParseTask):
        """Execute channel comments parse task - parses users from comments under channel posts.
        
        This method works specifically for channels:
        1. Iterates through channel posts (message history)
        2. For each post, gets discussion/comments (replies)
        3. Extracts users who commented
        4. Applies keyword/exclude filters to comment text
        5. Saves matching users incrementally to file
        """
        # Force disable admin/inactive filters for channel comments as they are not relevant/needed
        task.filter_admins = False
        task.filter_inactive = False

        task_id = task.id
        user_id = task.user_id
        unsaved_members: List[Dict] = []
        
        try:
            logger.info(f"🚀 Запуск задачи парсинга комментариев канала {task_id}: {task.file_name}")
            logger.info(f"📢 Канал: {task.source_group_title}")
            logger.info(f"🔑 Ключевые слова: {task.keyword_filter if task.keyword_filter else 'нет'}")
            logger.info(f"🚫 Слова для исключения: {task.exclude_keywords if task.exclude_keywords else 'нет'}")
            logger.info(f"📊 Лимит постов: {task.messages_limit if task.messages_limit else 'без лимита'}")
            logger.info(f"💾 Сохранять каждые: {task.save_every_users} уникальных пользователей" if task.save_every_users > 0 else "💾 Сохранение: в конце")
            logger.info(f"⏱️ Задержка: {task.delay_seconds} сек каждые {task.delay_every_requests} запросов")
            logger.info(f"🔄 Ротация: каждые {task.rotate_every_requests} запросов" if task.rotate_every_requests > 0 else "🔄 Ротация: только при ошибках")
            
            # Get available sessions
            available_sessions = task.available_sessions.copy() if task.available_sessions else []
            
            if not available_sessions:
                parsing_sessions = await self.db.get_sessions_for_task("parsing")
                available_sessions = [s.alias for s in parsing_sessions if s.alias]
                
                if not available_sessions:
                    all_sessions = await self.session_manager.list_sessions()
                    available_sessions = [s['alias'] for s in all_sessions if s.get('alias')]
                
                if not available_sessions:
                    raise Exception("No sessions available for parsing")
                
                await self.db.update_parse_task(task_id, available_sessions=available_sessions)
            
            # Remove failed sessions
            available_sessions = [s for s in available_sessions if s and s not in task.failed_sessions]
            if not available_sessions:
                raise Exception("All sessions have failed")
            
            # Use current session or first available
            current_session = task.session_alias if task.session_alias in available_sessions else available_sessions[0]
            
            # Get proxy info
            proxy_info = await self.session_manager.get_proxy_info(current_session, task.use_proxy)
            proxy_str = f" через прокси {proxy_info}" if proxy_info else " без прокси"
            
            # Get client
            client = await self.session_manager.get_client(current_session, use_proxy=task.use_proxy)
            if not client:
                raise Exception(f"Session {current_session} not available")
            
            logger.info(f"✅ Задача {task_id} - клиент получен для сессии {current_session}{proxy_str}")
            
            # Initialize counters
            total_unique_users = 0  # Total unique users found (matching keywords/exclude criteria)
            saved_count = task.saved_count
            processed_posts = task.messages_offset  # Resume from where we left off
            api_requests_count = 0  # Count API requests for delay/rotation
            
            # Load already saved user IDs to skip duplicates
            already_saved_ids = self.user_files_manager.get_saved_user_ids(task.file_name)
            if already_saved_ids:
                logger.info(f"📂 Задача {task_id} - загружено {len(already_saved_ids)} уже сохранённых ID из файла (пропустим)")
            
            # Track seen users in this run to avoid duplicates within same run
            seen_user_ids = set()
            
            logger.info(f"📊 Задача {task_id} - начало итерации по постам канала {task.source_group_title}")
            if processed_posts > 0:
                logger.info(f"🔄 Задача {task_id} - продолжение с поста #{processed_posts}")
            
            # Join channel if needed
            await self.session_manager.join_chat_if_needed(
                client, 
                task.source_group_id, 
                task.source_username
            )
            
            # Iterate through channel posts
            async for post in client.get_chat_history(task.source_group_id, offset=task.messages_offset):
                # Check if task was cancelled
                if task_id not in self.running_tasks:
                    logger.info(f"⏹️ Задача {task_id} была отменена")
                    break
                
                processed_posts += 1
                
                # Log progress every 10 posts
                if processed_posts % 10 == 0:
                    logger.info(
                        f"📨 Задача {task_id} - обработано {processed_posts} постов, "
                        f"найдено {total_unique_users} уникальных пользователей, "
                        f"сохранено в файл: {saved_count}"
                    )
                
                # Check posts limit
                if task.messages_limit and processed_posts >= task.messages_limit:
                    logger.info(f"✅ Задача {task_id} - достигнут лимит по постам: {task.messages_limit}")
                    break
                
                # Count API requests
                api_requests_count += 1
                
                # Check for delay after N requests
                if task.delay_every_requests > 0 and api_requests_count % task.delay_every_requests == 0:
                    logger.info(f"⏱️ Задача {task_id} - задержка {task.delay_seconds} сек после {api_requests_count} запросов")
                    await asyncio.sleep(task.delay_seconds)
                
                # Check for session rotation after N requests
                if task.rotate_sessions and task.rotate_every_requests > 0 and api_requests_count % task.rotate_every_requests == 0:
                    if len(available_sessions) > 1:
                        logger.info(f"🔄 Задача {task_id} - ротация сессии после {api_requests_count} запросов...")
                        
                        try:
                            current_index = available_sessions.index(current_session)
                        except ValueError:
                            current_index = -1
                        
                        next_index = (current_index + 1) % len(available_sessions)
                        next_session = available_sessions[next_index]
                        
                        if next_session != current_session:
                            new_proxy_info = await self.session_manager.get_proxy_info(next_session, task.use_proxy)
                            new_proxy_str = f" через прокси {new_proxy_info}" if new_proxy_info else " без прокси"
                            
                            logger.info(f"🔄 Задача {task_id} - ротация: {current_session}{proxy_str} -> {next_session}{new_proxy_str}")
                            current_session = next_session
                            proxy_str = new_proxy_str
                            
                            await self.db.update_parse_task(task_id, session_alias=current_session)
                            
                            client = await self.session_manager.get_client(current_session, use_proxy=task.use_proxy)
                            if not client:
                                raise Exception(f"Failed to get client for session {current_session}")
                
                # Check replies count if available (Pyrogram Message may not have .replies in all versions)
                replies_obj = getattr(post, 'replies', None)
                replies_count = getattr(replies_obj, 'replies', None) if replies_obj is not None else None
                if replies_count is not None and replies_count == 0:
                    logger.debug(f"📭 Задача {task_id} - пост {post.id} не имеет комментариев, пропуск")
                    continue
                if replies_count is not None:
                    logger.info(f"💬 Задача {task_id} - пост {post.id} имеет {replies_count} комментариев, обработка...")
                else:
                    logger.info(f"💬 Задача {task_id} - пост {post.id}, запрос комментариев...")
                
                try:
                    # Get discussion/comments for this post
                    # Note: Pyrogram uses get_discussion_replies to get comments
                    async for comment in client.get_discussion_replies(
                        chat_id=task.source_group_id,
                        message_id=post.id
                    ):
                        # Get comment author
                        user = comment.from_user
                        if not user or user.is_bot:
                            continue
                        
                        comment_user_id = user.id
                        
                        # Skip if already saved or seen in this run
                        if comment_user_id in already_saved_ids or comment_user_id in seen_user_ids:
                            continue
                        
                        # Get comment text
                        comment_text = comment.text or comment.caption or ""
                        comment_text_lower = comment_text.lower()
                        
                        # Check for keywords
                        has_keyword = False
                        if task.keyword_filter:
                            for keyword in task.keyword_filter:
                                if keyword.lower() in comment_text_lower:
                                    has_keyword = True
                                    break
                        else:
                            # No keyword filter = all match
                            has_keyword = True
                        
                        if not has_keyword:
                            continue
                        
                        # Check for exclude keywords
                        has_exclude = False
                        if task.exclude_keywords:
                            for exclude_word in task.exclude_keywords:
                                if exclude_word.lower() in comment_text_lower:
                                    has_exclude = True
                                    break
                        
                        if has_exclude:
                            continue
                        
                        # User matches keyword criteria - mark as seen and add to unique count
                        seen_user_ids.add(comment_user_id)
                        total_unique_users += 1
                        
                        user_data = {
                            'id': comment_user_id,
                            'username': user.username,
                            'first_name': user.first_name,
                            'last_name': user.last_name
                        }
                        user_info = f"{user_data.get('username') or user_data.get('id')}"
                        
                        # Apply admin filter if enabled
                        if task.filter_admins:
                            try:
                                source_member = await client.get_chat_member(task.source_group_id, comment_user_id)
                                raw_status = source_member.status
                                status_str = getattr(raw_status, "name", str(raw_status)).upper()
                                
                                if status_str in ['ADMINISTRATOR', 'CREATOR', 'OWNER']:
                                    logger.info(f"🔍 Задача {task_id} - пропуск админа: {user_info}")
                                    continue
                            except Exception as e:
                                logger.warning(f"🔍 Задача {task_id} - не удалось проверить статус админа для {user_info}: {e}")
                        
                        # Apply inactive filter if enabled
                        if task.filter_inactive and task.inactive_threshold_days is not None:
                            try:
                                users = await client.get_users([comment_user_id])
                                if users:
                                    user_obj = users[0]
                                    user_last_online = getattr(user_obj, 'last_online_date', None)
                                    
                                    if user_last_online is not None:
                                        from datetime import datetime, timedelta
                                        threshold_date = datetime.now() - timedelta(days=task.inactive_threshold_days)
                                        
                                        if user_last_online < threshold_date:
                                            days_since_online = (datetime.now() - user_last_online).days
                                            logger.info(f"🔍 Задача {task_id} - пропуск неактивного: {user_info} ({days_since_online} дн.)")
                                            continue
                            except Exception as e:
                                logger.warning(f"🔍 Задача {task_id} - не удалось проверить активность для {user_info}: {e}")
                        
                        # User passed all filters - add to unsaved list
                        unsaved_members.append(user_data)
                        self.task_unsaved_members[task_id] = unsaved_members
                        already_saved_ids.add(comment_user_id)
                        
                        logger.info(f"✅ Задача {task_id} - найден пользователь #{total_unique_users}: {user_info} (из комментариев)")
                        
                        # Update messages offset in DB for resume capability
                        await self.db.update_parse_task(task_id, messages_offset=processed_posts, parsed_count=total_unique_users)
                        
                        # Check for incremental save by unique users found
                        if task.save_every_users > 0 and len(unsaved_members) >= task.save_every_users:
                            saved_count = await self._save_users_incremental(task, unsaved_members)
                            await self.db.update_parse_task(task_id, saved_count=saved_count)
                            unsaved_members = []
                            self.task_unsaved_members[task_id] = unsaved_members
                            logger.info(f"💾 Задача {task_id} - сохранено {len(unsaved_members)} польз., всего в файле: {saved_count}")
                
                except Exception as e:
                    logger.error(f"❌ Задача {task_id} - ошибка при обработке комментариев поста {post.id}: {e}")
                    # Continue with next post
                    continue
            
            # Save remaining users
            if unsaved_members:
                saved_count = await self._save_users_incremental(task, unsaved_members)
                await self.db.update_parse_task(task_id, saved_count=saved_count)
                logger.info(f"💾 Задача {task_id} - финальное сохранение: всего в файле {saved_count} польз.")
                unsaved_members = []
                self.task_unsaved_members[task_id] = unsaved_members
            
            # Mark as completed
            await self.db.update_parse_task(task_id, status='completed')
            logger.info(f"🎉 Задача {task_id} завершена: обработано {processed_posts} постов, найдено {total_unique_users} пользователей.")
            
            # Notify user
            await self._notify_user(
                user_id,
                f"✅ **Парсинг завершен!**\n\n"
                f"📝 Файл: `{task.file_name}`\n"
                f"📢 Канал: {task.source_group_title}\n"
                f"📋 Режим: из комментариев канала\n"
                f"📨 Обработано постов: {processed_posts}\n"
                f"👥 Найдено пользователей: {total_unique_users}\n"
                f"💾 Сохранено в файл: {saved_count}"
            )
            
        except asyncio.CancelledError:
            logger.info(f"⏹️ Задача {task_id} была отменена пользователем")
            # Save unsaved data
            if unsaved_members:
                saved_count = await self._save_users_incremental(task, unsaved_members)
                await self.db.update_parse_task(task_id, saved_count=saved_count)
                logger.info(f"💾 Задача {task_id} - сохранено {len(unsaved_members)} польз. при остановке, всего: {saved_count}")
            await self.db.update_parse_task(task_id, status='paused')
            
            # Notify user
            await self._notify_user(
                user_id,
                f"⏸️ **Парсинг приостановлен**\n\n"
                f"📝 Файл: `{task.file_name}`\n"
                f"💾 Сохранено в файл: `{saved_count}` пользователей"
            )
        except Exception as e:
            error_str = str(e)
            logger.error(f"💥 Задача {task_id} завершилась с ошибкой: {error_str}", exc_info=True)
            
            # Check if it's a flood wait error and we can rotate
            is_flood_wait = 'FloodWait' in error_str or 'flood' in error_str.lower()
            
            if is_flood_wait and task.rotate_sessions and len(available_sessions) > 1:
                logger.warning(f"⚠️ Задача {task_id} - обнаружен FloodWait, попытка ротации сессии...")
                
                # Save unsaved data first
                if unsaved_members:
                    try:
                        saved_count = await self._save_users_incremental(task, unsaved_members)
                        await self.db.update_parse_task(task_id, saved_count=saved_count)
                        logger.info(f"💾 Задача {task_id} - сохранено {len(unsaved_members)} польз. перед ротацией")
                    except Exception as save_error:
                        logger.error(f"❌ Не удалось сохранить данные: {save_error}")
                
                # Try rotating to next session
                try:
                    current_index = available_sessions.index(current_session)
                except ValueError:
                    current_index = -1
                
                next_index = (current_index + 1) % len(available_sessions)
                next_session = available_sessions[next_index]
                
                if next_session != current_session:
                    logger.info(f"🔄 Задача {task_id} - ротация из-за FloodWait: {current_session} -> {next_session}")
                    await self.db.update_parse_task(task_id, session_alias=next_session, status='paused')
                    await self._notify_user(
                        user_id,
                        f"⚠️ **FloodWait на сессии {current_session}**\n\n"
                        f"Парсинг приостановлен. Сессия переключена на `{next_session}`.\n"
                        f"Нажмите 'Продолжить' чтобы возобновить парсинг."
                    )
                    return
            
            # Save unsaved data on error
            if unsaved_members:
                try:
                    saved_count = await self._save_users_incremental(task, unsaved_members)
                    await self.db.update_parse_task(task_id, saved_count=saved_count)
                    logger.info(f"💾 Задача {task_id} - сохранено {len(unsaved_members)} польз. при ошибке, всего: {saved_count}")
                except Exception as save_error:
                    logger.error(f"❌ Не удалось сохранить данные при ошибке: {save_error}")
            
            await self.db.update_parse_task(
                task_id,
                status='failed',
                error_message=error_str
            )
            await self._notify_user(
                user_id,
                f"❌ **Ошибка парсинга!**\n\n"
                f"Задача: `{task.file_name}`\n"
                f"Ошибка: `{error_str}`"
            )
        finally:
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
            if task_id in self.task_unsaved_members:
                del self.task_unsaved_members[task_id]
            if task_id in self.task_metadata:
                del self.task_metadata[task_id]

