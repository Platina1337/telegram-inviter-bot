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
        # Simple in-memory cache for validation results
        self._validation_cache = {}  # {session_alias: {file_hash: (capabilities, timestamp)}}
    
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
    
    def _load_file_data(self, file_source: str) -> Optional[Dict[str, Any]]:
        """Load full file data (users + metadata) for file-based validation."""
        if not file_source:
            return None
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
            from user_files_manager import UserFilesManager
            manager = UserFilesManager()
            return manager.load_users_from_file(file_source)
        except Exception as e:
            logger.error(f"Error loading file {file_source}: {e}")
            return None

    def _get_sample_and_metadata(self, file_source: str, sample_size: int = 10) -> tuple:
        """Load file once and return (sample_users, metadata) for file-based validation."""
        file_data = self._load_file_data(file_source)
        if not file_data:
            return [], {}
        users = file_data.get('users', [])
        metadata = file_data.get('metadata', {})
        if not users:
            return [], metadata
        users_with_id = [u for u in users if u.get('id')]
        if len(users_with_id) >= sample_size:
            import random
            sample = random.sample(users_with_id, min(sample_size, len(users_with_id)))
        else:
            users_with_username = [u for u in users if u.get('username') and u not in users_with_id]
            combined = users_with_id + users_with_username[:sample_size - len(users_with_id)]
            sample = combined[:sample_size]
        return sample, metadata

    async def _load_sample_users_from_file(self, file_source: str, sample_size: int = 10) -> List[Dict[str, Any]]:
        """Load a sample of users from file for PEER_ID validation."""
        sample, _ = self._get_sample_and_metadata(file_source, sample_size)
        return sample
    
    def _create_smart_sample(self, file_users: List[Dict[str, Any]], loaded_members: Dict[int, Any], 
                           sample_size: int = 10) -> tuple:
        """
        Создает умную выборку тестовых пользователей из пересечения файла и загруженных участников.
        
        Returns:
            tuple: (sample_users, strategy_used, stats)
        """
        import random
        
        # Создаем множества ID для быстрого поиска пересечений
        file_user_ids = set()
        file_users_dict = {}
        
        for user in file_users:
            user_id = user.get('id')
            if user_id:
                file_user_ids.add(user_id)
                file_users_dict[user_id] = user
        
        loaded_member_ids = set(loaded_members.keys())
        
        # Находим пересечение
        intersection_ids = file_user_ids.intersection(loaded_member_ids)
        
        stats = {
            'file_users_total': len(file_users),
            'file_users_with_id': len(file_user_ids),
            'loaded_members': len(loaded_members),
            'intersection_size': len(intersection_ids)
        }
        
        # Стратегия 1: Умная выборка из пересечения (приоритет)
        if len(intersection_ids) >= sample_size:
            selected_ids = random.sample(list(intersection_ids), sample_size)
            sample_users = [file_users_dict[user_id] for user_id in selected_ids]
            return sample_users, "smart_intersection", stats
        
        # Стратегия 2: Частичное пересечение + дополнение из файла
        elif len(intersection_ids) > 0:
            # Берем все из пересечения
            intersection_users = [file_users_dict[user_id] for user_id in intersection_ids]
            
            # Дополняем из файла (исключая уже взятых)
            remaining_file_users = [u for u in file_users 
                                  if u.get('id') not in intersection_ids and u.get('id')]
            
            needed = sample_size - len(intersection_users)
            if len(remaining_file_users) >= needed:
                additional_users = random.sample(remaining_file_users, needed)
            else:
                additional_users = remaining_file_users
            
            sample_users = intersection_users + additional_users
            return sample_users, "partial_intersection", stats
        
        # Стратегия 3: Fallback к обычной выборке из файла
        else:
            users_with_id = [u for u in file_users if u.get('id')]
            if len(users_with_id) >= sample_size:
                sample_users = random.sample(users_with_id, sample_size)
            else:
                sample_users = users_with_id
            return sample_users, "file_only", stats
    
    async def _introduce_session_to_file_users(self, client, file_users: List[Dict[str, Any]], 
                                             introduction_size: int = 20) -> Dict[str, int]:
        """
        Предварительно 'знакомит' сессию с пользователями из файла через get_users.
        Это добавляет их в peer cache и может улучшить доступность.
        Использует id и/или username (тег): сначала по id, при ошибке — по username.
        
        Returns:
            Dict[str, int]: Статистика введения {'introduced': count, 'errors': count}
        """
        stats = {'introduced': 0, 'errors': 0}
        
        # Выбираем случайных пользователей для знакомства (с id или username)
        import random
        users_with_id_or_username = [u for u in file_users if u.get('id') or u.get('username')]
        if not users_with_id_or_username:
            return stats
        users_to_introduce = random.sample(
            users_with_id_or_username, 
            min(introduction_size, len(users_with_id_or_username))
        )
        
        logger.info(f"🔍 [USER_INTRODUCTION] Introducing session to {len(users_to_introduce)} file users...")
        
        for user in users_to_introduce:
            user_id = user.get('id')
            username = user.get('username')
            introduced = False
            
            # Попытка по id
            if user_id:
                try:
                    await client.get_users(user_id)
                    stats['introduced'] += 1
                    introduced = True
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.debug(f"🔍 [USER_INTRODUCTION] Failed by id {user_id}: {e}")
                    # При ошибке по id пробуем по username, если есть
                    if username:
                        try:
                            target = username if username.startswith('@') else f"@{username}"
                            await client.get_users(target)
                            stats['introduced'] += 1
                            introduced = True
                            await asyncio.sleep(0.1)
                        except Exception as e2:
                            stats['errors'] += 1
                            logger.debug(f"🔍 [USER_INTRODUCTION] Failed by username {target}: {e2}")
                    else:
                        stats['errors'] += 1
            # Нет id — пробуем по username
            elif username:
                try:
                    target = username if username.startswith('@') else f"@{username}"
                    await client.get_users(target)
                    stats['introduced'] += 1
                    introduced = True
                    await asyncio.sleep(0.1)
                except Exception as e:
                    stats['errors'] += 1
                    logger.debug(f"🔍 [USER_INTRODUCTION] Failed to introduce user {username}: {e}")
        
        logger.info(f"🔍 [USER_INTRODUCTION] Introduction complete: {stats['introduced']} successful, {stats['errors']} errors")
        return stats
    
    def _get_cached_validation(self, alias: str, file_hash: str) -> Optional[SessionCapabilities]:
        """Get cached validation result if available and not expired."""
        if not file_hash or alias not in self._validation_cache:
            return None
        
        cache_entry = self._validation_cache[alias].get(file_hash)
        if not cache_entry:
            return None
        
        capabilities, timestamp = cache_entry
        
        # Check if cache is not expired (1 hour)
        from datetime import datetime, timedelta
        cache_time = datetime.fromisoformat(timestamp)
        if datetime.now() - cache_time > timedelta(hours=1):
            # Remove expired entry
            del self._validation_cache[alias][file_hash]
            if not self._validation_cache[alias]:
                del self._validation_cache[alias]
            return None
        
        logger.info(f"🔍 [CACHE_HIT] Using cached validation for session {alias} (file hash: {file_hash})")
        return capabilities
    
    def _cache_validation_result(self, alias: str, file_hash: str, capabilities: SessionCapabilities):
        """Cache validation result for future use."""
        if not file_hash:
            return
        
        if alias not in self._validation_cache:
            self._validation_cache[alias] = {}
        
        timestamp = datetime.now().isoformat()
        self._validation_cache[alias][file_hash] = (capabilities, timestamp)
        logger.debug(f"🔍 [CACHE_STORE] Cached validation for session {alias} (file hash: {file_hash})")
    
    async def _validate_session_for_file_users(
        self, alias: str, target_group_id: int, target_username: str,
        sample_users: List[Dict[str, Any]], use_proxy: bool = True, auto_join: bool = True,
        source_group_id: Optional[int] = None, source_username: Optional[str] = None,
        auto_join_source: bool = True, file_users: Optional[List[Dict[str, Any]]] = None
    ) -> SessionCapabilities:
        """
        Адаптивная валидация сессии для файлового инвайтинга с несколькими стратегиями.
        
        Стратегии валидации:
        1. Smart sampling - умная выборка из пересечения загруженных участников и файла
        2. Introduction - предварительное знакомство с пользователями файла
        3. Standard - обычная валидация
        """
        capabilities = SessionCapabilities(last_validated=datetime.now().isoformat())
        
        try:
            client = await self.session_manager.get_client(alias, use_proxy=use_proxy)
            if not client or not client.is_connected:
                capabilities.target_access_error = "Session not connected"
                capabilities.file_users_error = "Session not connected"
                return capabilities
            
            # Test target access with auto-join option
            await self._test_target_access(client, target_group_id, target_username, capabilities, alias, auto_join)
            
            loaded_members = {}
            validation_strategy = "standard"
            resolved_source_id = None  # ID группы для загрузки участников (после входа)
            
            # Попытка вступить в группу-источник для умной проверки (не обязательно — валидация пройдёт в любом случае)
            # Порядок: сначала по id, потом по username из файла (source_username)
            if auto_join_source and (source_group_id is not None and source_group_id != -1 or source_username):
                joined = False
                # 1. Пробуем по id
                if source_group_id is not None and source_group_id != -1:
                    joined, join_err = await self.session_manager.join_chat_if_needed(
                        client, source_group_id, None  # только по id
                    )
                    if joined:
                        resolved_source_id = source_group_id
                        logger.info(f"🔍 [AUTO_JOIN_SOURCE] Session {alias} joined source by id {source_group_id}")
                # 2. Не получилось — пробуем по username из файла (Gruzchempoin, @Gruzchempoin)
                if not joined and source_username:
                    username_clean = source_username.strip()
                    if not username_clean.startswith('@'):
                        username_clean = f"@{username_clean}"
                    try:
                        chat = await client.join_chat(username_clean)
                        resolved_source_id = getattr(chat, 'id', None) if chat else None
                        joined = resolved_source_id is not None
                        if joined:
                            logger.info(f"🔍 [AUTO_JOIN_SOURCE] Session {alias} joined source by username {username_clean}")
                    except Exception as e:
                        logger.debug(f"🔍 [AUTO_JOIN_SOURCE] Session {alias} could not join by username {username_clean}: {e}")
                
                if joined and resolved_source_id:
                    await asyncio.sleep(2)
                    loaded_members = await self._load_source_members_into_cache(client, resolved_source_id, alias)
                    capabilities.loaded_source_members = len(loaded_members)
                    await asyncio.sleep(1)
            
            # Адаптивная стратегия валидации
            final_sample = sample_users
            
            # Стратегия 1: Smart sampling (если есть загруженные участники и полный список файла)
            if loaded_members and file_users:
                smart_sample, strategy, stats = self._create_smart_sample(file_users, loaded_members, sample_size=10)
                if strategy in ["smart_intersection", "partial_intersection"]:
                    final_sample = smart_sample
                    validation_strategy = f"smart_sampling_{strategy}"
                    logger.info(f"🔍 [SMART_SAMPLING] Session {alias}: Using {strategy} strategy. "
                              f"Intersection: {stats['intersection_size']}/{stats['file_users_with_id']} users")
            
            # Стратегия 2: Introduction (если нет хорошего пересечения)
            if validation_strategy == "standard" and file_users:
                # Предварительное знакомство с пользователями файла
                intro_stats = await self._introduce_session_to_file_users(client, file_users, introduction_size=30)
                if intro_stats['introduced'] > 0:
                    validation_strategy = "introduction"
                    await asyncio.sleep(2)  # Пауза после знакомства
            
            # Test file users access with chosen strategy
            await self._test_file_users_access(client, final_sample, capabilities, alias, validation_strategy)
            
        except Exception as e:
            logger.error(f"Error validating file capabilities for {alias}: {e}")
            capabilities.target_access_error = str(e)
            capabilities.file_users_error = str(e)
        
        return capabilities

    async def _load_source_members_into_cache(self, client, source_group_id: int, alias: str, limit: int = 1000) -> Dict[int, Any]:
        """
        Загружает участников группы-источника в peer cache сессии и возвращает их для умной выборки.
        После этого get_users() для этих пользователей не даёт PEER_ID_INVALID.
        
        Returns:
            Dict[int, Any]: Словарь {user_id: user_info} загруженных участников
        """
        loaded_members = {}
        try:
            count = 0
            async for member in client.get_chat_members(source_group_id, limit=limit):
                if hasattr(member, 'user') and member.user:
                    user_id = member.user.id
                    loaded_members[user_id] = {
                        'id': user_id,
                        'username': getattr(member.user, 'username', None),
                        'first_name': getattr(member.user, 'first_name', ''),
                        'last_name': getattr(member.user, 'last_name', ''),
                        'is_bot': getattr(member.user, 'is_bot', False)
                    }
                    count += 1
            
            if count > 0:
                logger.info(f"🔍 [AUTO_JOIN_SOURCE] Session {alias}: загружено {count} участников группы-источника в кэш")
            return loaded_members
        except Exception as e:
            err = str(e).lower()
            if "chat_admin_required" in err:
                logger.debug(f"🔍 [AUTO_JOIN_SOURCE] Session {alias}: нет прав на список участников (только админ)")
            else:
                logger.debug(f"🔍 [AUTO_JOIN_SOURCE] Session {alias}: не удалось загрузить участников источника: {e}")
            return {}
    
    async def _test_file_users_access(self, client, sample_users: List[Dict[str, Any]], 
                                    capabilities: SessionCapabilities, alias: str, 
                                    validation_strategy: str = "standard"):
        """Test if session can access users from file (PEER_ID validation) with detailed metrics."""
        if not sample_users:
            capabilities.file_users_error = "No sample users to test"
            return
        
        accessible_count = 0
        total_tested = 0
        peer_errors = 0
        privacy_errors = 0
        other_errors = 0
        
        capabilities.validation_strategy = validation_strategy
        
        logger.info(f"🔍 [FILE_VALIDATION] Session {alias} testing access to {len(sample_users)} sample users (strategy: {validation_strategy})")
        
        # Тестируем до 10 пользователей для более точной статистики
        test_limit = min(10, len(sample_users))
        
        for user in sample_users[:test_limit]:
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
                    other_errors += 1
                    logger.debug(f"🔍 [FILE_VALIDATION] Session {alias} cannot resolve user {target}")
                    
            except Exception as e:
                error_str = str(e).lower()
                if "peer_id_invalid" in error_str:
                    peer_errors += 1
                    logger.debug(f"🔍 [FILE_VALIDATION] Session {alias} PEER_ID_INVALID for user {target}")
                elif "user_privacy_restricted" in error_str or "privacy" in error_str:
                    privacy_errors += 1
                    logger.debug(f"🔍 [FILE_VALIDATION] Session {alias} privacy restricted for user {target}")
                elif "user_not_found" in error_str:
                    other_errors += 1
                    logger.debug(f"🔍 [FILE_VALIDATION] Session {alias} user not found: {target}")
                else:
                    other_errors += 1
                    logger.debug(f"🔍 [FILE_VALIDATION] Session {alias} error accessing user {target}: {e}")
        
        # Сохраняем детальную статистику
        capabilities.tested_file_users = total_tested
        capabilities.accessible_file_users = accessible_count
        capabilities.peer_id_errors = peer_errors
        capabilities.privacy_errors = privacy_errors
        capabilities.other_errors = other_errors
        
        # Определяем доступность с более гибкими критериями
        if total_tested == 0:
            capabilities.file_users_error = "No valid users to test"
        elif accessible_count == 0:
            if peer_errors == total_tested:
                capabilities.file_users_error = f"All {peer_errors} tested users have PEER_ID_INVALID (session doesn't know these users)"
            elif privacy_errors > 0:
                capabilities.file_users_error = f"All tested users are inaccessible: {peer_errors} PEER_ID, {privacy_errors} privacy, {other_errors} other"
            else:
                capabilities.file_users_error = f"Cannot access any of {total_tested} tested users"
        else:
            # Более мягкий порог: 30% вместо 50% для учета privacy ограничений
            access_ratio = accessible_count / total_tested
            if access_ratio >= 0.3:
                capabilities.can_access_file_users = True
                logger.info(f"🔍 [FILE_VALIDATION] Session {alias} can access {accessible_count}/{total_tested} sample users ({access_ratio:.1%})")
            else:
                capabilities.file_users_error = f"Low accessibility: only {accessible_count}/{total_tested} users accessible ({access_ratio:.1%})"
                capabilities.can_access_file_users = False
    
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
        """Generate enhanced summary for file-based validation with detailed metrics."""
        total = len(session_roles)
        valid_count = len(inviters)
        invalid_count = len(invalid)
        
        # Count specific issues with enhanced categorization
        peer_issues = len([r for r in session_roles 
                          if r.capabilities.file_users_error and 'peer_id_invalid' in r.capabilities.file_users_error.lower()])
        
        privacy_issues = len([r for r in session_roles 
                            if r.capabilities.privacy_errors > 0])
        
        # Count auto-join statistics
        auto_joined = len([r for r in session_roles if getattr(r.capabilities, 'auto_joined_target', False)])
        auto_join_failed = len([r for r in session_roles if getattr(r.capabilities, 'auto_join_error', None)])
        
        # Count validation strategies used
        smart_sampling = len([r for r in session_roles 
                            if r.capabilities.validation_strategy and 'smart_sampling' in r.capabilities.validation_strategy])
        introduction = len([r for r in session_roles 
                          if r.capabilities.validation_strategy == 'introduction'])
        
        # Calculate average access ratio for valid sessions
        valid_sessions = [r for r in session_roles if r.role == 'inviter']
        avg_access_ratio = 0
        if valid_sessions:
            total_tested = sum(r.capabilities.tested_file_users for r in valid_sessions)
            total_accessible = sum(r.capabilities.accessible_file_users for r in valid_sessions)
            if total_tested > 0:
                avg_access_ratio = total_accessible / total_tested
        
        summary_parts = []
        if valid_count > 0:
            summary_parts.append(f"{valid_count} пригодных для инвайтинга")
            if avg_access_ratio > 0:
                summary_parts.append(f"(средняя доступность {avg_access_ratio:.1%})")
        
        if smart_sampling > 0:
            summary_parts.append(f"{smart_sampling} умная выборка")
        if introduction > 0:
            summary_parts.append(f"{introduction} с предзнакомством")
        if auto_joined > 0:
            summary_parts.append(f"{auto_joined} автоприсоединено")
        if peer_issues > 0:
            summary_parts.append(f"{peer_issues} с проблемами PEER_ID")
        if privacy_issues > 0:
            summary_parts.append(f"{privacy_issues} с ограничениями приватности")
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
        
        # Load full file data for adaptive validation
        file_data = self._load_file_data(task.file_source)
        if not file_data:
            logger.warning(f"🔍 [FILE_VALIDATION] No file data loaded from {task.file_source}")
            return await self._validate_sessions_basic_file_mode(task, sessions_to_check)
        
        file_users = file_data.get('users', [])
        file_metadata = file_data.get('metadata', {})
        
        # Create initial sample for basic validation
        sample_users, _ = self._get_sample_and_metadata(task.file_source, sample_size=10)
        if not sample_users:
            logger.warning(f"🔍 [FILE_VALIDATION] No sample users loaded from {task.file_source}")
            return await self._validate_sessions_basic_file_mode(task, sessions_to_check)
        
        source_group_id = file_metadata.get('source_group_id')
        source_username = file_metadata.get('source_username') or getattr(task, 'source_username', None)
        auto_join_source = getattr(task, 'auto_join_source', True)
        
        # Generate file hash for caching (simple hash of user IDs)
        file_hash = None
        if file_users:
            import hashlib
            user_ids_str = ','.join(str(u.get('id', '')) for u in file_users[:100])  # First 100 users for hash
            file_hash = hashlib.md5(user_ids_str.encode()).hexdigest()[:8]
        
        if source_group_id is not None and source_group_id != -1:
            logger.info(f"🔍 [FILE_VALIDATION] Source group from file: {source_group_id}, auto_join_source={auto_join_source}")
        
        logger.info(f"🔍 [FILE_VALIDATION] Testing {len(sessions_to_check)} sessions with {len(sample_users)} sample users (file: {len(file_users)} total users)")
        
        for alias in sessions_to_check:
            if not alias:
                continue
                
            try:
                # Check cache first
                cached_capabilities = self._get_cached_validation(alias, file_hash) if file_hash else None
                
                if cached_capabilities:
                    capabilities = cached_capabilities
                else:
                    capabilities = await self._validate_session_for_file_users(
                        alias, task.target_group_id, task.target_username,
                        sample_users, task.use_proxy, getattr(task, 'auto_join_target', True),
                        source_group_id=source_group_id, source_username=source_username,
                        auto_join_source=auto_join_source, file_users=file_users
                    )
                    
                    # Cache the result
                    if file_hash:
                        self._cache_validation_result(alias, file_hash, capabilities)
                
                # Set file hash for reference
                capabilities.file_hash = file_hash
                
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
                
                # Enhanced logging with detailed metrics
                access_ratio = 0
                if capabilities.tested_file_users > 0:
                    access_ratio = capabilities.accessible_file_users / capabilities.tested_file_users
                
                logger.info(f"🔍 [FILE_VALIDATION] Session {alias}: role={role}, "
                          f"target_invite={capabilities.can_invite_to_target}, "
                          f"file_users_access={capabilities.can_access_file_users} "
                          f"({capabilities.accessible_file_users}/{capabilities.tested_file_users} = {access_ratio:.1%})")
                
                if capabilities.validation_strategy:
                    logger.debug(f"🔍 [FILE_VALIDATION] Session {alias} strategy: {capabilities.validation_strategy}, "
                               f"loaded_members: {capabilities.loaded_source_members}, "
                               f"errors: PEER_ID={capabilities.peer_id_errors}, "
                               f"privacy={capabilities.privacy_errors}, other={capabilities.other_errors}")
                
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