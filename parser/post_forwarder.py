# -*- coding: utf-8 -*-
"""
Post Forwarder Worker for parsing and monitoring posts from channels/groups.
Adapted from example/parser/forwarder.py for the inviter bot.
"""
import asyncio
import logging
import re
import httpx
from datetime import datetime
from typing import Dict, Optional, Callable, List, Tuple, Any

from pyrogram import Client
from pyrogram.errors import FloodWait, ChannelPrivate, ChatAdminRequired, ChatWriteForbidden
from pyrogram.types import Message as PyrogramMessage
from pyrogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio

from parser.config import config
from parser.session_manager import SessionManager

logger = logging.getLogger(__name__)


class PostForwarder:
    """Класс для пересылки постов из каналов/групп."""
    
    def __init__(self, db_instance, session_manager: SessionManager = None):
        """
        Initialize PostForwarder.
        
        Args:
            db_instance: Database instance for storing task state
            session_manager: SessionManager instance for managing Telegram sessions
        """
        self.db = db_instance
        self.session_manager = session_manager
        self._parse_tasks: Dict[int, asyncio.Task] = {}  # task_id -> asyncio.Task
        self._monitoring_tasks: Dict[int, asyncio.Task] = {}  # task_id -> asyncio.Task
        # task_id -> {"client": Client, "callback": Callable}
        self._monitoring_handlers: Dict[int, Dict[str, Any]] = {}
        self._stop_flags: Dict[int, bool] = {}  # task_id -> should_stop
        self._last_heartbeat: Dict[int, datetime] = {}  # task_id -> last heartbeat time
        
        
        logger.info("[POST_FORWARDER] PostForwarder initialized")
    
    async def _update_heartbeat_if_needed(self, task_id: int):
        """Update last_heartbeat for task if needed (every 60s)."""
        now = datetime.now()
        last_hb = self._last_heartbeat.get(task_id)
        
        # Check if 60 seconds passed
        if not last_hb or (now - last_hb).total_seconds() > 20:
            self._last_heartbeat[task_id] = now
            timestamp = now.isoformat()
            
            # Since PostForwarder handles both parse and monitoring tasks,
            # we need to know which type it is.
            # But wait, run_post_parse_task and run_post_monitoring_task are separate methods.
            # So I should probably just call separate update queries inside those methods or pass checks.
            # Let's define generic update here and separate specific calls inside.
            # Actually, I can pass task_type argument or handle it inside the caller.
            # But the caller is _run_post_parse_task or _run_post_monitoring_task.
            
            # It's cleaner to have separate methods or pass type.
            pass

    async def _update_heartbeat_parse(self, task_id: int):
        """Update last_heartbeat for parse task."""
        now = datetime.now()
        last_hb = self._last_heartbeat.get(task_id)
        if not last_hb or (now - last_hb).total_seconds() > 20:
            self._last_heartbeat[task_id] = now
            try:
                await self.db.update_post_parse_task(task_id, last_heartbeat=now.isoformat())
            except Exception as e:
                logger.warning(f"[POST_FORWARDER] Failed to update parse task heartbeat: {e}")

    async def _update_heartbeat_monitoring(self, task_id: int):
        """Update last_heartbeat for monitoring task."""
        now = datetime.now()
        last_hb = self._last_heartbeat.get(task_id)
        if not last_hb or (now - last_hb).total_seconds() > 20:
            self._last_heartbeat[task_id] = now
            try:
                await self.db.update_post_monitoring_task(task_id, last_heartbeat=now.isoformat())
            except Exception as e:
                logger.warning(f"[POST_FORWARDER] Failed to update monitoring task heartbeat: {e}")
    
    async def _get_client(self, session_alias: str, use_proxy: bool = True) -> Optional[Client]:
        """Get Pyrogram client for the given session."""
        if not self.session_manager:
            logger.error("[POST_FORWARDER] SessionManager not available")
            return None
        
        try:
            client = await self.session_manager.get_client(session_alias, use_proxy=use_proxy)
            if client and not client.is_connected:
                await client.start()
            return client
        except Exception as e:
            logger.error(f"[POST_FORWARDER] Error getting client for {session_alias}: {e}")
            return None
    
    async def _release_client(self, session_alias: str):
        """Release client back to session manager."""
        if self.session_manager:
            try:
                await self.session_manager.release_client(session_alias)
            except Exception as e:
                logger.warning(f"[POST_FORWARDER] Error releasing client {session_alias}: {e}")
    
    async def _notify_user(self, user_id: int, message: str):
        """Send notification to user via Telegram Bot API (like inviter)."""
        if not getattr(config, 'BOT_TOKEN', None):
            logger.warning("[POST_FORWARDER] BOT_TOKEN not set, skip notification")
            return
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"https://api.telegram.org/bot{config.BOT_TOKEN}/sendMessage",
                    json={"chat_id": user_id, "text": message, "parse_mode": "Markdown"},
                    timeout=10.0
                )
        except Exception as e:
            logger.error(f"[POST_FORWARDER] Failed to send notification to user {user_id}: {e}")
    
    def _is_session_error(self, e: Exception) -> bool:
        """True if error is due to session/access (no rights, ban, flood) — then retry same post with other sessions."""
        err_str = str(e).upper()
        if isinstance(e, (FloodWait, ChannelPrivate, ChatAdminRequired, ChatWriteForbidden)):
            return True
        if "403" in err_str or "CHAT_SEND" in err_str or "CHANNEL_PRIVATE" in err_str:
            return True
        if "CHAT_ADMIN_REQUIRED" in err_str or "CHAT_WRITE_FORBIDDEN" in err_str:
            return True
        if "USER_BANNED" in err_str or "AUTH_KEY" in err_str or "SESSION" in err_str:
            return True
        if "FLOOD" in err_str or "PEER_FLOOD" in err_str:
            return True
        return False
    
    def _has_contacts(self, text: str, entities: List = None) -> bool:
        """Check if text contains user tags, phone numbers, or links.
        Also checks message entities for text_link URLs (hidden hyperlinks).
        
        Args:
            text: Message text or caption
            entities: List of MessageEntity objects (optional)
        """
        # 1. Check message entities (hidden links)
        if entities:
            from pyrogram.enums import MessageEntityType
            for entity in entities:
                if entity.type == MessageEntityType.TEXT_LINK and entity.url:
                    # Found a hidden hyperlink
                    return True
                # Also check external links within the entity offset if needed, 
                # but regex below covers visible links.
                # MENTION and PHONE_NUMBER entities are also covered by regex usually,
                # but we can add explicit checks if regex misses them.
                if entity.type in (MessageEntityType.MENTION, MessageEntityType.PHONE_NUMBER, MessageEntityType.EMAIL):
                     return True

        if not text:
            return False
        
        # 2. Check visible text with Regex
        # Check for @mentions
        if re.search(r'@[\w\d_]+', text):
            return True
        
        # Check for phone numbers
        if re.search(r'\+?\d[\d\s\-\(\)]{7,}\d', text):
            return True
        
        # Check for links
        if re.search(r'https?://\S+', text) or re.search(r't\.me/\S+', text):
            return True
        
        return False
    
    def _get_message_entities(self, message: PyrogramMessage) -> List:
        """Return all entities for the message (text + caption). For media with caption, links are in caption_entities."""
        entities = getattr(message, 'entities', None) or []
        caption_entities = getattr(message, 'caption_entities', None) or []
        return list(entities) + list(caption_entities)
    
    def _entities_summary(self, entities: List) -> str:
        """Return short summary of entity types for logging (e.g. 'TEXT_LINK x2, MENTION x1')."""
        if not entities:
            return "none"
        from collections import Counter
        type_names = []
        for e in entities:
            try:
                name = getattr(e.type, "name", None) or str(e.type)
                type_names.append(name)
            except Exception:
                type_names.append("?")
        counts = Counter(type_names)
        return ", ".join(f"{k} x{v}" for k, v in sorted(counts.items()))
    
    def _log_post_content_preview(self, task_id: int, post_messages: List[PyrogramMessage]) -> None:
        """Log what content the parser sees for each message in the post (text/caption preview + entities)."""
        for msg in post_messages:
            text = msg.text or msg.caption or ""
            entities = self._get_message_entities(msg)
            preview = (text[:200] + "…") if len(text) > 200 else text
            preview_escaped = preview.replace("\n", " ").strip() or "(empty)"
            summary = self._entities_summary(entities)
            logger.info(
                f"[POST_FORWARDER] Task {task_id} content preview: msg {msg.id} "
                f"text_len={len(text)} entities=[{summary}] preview={repr(preview_escaped)}"
            )
    
    def _post_has_contacts(self, post_messages: List[PyrogramMessage]) -> bool:
        """Return True if any message in the post has contacts/links (text or caption + entities)."""
        for msg in post_messages:
            text = msg.text or msg.caption or ""
            if self._has_contacts(text, self._get_message_entities(msg)):
                return True
        return False
    
    def _filter_contacts(self, text: str, filter_contacts: bool, remove_contacts: bool) -> str:
        """Filter user tags, phone numbers, and links from text."""
        if not text:
            return text
        
        # Debug: логируем исходный текст и флаги фильтрации (обрезаем до 300 символов)
        try:
            preview = text if len(text) <= 300 else text[:300] + "…"
            logger.debug(
                "[POST_FORWARDER] _filter_contacts: start "
                f"filter_contacts={filter_contacts}, remove_contacts={remove_contacts}, "
                f"orig_len={len(text)}, preview={repr(preview)}"
            )
        except Exception:
            # Не ломаем рабочий поток, если вдруг что‑то пошло не так при логировании
            pass

        if filter_contacts or remove_contacts:
            # Хотим удалить контакты, но сохранить исходное форматирование (переносы строк).
            # Поэтому обрабатываем текст построчно и чистим только "внутри строк".
            lines = text.splitlines(keepends=False)
            cleaned_lines = []
            for line in lines:
                # Remove @mentions
                line = re.sub(r'@[\w\d_]+', '', line)
                
                # Remove phone numbers
                line = re.sub(r'\+?\d[\d\s\-\(\)]{7,}\d', '', line)
                
                # Remove links
                line = re.sub(r'https?://\S+', '', line)
                line = re.sub(r't\.me/\S+', '', line)
                
                # Аккуратно чистим только лишние пробелы в самой строке,
                # не трогая структуру абзацев.
                line = re.sub(r'[ \t]{2,}', ' ', line).strip()
                cleaned_lines.append(line)

            # Собираем обратно, сохраняя исходные переносы
            text = "\n".join(cleaned_lines)

        # Debug: логируем результат фильтрации
        try:
            preview_after = text if len(text) <= 300 else text[:300] + "…"
            logger.debug(
                "[POST_FORWARDER] _filter_contacts: end "
                f"filter_contacts={filter_contacts}, remove_contacts={remove_contacts}, "
                f"result_len={len(text)}, preview={repr(preview_after)}"
            )
        except Exception:
            pass

        return text

    def _generate_signature(
        self,
        message: PyrogramMessage,
        source_title: str,
        source_username: str = None,
        signature_options: dict = None
    ) -> str:
        """Generate signature for forwarded message.
        signature_options: include_post, include_source, include_author (bool),
                          label_post, label_source, label_author (str).
        Backward compat: if label_post missing, label_source is used for post line.
        """
        opts = signature_options or {}
        include_post = opts.get('include_post', False)
        include_source = opts.get('include_source', False)
        include_author = opts.get('include_author', True)
        label_post = (opts.get('label_post') or opts.get('label_source') or 'Ссылка на пост').strip()
        label_source = (opts.get('label_source') or 'Источник').strip()
        label_author = (opts.get('label_author') or 'Обращаться по объявлению сюда:').strip()

        parts = []
        username = source_username or (message.chat.username if message.chat else None)
        clean_username = username.replace('@', '').replace('https://t.me/', '').split('/')[0] if username else None
        source_name = (source_title or username or (message.chat.title if message.chat else None) or 'Source').replace('[', '').replace(']', '')

        if include_post and clean_username:
            post_link = f"https://t.me/{clean_username}/{message.id}"
            parts.append(f"{label_post}: [{source_name}]({post_link})")
        if include_source and clean_username:
            channel_link = f"https://t.me/{clean_username}"
            parts.append(f"{label_source}: [{source_name}]({channel_link})")
        if include_author:
            author_link = ""
            author_name = ""
            if message.from_user:
                author_link = f"https://t.me/{message.from_user.username}" if message.from_user.username else f"tg://user?id={message.from_user.id}"
                author_name = f"{message.from_user.first_name or ''} {message.from_user.last_name or ''}".strip() or "User"
            elif message.sender_chat and getattr(message.sender_chat, 'username', None):
                author_link = f"https://t.me/{message.sender_chat.username}"
                author_name = (message.sender_chat.title or "Channel").replace('[', '').replace(']', '')
            if author_link:
                author_name = author_name.replace('[', '').replace(']', '') if author_name else "User"
                parts.append(f"{label_author} [{author_name}]({author_link})")

        if not parts:
            return ""
        return "\n\n" + "\n".join(parts)
    
    def _message_has_content(self, message: PyrogramMessage) -> bool:
        """Check if message has any content — all possible Pyrogram Message fields.
        Post is not considered empty if any of these is present.
        """
        # Text / caption
        if message.text or message.caption:
            return True
        # Media (all types)
        if (message.photo or message.video or message.document or message.audio or
                message.voice or message.animation or
                getattr(message, 'sticker', None) or getattr(message, 'video_note', None)):
            return True
        if getattr(message, 'media', None):
            return True
        # Formatting / custom emoji / premium
        if getattr(message, 'entities', None):
            return True
        # Link preview
        if getattr(message, 'web_page', None):
            return True
        # Interactive: poll, dice, game
        if getattr(message, 'poll', None) or getattr(message, 'dice', None) or getattr(message, 'game', None):
            return True
        # Location / venue / contact
        if getattr(message, 'venue', None) or getattr(message, 'location', None) or getattr(message, 'contact', None):
            return True
        # Reply keyboard / inline buttons
        if getattr(message, 'reply_markup', None):
            return True
        # Story (if supported in Pyrogram)
        if getattr(message, 'story', None):
            return True
        return False
    
    def _log_message_fields(self, message: PyrogramMessage, task_id: int, context: str = "post"):
        """Log all content-related fields of a message (for debugging service_message_or_empty)."""
        def _val(v):
            if v is None:
                return None
            if isinstance(v, (str, int, bool)):
                return v
            if isinstance(v, list):
                return f"list(len={len(v)})" if v else None
            return type(v).__name__
        
        entities = getattr(message, 'entities', None)
        text_preview = message.text
        if text_preview and len(text_preview) > 100:
            text_preview = text_preview[:100] + "..."
        caption_preview = message.caption
        if caption_preview and len(caption_preview) > 100:
            caption_preview = caption_preview[:100] + "..."
        fields = {
            "id": getattr(message, 'id', None),
            "text": text_preview,
            "caption": caption_preview,
            "media": _val(getattr(message, 'media', None)),
            "photo": _val(getattr(message, 'photo', None)),
            "video": _val(getattr(message, 'video', None)),
            "document": _val(getattr(message, 'document', None)),
            "audio": _val(getattr(message, 'audio', None)),
            "voice": _val(getattr(message, 'voice', None)),
            "animation": _val(getattr(message, 'animation', None)),
            "sticker": _val(getattr(message, 'sticker', None)),
            "video_note": _val(getattr(message, 'video_note', None)),
            "entities": f"list(len={len(entities)})" if entities is not None else None,
            "web_page": _val(getattr(message, 'web_page', None)),
            "poll": _val(getattr(message, 'poll', None)),
            "dice": _val(getattr(message, 'dice', None)),
            "game": _val(getattr(message, 'game', None)),
            "venue": _val(getattr(message, 'venue', None)),
            "location": _val(getattr(message, 'location', None)),
            "contact": _val(getattr(message, 'contact', None)),
            "reply_markup": _val(getattr(message, 'reply_markup', None)),
            "story": _val(getattr(message, 'story', None)),
            "service": getattr(message, 'service', None),
            "media_group_id": getattr(message, 'media_group_id', None),
        }
        parts = [f"{k}={repr(v)}" for k, v in fields.items()]
        logger.info(
            f"[POST_FORWARDER] Task {task_id}: {context} msg {message.id} fields (service_message_or_empty): "
            f"{', '.join(parts)}"
        )
    
    def _should_skip_message(self, message: PyrogramMessage, media_filter: str) -> bool:
        """Check if message should be skipped based on media filter."""
        has_media = bool(message.photo or message.video or message.document or 
                        message.audio or message.voice or message.animation)
        
        if media_filter == 'media_only' and not has_media:
            return True
        if media_filter == 'text_only' and has_media:
            return True
        
        return False
    
    def _check_keywords(self, post_messages: List[PyrogramMessage], whitelist: List[str], blacklist: List[str]) -> bool:
        """Check if post content passes keyword whitelist/blacklist filters.
        
        Args:
            post_messages: List of messages in the post (single or media group)
            whitelist: List of keywords that MUST be present (if empty, ignored)
            blacklist: List of keywords that MUST NOT be present
            
        Returns:
            True if post passes filters, False otherwise.
        """
        if not whitelist and not blacklist:
            return True
            
        # Combine text from all messages in post
        full_text = ""
        for msg in post_messages:
            text = msg.text or msg.caption or ""
            if text:
                full_text += text.lower() + " "
        
        full_text = full_text.strip()
        
        # Check blacklist - if ANY word found, reject
        if blacklist:
            for word in blacklist:
                if word.lower() in full_text:
                    logger.info(f"Post rejected by blacklist keyword: '{word}'")
                    return False
        
        # Check whitelist - if whitelist exists, AT LEAST ONE word must be found
        if whitelist:
            found = False
            for word in whitelist:
                if word.lower() in full_text:
                    found = True
                    break
            
            if not found:
                logger.info("Post rejected: no whitelist keywords found")
                return False
                
        return True
    
    async def start_post_parse_task(self, task_id: int) -> bool:
        """Start a post parsing task."""
        import json
        logger.info(f"[POST_FORWARDER] Starting post parse task {task_id}")
        
        if task_id in self._parse_tasks:
            logger.warning(f"[POST_FORWARDER] Task {task_id} already running")
            return False
            
        task = await self.db.get_post_parse_task(task_id)
        if not task:
            logger.error(f"[POST_FORWARDER] Task {task_id} not found")
            return False
            
        # Validate sessions
        logger.info(f"[POST_FORWARDER] Validating sessions for post parse task {task_id}...")
        validation_result = await self.session_manager.validate_sessions_for_task('post_parse', task)
        valid_sessions = validation_result['valid']
        validation_errors = validation_result['invalid']
        
        await self.db.update_post_parse_task(
            task_id, 
            validated_sessions=valid_sessions,
            validation_errors=json.dumps(validation_errors) if validation_errors else None
        )
        
        if not valid_sessions:
            logger.error(f"[POST_FORWARDER] Task {task_id} failed validation: No valid sessions. Errors: {validation_errors}")
            await self.db.update_post_parse_task(task_id, status='failed', error_message="No valid sessions found.")
            return False

        # При наличии валидных сессий очищаем старое сообщение об ошибке
        # (например, "No valid sessions found." от предыдущего запуска).
        await self.db.update_post_parse_task(task_id, error_message=None)

        # Switch session if needed
        if task.available_sessions and task.session_alias not in valid_sessions:
            if valid_sessions:
                new_session = valid_sessions[0]
                logger.info(f"[POST_FORWARDER] Switching task {task_id} to valid session: {new_session}")
                await self.db.update_post_parse_task(task_id, session_alias=new_session, current_session=new_session)
            else:
                return False

        self._stop_flags[task_id] = False
        self._parse_tasks[task_id] = asyncio.create_task(
            self._run_post_parse_task(task_id)
        )
        return True
    
    async def stop_post_parse_task(self, task_id: int) -> bool:
        """Stop a post parsing task."""
        logger.info(f"[POST_FORWARDER] Stopping post parse task {task_id}")
        
        self._stop_flags[task_id] = True
        
        if task_id in self._parse_tasks:
            task = self._parse_tasks[task_id]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            # Task may have already removed itself in its finally block
            self._parse_tasks.pop(task_id, None)
        
        await self.db.update_post_parse_task(task_id, status='paused')
        return True
    
    async def _run_post_parse_task(self, task_id: int):
        """Run the main post parsing loop."""
        try:
            task = await self.db.get_post_parse_task(task_id)
            if not task:
                logger.error(f"[POST_FORWARDER] Task {task_id} not found")
                return
            # Если текущая сессия не в списке выбранных (настройки сменили) — переключаем на первую из списка
            if task.available_sessions and task.session_alias not in task.available_sessions:
                await self.db.update_post_parse_task(
                    task_id,
                    session_alias=task.available_sessions[0],
                    current_session=task.available_sessions[0]
                )
                task = await self.db.get_post_parse_task(task_id)
            
            await self.db.update_post_parse_task(task_id, status='running')
            
            session_alias = task.session_alias
            candidates = task.validated_sessions if task.validated_sessions else task.available_sessions
            available_sessions = candidates or [session_alias]
            failed_sessions = list(getattr(task, "failed_sessions", []) or [])
            # Try to connect (with rotation if first fails)
            client = None
            found_initial_session = False
            
            # Find start index
            if session_alias in available_sessions:
                current_session_idx = available_sessions.index(session_alias)
            else:
                current_session_idx = 0
                
            start_idx = current_session_idx
            
            for i in range(len(available_sessions)):
                idx = (start_idx + i) % len(available_sessions)
                candidate_alias = available_sessions[idx]
                
                # Check if session is explicitly failed (skip only if we have others to try, or just try anyway?)
                # User logic: "Skip known bad sessions if needed"
                if failed_sessions and candidate_alias in failed_sessions:
                    # If all are failed, we might want to try anyway or just fail? 
                    # Let's try to skip, but if we loop 360 and find nothing, we fail.
                    logger.info(f"[POST_FORWARDER] Skipping failed session {candidate_alias} during init")
                    continue
                
                logger.info(f"[POST_FORWARDER] Task {task_id}: Trying session {candidate_alias}...")
                client = await self._get_client(candidate_alias, task.use_proxy)
                
                if client:
                    session_alias = candidate_alias
                    current_session_idx = idx
                    found_initial_session = True
                    logger.info(f"[POST_FORWARDER] Connected to session {session_alias}")
                    break
                else:
                    logger.warning(f"[POST_FORWARDER] Failed to connect to session {candidate_alias}")
                    if candidate_alias not in failed_sessions:
                        failed_sessions.append(candidate_alias)
            
            # If still no client (e.g. all were in failed_sessions or all connection attempts failed)
            if not found_initial_session:
                # One last desperate try: if we skipped everything because they were in failed_sessions, 
                # maybe we should have tried them? 
                # But for now let's stick to the logic: if all failed, fail task.
                error_msg = f"Не удалось подключить ни одну из {len(available_sessions)} сессий"
                await self.db.update_post_parse_task(
                    task_id, status='failed', error_message=error_msg, failed_sessions=failed_sessions
                )
                await self._notify_user(
                    task.user_id,
                    f"❌ **Задача пересылки постов остановлена**\n\n{error_msg}"
                )
                return
            
            # Update current session in DB
            await self.db.update_post_parse_task(
                task_id,
                current_session=session_alias,
                failed_sessions=failed_sessions
            )
            
            try:
                # Resolve source and target
                source_chat = await client.get_chat(task.source_id)
                target_chat = await client.get_chat(task.target_id)
                
                logger.info(
                    f"[POST_FORWARDER] Task {task_id} settings: "
                    f"Session={session_alias}, Source={source_chat.title} ({task.source_id}), "
                    f"Target={target_chat.title} ({task.target_id}), "
                    f"Limit={task.limit}, Direction={task.parse_direction}, "
                    f"MediaFilter={task.media_filter}, Delay={task.delay_seconds}s every {task.delay_every} posts, "
                    f"Rotation={'On' if task.rotate_sessions else 'Off'} every {task.rotate_every} posts"
                )
                logger.info(
                    f"[POST_FORWARDER] Task {task_id} content options: "
                    f"use_native_forward={getattr(task, 'use_native_forward', False)}, "
                    f"check_content_if_native={getattr(task, 'check_content_if_native', True)}, "
                    f"skip_on_contacts={getattr(task, 'skip_on_contacts', False)}, "
                    f"forward_show_source={getattr(task, 'forward_show_source', True)}, "
                    f"filter_contacts={getattr(task, 'filter_contacts', False)}, "
                    f"remove_contacts={getattr(task, 'remove_contacts', False)}, "
                    f"add_signature={getattr(task, 'add_signature', False)}"
                )
                
                # Initialize counters
                offset_id = task.last_message_id or 0
                forwarded_count = task.forwarded_count
                posts_since_delay = 0
                posts_since_rotate = 0
                
                # Batch processing: load history in chunks until limit reached
                BATCH_SIZE = 100  # Load 100 messages per batch
                history_exhausted = False
                
                while not self._stop_flags.get(task_id):
                    # Update heartbeat and phase
                    await self._update_heartbeat_parse(task_id)
                    await self.db.update_post_parse_task(task_id, worker_phase='forwarding')

                    # Check if limit already reached
                    if task.limit and forwarded_count >= task.limit:
                        logger.info(
                            f"[POST_FORWARDER] Task {task_id}: Limit reached "
                            f"({forwarded_count} posts forwarded)"
                        )
                        break
                    
                    # Fetch next batch of messages
                    messages = []
                    if task.parse_direction == 'backward':
                        # Oldest first - fetch and reverse
                        async for message in client.get_chat_history(
                            task.source_id,
                            limit=BATCH_SIZE,
                            offset_id=offset_id
                        ):
                            if self._stop_flags.get(task_id):
                                break
                            messages.append(message)
                        messages.reverse()
                    else:
                        # Newest first
                        async for message in client.get_chat_history(
                            task.source_id,
                            limit=BATCH_SIZE,
                            offset_id=offset_id
                        ):
                            if self._stop_flags.get(task_id):
                                break
                            messages.append(message)
                    
                    # Check if history is exhausted
                    if not messages:
                        history_exhausted = True
                        logger.info(
                            f"[POST_FORWARDER] Task {task_id}: History exhausted "
                            f"({forwarded_count} posts forwarded)"
                        )
                        break
                    
                    # Group messages into posts (media groups = 1 post, single message = 1 post)
                    posts = []  # List of lists: each inner list is one post
                    media_groups = {}  # media_group_id -> list of messages
                    
                    for message in messages:
                        if message.media_group_id:
                            # Part of media group
                            if message.media_group_id not in media_groups:
                                media_groups[message.media_group_id] = []
                            media_groups[message.media_group_id].append(message)
                        else:
                            # Single message = single post
                            posts.append([message])
                    
                    # Add media groups as posts
                    for group_messages in media_groups.values():
                        posts.append(group_messages)
                    
                    # Sort posts by first message ID: backward = oldest first (asc), forward = newest first (desc)
                    posts.sort(key=lambda p: p[0].id, reverse=(task.parse_direction == 'forward'))
                    
                    batch_posts_count = len(posts)
                    logger.info(
                        f"[POST_FORWARDER] Task {task_id}: Loaded batch of {len(messages)} messages "
                        f"grouped into {batch_posts_count} posts"
                    )
                    
                    # Process posts in this batch
                    for post_idx, post_messages in enumerate(posts, 1):
                        if self._stop_flags.get(task_id):
                            break
                        
                        # Check limit before processing each post
                        if task.limit and forwarded_count >= task.limit:
                            break
                        
                        # Use first message for filtering decisions
                        first_msg = post_messages[0]
                        is_media_group = len(post_messages) > 1
                        
                        # Update offset for next batch
                        offset_id = post_messages[-1].id
                        
                        # Skip service messages (always skip regardless of mode)
                        if getattr(first_msg, 'service', False):
                            logger.info(
                                f"[POST_FORWARDER] Task {task_id}: Skipped post "
                                f"(msg {first_msg.id}, причина: service_message)"
                            )
                            continue
                        
                        # Get native forwarding settings
                        use_native_forward = getattr(task, 'use_native_forward', False)
                        check_content_if_native = getattr(task, 'check_content_if_native', True)
                        
                        # Content checking logic
                        if use_native_forward:
                            # Native forwarding mode: check content only if enabled
                            if check_content_if_native and not self._message_has_content(first_msg):
                                logger.info(
                                    f"[POST_FORWARDER] Task {task_id}: Skipped post "
                                    f"(msg {first_msg.id}, причина: native_forward + check_content_if_native=True, no content)"
                                )
                                self._log_message_fields(first_msg, task_id, context="post")
                                continue
                        else:
                            # Copy mode: always check content
                            if not self._message_has_content(first_msg):
                                logger.info(
                                    f"[POST_FORWARDER] Task {task_id}: Skipped post "
                                    f"(msg {first_msg.id}, причина: copy_mode, service_message_or_empty)"
                                )
                                self._log_message_fields(first_msg, task_id, context="post")
                                continue
                        
                        # Apply filters only in copy mode OR native mode depending on logic
                        # BUT: Keyword filtering applies to BOTH modes if content is available
                        keywords_whitelist = getattr(task, 'keywords_whitelist', [])
                        keywords_blacklist = getattr(task, 'keywords_blacklist', [])
                        
                        if keywords_whitelist or keywords_blacklist:
                             # Check keywords (even in native mode if check_content_if_native is True)
                             if use_native_forward and not check_content_if_native:
                                 pass # Skip check if content checking disabled in native mode
                             else:
                                 if not self._check_keywords(post_messages, keywords_whitelist, keywords_blacklist):
                                     logger.info(
                                         f"[POST_FORWARDER] Task {task_id}: Skipped post "
                                         f"(msg {first_msg.id}, причина: keyword_filter)"
                                     )
                                     continue

                        # Apply filters only in copy mode (native mode ignores these)
                        if not use_native_forward:
                            # Apply media filter (only in copy mode)
                            if self._should_skip_message(first_msg, task.media_filter):
                                has_media = bool(
                                    first_msg.photo or first_msg.video or first_msg.document or
                                    first_msg.audio or first_msg.voice or first_msg.animation
                                )
                                reason = (
                                    f"media_filter={task.media_filter}, сообщение без медиа"
                                    if (task.media_filter == 'media_only' and not has_media)
                                    else f"media_filter={task.media_filter}, в сообщении есть медиа (нужен только текст)"
                                )
                                logger.info(
                                    f"[POST_FORWARDER] Task {task_id}: Skipped post "
                                    f"(msg {first_msg.id}, причина: {reason})"
                                )
                                continue
                            
                            # Check for contacts and skip if skip_on_contacts is enabled (only in copy mode)
                            skip_on_contacts = getattr(task, 'skip_on_contacts', False)
                            if skip_on_contacts and self._post_has_contacts(post_messages):
                                logger.info(
                                    f"[POST_FORWARDER] Task {task_id}: Skipped post "
                                    f"(msg {first_msg.id}, причина: skip_on_contacts=True, обнаружены контакты в тексте)"
                                )
                                continue
                        
                        
                        # Try to forward the post — one post = one unit: retry same post with all sessions on session error (like inviting)
                        post_forwarded = False
                        tried_sessions_this_post = []
                        last_session_error = None
                        session_idx = current_session_idx
                        attempt_client = client
                        attempt_session = session_alias
                        for _ in range(len(available_sessions)):
                            if attempt_session in tried_sessions_this_post:
                                session_idx = (session_idx + 1) % len(available_sessions)
                                attempt_session = available_sessions[session_idx]
                                attempt_client = await self._get_client(attempt_session, task.use_proxy)
                                if not attempt_client:
                                    tried_sessions_this_post.append(attempt_session)
                                    last_session_error = f"Не удалось подключить сессию {attempt_session}"
                                    continue
                            try:
                                post_type = f"media group ({len(post_messages)} items)" if is_media_group else "single message"
                                logger.info(
                                    f"[POST_FORWARDER] Task {task_id}: Processing post "
                                    f"({post_type}, ID: {first_msg.id}) session={attempt_session}"
                                )
                                self._log_post_content_preview(task_id, post_messages)
                                
                                if use_native_forward:
                                    skip_on_contacts = getattr(task, 'skip_on_contacts', False)
                                    if skip_on_contacts and self._post_has_contacts(post_messages):
                                        logger.info(
                                            f"[POST_FORWARDER] Task {task_id}: Skipped post {first_msg.id} "
                                            f"(reason: native_forward + skip_on_contacts=True)"
                                        )
                                        break
                                    message_ids = [msg.id for msg in post_messages]
                                    await attempt_client.forward_messages(
                                        chat_id=task.target_id,
                                        from_chat_id=task.source_id,
                                        message_ids=message_ids
                                    )
                                    post_forwarded = True
                                    logger.info(
                                        f"[POST_FORWARDER] Task {task_id}: Native forwarded post "
                                        f"(msg {first_msg.id})"
                                    )
                                    if attempt_session != session_alias:
                                        await self._release_client(session_alias)
                                        session_alias = attempt_session
                                        client = attempt_client
                                        current_session_idx = session_idx
                                        await self.db.update_post_parse_task(
                                            task_id, current_session=session_alias, failed_sessions=failed_sessions
                                        )
                                    break
                                else:
                                    if is_media_group:
                                        await self._forward_media_group(
                                            attempt_client, post_messages, task.target_id,
                                            task.filter_contacts, task.remove_contacts,
                                            getattr(task, 'add_signature', False),
                                            task.source_title,
                                            getattr(task, 'source_username', None),
                                            signature_options=getattr(task, 'signature_options', None)
                                        )
                                    else:
                                        await self._forward_message(
                                            attempt_client, post_messages[0], task.target_id,
                                            task.filter_contacts, task.remove_contacts,
                                            getattr(task, 'add_signature', False),
                                            task.source_title,
                                            getattr(task, 'source_username', None),
                                            signature_options=getattr(task, 'signature_options', None)
                                        )
                                    post_forwarded = True
                                    if attempt_session != session_alias:
                                        await self._release_client(session_alias)
                                        session_alias = attempt_session
                                        client = attempt_client
                                        current_session_idx = session_idx
                                        await self.db.update_post_parse_task(
                                            task_id, current_session=session_alias, failed_sessions=failed_sessions
                                        )
                                    break
                            except FloodWait as e:
                                tried_sessions_this_post.append(attempt_session)
                                last_session_error = f"FloodWait: {e.value}s"
                                logger.warning(
                                    f"[POST_FORWARDER] Task {task_id}: FloodWait на сессии {attempt_session} для поста {first_msg.id}, пробуем другую сессию"
                                )
                                if attempt_session == session_alias:
                                    await self._release_client(attempt_session)
                                if len(tried_sessions_this_post) >= len(available_sessions):
                                    error_msg = f"Не удалось переслать пост (msg {first_msg.id}). Все сессии: {last_session_error}"
                                    logger.error(f"[POST_FORWARDER] Task {task_id}: {error_msg}")
                                    await self.db.update_post_parse_task(
                                        task_id, status='failed', error_message=error_msg, failed_sessions=failed_sessions
                                    )
                                    await self._notify_user(
                                        task.user_id,
                                        f"❌ **Задача пересылки постов остановлена**\n\n"
                                        f"Источник: {getattr(task, 'source_title', '')}\n"
                                        f"Причина: {error_msg}"
                                    )
                                    return
                                session_idx = (session_idx + 1) % len(available_sessions)
                                attempt_session = available_sessions[session_idx]
                                attempt_client = await self._get_client(attempt_session, task.use_proxy)
                                if not attempt_client:
                                    tried_sessions_this_post.append(attempt_session)
                                    last_session_error = f"Сессия {attempt_session} недоступна"
                                    if len(tried_sessions_this_post) >= len(available_sessions):
                                        error_msg = f"Не удалось переслать пост (msg {first_msg.id}). {last_session_error}"
                                        await self.db.update_post_parse_task(
                                            task_id, status='failed', error_message=error_msg, failed_sessions=failed_sessions
                                        )
                                        await self._notify_user(task.user_id, f"❌ **Задача пересылки постов остановлена**\n\n{error_msg}")
                                        return
                                continue
                            except Exception as e:
                                err_str = str(e)
                                if not use_native_forward and "MEDIA_EMPTY" in err_str:
                                    try:
                                        skip_on_contacts = getattr(task, 'skip_on_contacts', False)
                                        if skip_on_contacts and self._post_has_contacts(post_messages):
                                            logger.info(
                                                f"[POST_FORWARDER] Task {task_id}: Skipped post (msg {first_msg.id}, MEDIA_EMPTY + skip_on_contacts)"
                                            )
                                            break
                                        forwarded_msgs = await self._forward_native(
                                            attempt_client, post_messages, task.target_id, task.remove_contacts
                                        )
                                        if forwarded_msgs:
                                            post_forwarded = True
                                            if attempt_session != session_alias:
                                                await self._release_client(session_alias)
                                                session_alias = attempt_session
                                                client = attempt_client
                                                current_session_idx = session_idx
                                                await self.db.update_post_parse_task(
                                                    task_id, current_session=session_alias, failed_sessions=failed_sessions
                                                )
                                            logger.info(
                                                f"[POST_FORWARDER] Task {task_id}: Used native forward for post (msg {first_msg.id}, MEDIA_EMPTY fallback)"
                                            )
                                            break
                                    except Exception as fallback_err:
                                        if self._is_session_error(fallback_err):
                                            tried_sessions_this_post.append(attempt_session)
                                            last_session_error = str(fallback_err)
                                            logger.error(
                                                f"[POST_FORWARDER] Task {task_id}: Fallback native forward failed (session error) для поста {first_msg.id}: {fallback_err}"
                                            )
                                            if attempt_session == session_alias:
                                                await self._release_client(attempt_session)
                                            if len(tried_sessions_this_post) >= len(available_sessions):
                                                error_msg = f"Не удалось переслать пост (msg {first_msg.id}). Все сессии: {last_session_error}"
                                                await self.db.update_post_parse_task(
                                                    task_id, status='failed', error_message=error_msg, failed_sessions=failed_sessions
                                                )
                                                await self._notify_user(
                                                    task.user_id,
                                                    f"❌ **Задача пересылки постов остановлена**\n\n{error_msg}"
                                                )
                                                return
                                            session_idx = (session_idx + 1) % len(available_sessions)
                                            attempt_session = available_sessions[session_idx]
                                            attempt_client = await self._get_client(attempt_session, task.use_proxy)
                                            if not attempt_client:
                                                tried_sessions_this_post.append(attempt_session)
                                                if len(tried_sessions_this_post) >= len(available_sessions):
                                                    await self.db.update_post_parse_task(
                                                        task_id, status='failed', error_message=last_session_error, failed_sessions=failed_sessions
                                                    )
                                                    await self._notify_user(task.user_id, f"❌ **Задача пересылки постов остановлена**\n\n{last_session_error}")
                                                    return
                                            continue
                                        else:
                                            logger.info(
                                                f"[POST_FORWARDER] Task {task_id}: Post (msg {first_msg.id}) skipped — MEDIA_EMPTY fallback failed (содержимое поста)"
                                            )
                                            break
                                elif self._is_session_error(e):
                                    tried_sessions_this_post.append(attempt_session)
                                    last_session_error = err_str
                                    logger.error(
                                        f"[POST_FORWARDER] Task {task_id}: Ошибка сессии при пересылке поста (msg {first_msg.id}): {e}"
                                    )
                                    if attempt_session == session_alias:
                                        await self._release_client(attempt_session)
                                    if len(tried_sessions_this_post) >= len(available_sessions):
                                        error_msg = f"Не удалось переслать пост (msg {first_msg.id}). Все сессии дали ошибку: {last_session_error}"
                                        await self.db.update_post_parse_task(
                                            task_id, status='failed', error_message=error_msg, failed_sessions=failed_sessions
                                        )
                                        await self._notify_user(
                                            task.user_id,
                                            f"❌ **Задача пересылки постов остановлена**\n\n"
                                            f"Источник: {getattr(task, 'source_title', '')}\n"
                                            f"Причина: {error_msg}"
                                        )
                                        return
                                    session_idx = (session_idx + 1) % len(available_sessions)
                                    attempt_session = available_sessions[session_idx]
                                    attempt_client = await self._get_client(attempt_session, task.use_proxy)
                                    if not attempt_client:
                                        tried_sessions_this_post.append(attempt_session)
                                        if len(tried_sessions_this_post) >= len(available_sessions):
                                            await self.db.update_post_parse_task(
                                                task_id, status='failed', error_message=last_session_error or f"Сессия {attempt_session} недоступна", failed_sessions=failed_sessions
                                            )
                                            await self._notify_user(task.user_id, f"❌ **Задача пересылки постов остановлена**\n\n{last_session_error or 'Все сессии недоступны'}")
                                            return
                                    continue
                                else:
                                    logger.info(
                                        f"[POST_FORWARDER] Task {task_id}: Post (msg {first_msg.id}) пропущен (ошибка не сессии): {e}"
                                    )
                                    break
                        
                        # Only count if post was successfully forwarded
                        if post_forwarded:
                            forwarded_count += 1
                            posts_since_delay += 1
                            posts_since_rotate += 1
                            
                            # Update task progress (use last message ID from post)
                            last_msg = post_messages[-1]
                            await self.db.update_post_parse_task(
                                task_id,
                                forwarded_count=forwarded_count,
                                last_message_id=last_msg.id,
                                last_action_time=datetime.now().isoformat()
                            )
                            
                            # Check limit AFTER successful forward
                            if task.limit and forwarded_count >= task.limit:
                                logger.info(
                                    f"[POST_FORWARDER] Limit reached for task {task_id} "
                                    f"({forwarded_count} posts forwarded)"
                                )
                                break
                            
                            # Apply delay (by posts, not messages)
                            if task.delay_every > 0 and posts_since_delay >= task.delay_every:
                                if task.delay_seconds > 0:
                                    await self.db.update_post_parse_task(task_id, worker_phase='sleeping')
                                    await asyncio.sleep(task.delay_seconds)
                                    await self.db.update_post_parse_task(task_id, worker_phase='forwarding')
                                posts_since_delay = 0
                            elif task.delay_every == 0 and task.delay_seconds > 0:
                                # Fallback if delay_every is 0 but delay_seconds > 0 (delay every post?)
                                # Assuming delay_every=0 means no delay in logic usually, but let's be safe
                                await self.db.update_post_parse_task(task_id, worker_phase='sleeping')
                                await asyncio.sleep(task.delay_seconds)
                                await self.db.update_post_parse_task(task_id, worker_phase='forwarding')
                                posts_since_delay = 0
                            
                            # Rotate session (by posts, not messages)
                            if task.rotate_sessions and task.rotate_every > 0:
                                if posts_since_rotate >= task.rotate_every:
                                    logger.info(f"[POST_FORWARDER] Task {task_id}: Rotation triggered (limit {task.rotate_every} posts)")
                                    await self._release_client(session_alias)
                                    
                                    found_new_session = False
                                    start_idx = (current_session_idx + 1) % len(available_sessions)
                                    
                                    # Try all sessions starting from next
                                    for i in range(len(available_sessions)):
                                        idx = (start_idx + i) % len(available_sessions)
                                        candidate_alias = available_sessions[idx]
                                        
                                        # Skip known bad sessions if needed (optional)
                                        if task.failed_sessions and candidate_alias in task.failed_sessions:
                                            continue

                                        # Try to connect
                                        logger.info(f"[POST_FORWARDER] Task {task_id}: Trying session {candidate_alias} for rotation...")
                                        new_client = await self._get_client(candidate_alias, task.use_proxy)
                                        if new_client:
                                            # Success
                                            current_session_idx = idx
                                            session_alias = candidate_alias
                                            client = new_client
                                            posts_since_rotate = 0
                                            found_new_session = True
                                            
                                            # Update DB
                                            await self.db.update_post_parse_task(
                                                task_id,
                                                current_session=session_alias
                                            )
                                            logger.info(f"[POST_FORWARDER] Rotated to session {session_alias}")
                                            break
                                        else:
                                            logger.warning(f"[POST_FORWARDER] Task {task_id}: Session {candidate_alias} failed to connect during rotation")
                                    
                                    if not found_new_session:
                                        error_msg = "Не удалось подключить ни одну сессию при ротации"
                                        await self.db.update_post_parse_task(
                                            task_id, status='failed', error_message=error_msg
                                        )
                                        await self._notify_user(task.user_id, f"❌ **Задача остановлена**\n\n{error_msg}")
                                        return
                
                # Task completed — обновляем статус и уведомляем пользователя (как в инвайтинге)
                completion_reason = (
                    "limit reached" if task.limit and forwarded_count >= task.limit 
                    else "history exhausted"
                )
                await self.db.update_post_parse_task(task_id, status='completed')
                logger.info(
                    f"[POST_FORWARDER] Task {task_id} completed ({completion_reason}). "
                    f"Forwarded {forwarded_count} posts."
                )
                task = await self.db.get_post_parse_task(task_id)
                if task:
                    await self._notify_user(
                        task.user_id,
                        f"✅ **Задача пересылки постов завершена**\n\n"
                        f"📊 **Итог:** переслано постов: {forwarded_count}\n"
                        f"📂 Источник: {getattr(task, 'source_title', '')}\n"
                        f"📂 Причина завершения: {'достигнут лимит' if (task.limit and forwarded_count >= task.limit) else 'история обработана'}\n\n"
                        f"🎯 Задача выполнена успешно."
                    )
            
            finally:
                await self._release_client(session_alias)
        
        except asyncio.CancelledError:
            logger.info(f"[POST_FORWARDER] Task {task_id} cancelled")
        except Exception as e:
            logger.error(f"[POST_FORWARDER] Task {task_id} failed: {e}")
            await self.db.update_post_parse_task(
                task_id, status='failed', error_message=str(e)
            )
            try:
                task = await self.db.get_post_parse_task(task_id)
                if task:
                    await self._notify_user(
                        task.user_id,
                        f"❌ **Задача пересылки постов остановлена**\n\n"
                        f"Причина: {str(e)}"
                    )
            except Exception as notify_err:
                logger.error(f"[POST_FORWARDER] Failed to send failure notification: {notify_err}")
        finally:
            if task_id in self._parse_tasks:
                del self._parse_tasks[task_id]
            if task_id in self._stop_flags:
                del self._stop_flags[task_id]
    
    async def _forward_message(
        self, 
        client: Client, 
        message: PyrogramMessage, 
        target_id: int,
        filter_contacts: bool = False,
        remove_contacts: bool = False,
        add_signature: bool = False,
        source_title: str = "",
        source_username: str = None,
        **kwargs
    ):
        """Forward a single message to target channel/group.
        
        Note: Media groups are handled at the post level, so this function
        just sends individual messages without buffering.
        """
        task_id = kwargs.get("task_id")

        # Debug: логируем, как именно будет обрабатываться сообщение перед пересылкой
        try:
            raw_text = message.text or message.caption or ""
            preview = raw_text if len(raw_text) <= 300 else raw_text[:300] + "…"
            entities = self._get_message_entities(message)
            entities_summary = self._entities_summary(entities)
            logger.info(
                f"[POST_FORWARDER] _forward_message: task={task_id}, msg_id={getattr(message, 'id', None)}, "
                f"filter_contacts={filter_contacts}, remove_contacts={remove_contacts}, "
                f"add_signature={add_signature}, has_photo={bool(message.photo)}, "
                f"has_video={bool(message.video)}, has_document={bool(message.document)}, "
                f"has_audio={bool(message.audio)}, has_voice={bool(message.voice)}, "
                f"has_animation={bool(message.animation)}, "
                f"text_len={len(raw_text)}, entities=[{entities_summary}], "
                f"text_preview={repr(preview)}"
            )
        except Exception:
            # Не прерываем пересылку из‑за проблем с логированием
            pass
        # Process text
        text = message.text or message.caption or ""
        if filter_contacts or remove_contacts:
            text = self._filter_contacts(text, filter_contacts, remove_contacts)
        
        # Add signature if enabled
        if add_signature:
            signature = self._generate_signature(
                message, source_title, source_username,
                signature_options=kwargs.get('signature_options')
            )
            if signature:
                text += signature
        
        # Send message based on type
        if message.photo:
            await client.send_photo(target_id, message.photo.file_id, caption=text)
        elif message.video:
            await client.send_video(target_id, message.video.file_id, caption=text)
        elif message.document:
            await client.send_document(target_id, message.document.file_id, caption=text)
        elif message.audio:
            await client.send_audio(target_id, message.audio.file_id, caption=text)
        elif message.voice:
            await client.send_voice(target_id, message.voice.file_id, caption=text)
        elif message.animation:
            await client.send_animation(target_id, message.animation.file_id, caption=text)
        elif text:
            await client.send_message(target_id, text)
        else:
            # No text/media in our view (e.g. premium/custom-emoji only) — use native forward
            await self._forward_native(client, [message], target_id, remove_contacts)
    
    async def _forward_media_group(
        self,
        client: Client,
        messages: List[PyrogramMessage],
        target_id: int,
        filter_contacts: bool = False,
        remove_contacts: bool = False,
        add_signature: bool = False,
        source_title: str = "",
        source_username: str = None,
        **kwargs
    ):
        """Forward a media group as a single album.
        
        Args:
            client: Pyrogram client
            messages: List of messages from the same media group
            target_id: Target chat ID
            filter_contacts: Whether to filter contacts from caption
            remove_contacts: Whether to remove contacts from caption
        """
        if not messages:
            return
        
        # Caption in Telegram media groups can be on any message; take from first that has text/caption
        caption = ""
        caption_msg = None
        for msg in messages:
            raw = msg.text or msg.caption or ""
            if raw:
                caption = raw
                caption_msg = msg
                break
        if filter_contacts or remove_contacts:
            caption = self._filter_contacts(caption, filter_contacts, remove_contacts)
        if add_signature and caption_msg is not None:
            signature = self._generate_signature(
                caption_msg, source_title, source_username,
                signature_options=kwargs.get('signature_options')
            )
            if signature:
                caption += signature
        
        media_list = []
        for idx, msg in enumerate(messages):
            # Attach caption only to the first media item (Telegram allows one caption per album)
            item_caption = caption if idx == 0 else None
            if msg.photo:
                media_list.append(
                    InputMediaPhoto(msg.photo.file_id, caption=item_caption)
                )
            elif msg.video:
                media_list.append(
                    InputMediaVideo(msg.video.file_id, caption=item_caption)
                )
            elif msg.document:
                media_list.append(
                    InputMediaDocument(msg.document.file_id, caption=item_caption)
                )
            elif msg.audio:
                media_list.append(
                    InputMediaAudio(msg.audio.file_id, caption=item_caption)
                )
        
        if media_list:
            await client.send_media_group(target_id, media_list)
    
    async def _rotate_monitoring_session(
        self,
        task_id: int,
        task: Any,
        old_client: Client,
        old_session: str,
        available_sessions: List[str],
        failed_sessions: List[str],
        current_idx: int
    ):
        """Rotate to next session during monitoring.
        
        Note: This releases the old client. The caller must get a new client
        and re-register the handler.
        """
        # Release old client
        await self._release_client(old_session)
        
        # Calculate next session
        next_idx = (current_idx + 1) % len(available_sessions)
        next_session = available_sessions[next_idx]
        
        # Update DB
        await self.db.update_post_monitoring_task(
            task_id,
            current_session=next_session
        )
        
        logger.info(
            f"[POST_FORWARDER] Monitoring task {task_id}: Rotated from {old_session} to {next_session}"
        )
    
    async def _forward_native(
        self,
        client: Client,
        messages: List[PyrogramMessage],
        target_id: int,
        remove_contacts: bool = False
    ) -> List[PyrogramMessage]:
        """Forward messages using native Telegram forwarding (fallback for MEDIA_EMPTY).
        
        This method:
        1. Forwards messages natively using forward_messages (preserves media)
        2. If remove_contacts=True, edits the forwarded message caption to remove contacts
        
        Args:
            client: Pyrogram client
            messages: List of messages to forward
            target_id: Target chat ID
            remove_contacts: Whether to edit caption after forward to remove contacts
            
        Returns:
            List of forwarded messages in target chat
        """
        if not messages:
            return []
        
        # Extract message IDs
        message_ids = [msg.id for msg in messages]
        source_chat_id = messages[0].chat.id
        
        # Forward messages natively
        forwarded_msgs = await client.forward_messages(
            chat_id=target_id,
            from_chat_id=source_chat_id,
            message_ids=message_ids
        )
        
        # Ensure forwarded_msgs is a list
        if not isinstance(forwarded_msgs, list):
            forwarded_msgs = [forwarded_msgs]
        
        # If remove_contacts is enabled, edit the caption/text of forwarded messages
        if remove_contacts and forwarded_msgs:
            # For media groups, only first message usually has caption
            # For single messages, edit the one message
            for fwd_msg in forwarded_msgs:
                try:
                    # Get current caption or text
                    current_text = fwd_msg.caption or fwd_msg.text or ""
                    
                    if not current_text:
                        continue
                    
                    # Check if has contacts (caption_entities for media with caption)
                    if not self._has_contacts(current_text, self._get_message_entities(fwd_msg)):
                        continue
                    
                    # Apply contact filtering
                    filtered_text = self._filter_contacts(current_text, False, True)
                    
                    # Edit message
                    if fwd_msg.caption:
                        # Has caption (photo, video, document, etc.)
                        await client.edit_message_caption(
                            chat_id=target_id,
                            message_id=fwd_msg.id,
                            caption=filtered_text
                        )
                    elif fwd_msg.text:
                        # Text message
                        await client.edit_message_text(
                            chat_id=target_id,
                            message_id=fwd_msg.id,
                            text=filtered_text
                        )
                    
                    logger.debug(
                        f"[POST_FORWARDER] Edited forwarded message {fwd_msg.id} "
                        f"to remove contacts"
                    )
                    
                    # For media groups, typically only first message has caption
                    # so we can break after editing the first one with text
                    if fwd_msg.caption or fwd_msg.text:
                        break
                        
                except Exception as e:
                    logger.warning(
                        f"[POST_FORWARDER] Failed to edit forwarded message caption: {e}"
                    )
        
        return forwarded_msgs
    
    
    # ============== Post Monitoring ==============
    
    async def start_post_monitoring_task(self, task_id: int) -> bool:
        """Start a post monitoring task."""
        import json
        logger.info(f"[POST_FORWARDER] Starting post monitoring task {task_id}")
        
        if task_id in self._monitoring_tasks:
            logger.warning(f"[POST_FORWARDER] Monitoring task {task_id} already running")
            return False
        
        task = await self.db.get_post_monitoring_task(task_id)
        if not task:
            logger.error(f"[POST_FORWARDER] Monitoring task {task_id} not found")
            return False
            
        # Validate sessions
        logger.info(f"[POST_FORWARDER] Validating sessions for post monitoring task {task_id}...")
        validation_result = await self.session_manager.validate_sessions_for_task('post_monitoring', task)
        valid_sessions = validation_result['valid']
        validation_errors = validation_result['invalid']
        
        await self.db.update_post_monitoring_task(
            task_id, 
            validated_sessions=valid_sessions,
            validation_errors=json.dumps(validation_errors) if validation_errors else None
        )
        
        if not valid_sessions:
            logger.error(f"[POST_FORWARDER] Task {task_id} failed validation: No valid sessions. Errors: {validation_errors}")
            await self.db.update_post_monitoring_task(task_id, status='failed', error_message="No valid sessions found.")
            return False

        # При успешной валидации (есть валидные сессии) очищаем старое сообщение об ошибке,
        # чтобы в статусе не висела устаревшая "No valid sessions found."
        await self.db.update_post_monitoring_task(task_id, error_message=None)

        # Switch session if needed
        if task.available_sessions and task.session_alias not in valid_sessions:
            if valid_sessions:
                new_session = valid_sessions[0]
                logger.info(f"[POST_FORWARDER] Switching task {task_id} to valid session: {new_session}")
                await self.db.update_post_monitoring_task(task_id, session_alias=new_session, current_session=new_session)
            else:
                return False
        
        self._stop_flags[task_id] = False
        self._monitoring_tasks[task_id] = asyncio.create_task(
            self._run_post_monitoring_task(task_id)
        )
        return True
    
    async def stop_post_monitoring_task(self, task_id: int) -> bool:
        """Stop a post monitoring task."""
        logger.info(f"[POST_FORWARDER] Stopping post monitoring task {task_id}")
        
        self._stop_flags[task_id] = True

        # Try to detach Pyrogram on_message handler for this task (if any)
        handler_info = self._monitoring_handlers.pop(task_id, None)
        if handler_info:
            client = handler_info.get("client")
            handler = handler_info.get("handler")
            try:
                if client and handler:
                    client.remove_handler(handler)
                    logger.info(f"[POST_FORWARDER] Removed monitoring handler for task {task_id}")
            except Exception as e:
                logger.warning(
                    f"[POST_FORWARDER] Failed to remove monitoring handler for task {task_id}: {e}"
                )
        
        if task_id in self._monitoring_tasks:
            task = self._monitoring_tasks[task_id]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            # Task may have already removed itself in its finally block
            self._monitoring_tasks.pop(task_id, None)
        
        await self.db.update_post_monitoring_task(task_id, status='paused')
        return True
    
    async def _run_post_monitoring_task(self, task_id: int):
        """Run the post monitoring loop."""
        client = None
        session_alias = None
        
        try:
            task = await self.db.get_post_monitoring_task(task_id)
            if not task:
                logger.error(f"[POST_FORWARDER] Monitoring task {task_id} not found")
                return
            # Если текущая сессия не в списке выбранных (настройки сменили) — переключаем на первую из списка
            if task.available_sessions and task.session_alias not in task.available_sessions:
                await self.db.update_post_monitoring_task(
                    task_id,
                    session_alias=task.available_sessions[0],
                    current_session=task.available_sessions[0]
                )
                task = await self.db.get_post_monitoring_task(task_id)
            
            # Логируем актуальные настройки задачи мониторинга, чтобы видеть,
            # с какими параметрами (включая add_signature) она действительно запущена.
            try:
                logger.info(
                    f"[POST_FORWARDER] Monitoring task {task_id} settings: "
                    f"source={task.source_title} ({task.source_id}), "
                    f"target={task.target_title} ({task.target_id}), "
                    f"limit={task.limit}, delay={task.delay_seconds}s, "
                    f"rotate_sessions={task.rotate_sessions}, rotate_every={task.rotate_every}, "
                    f"use_proxy={task.use_proxy}, "
                    f"filter_contacts={getattr(task, 'filter_contacts', False)}, "
                    f"remove_contacts={getattr(task, 'remove_contacts', False)}, "
                    f"skip_on_contacts={getattr(task, 'skip_on_contacts', False)}, "
                    f"use_native_forward={getattr(task, 'use_native_forward', False)}, "
                    f"check_content_if_native={getattr(task, 'check_content_if_native', True)}, "
                    f"forward_show_source={getattr(task, 'forward_show_source', True)}, "
                    f"media_filter={getattr(task, 'media_filter', 'all')}, "
                    f"add_signature={getattr(task, 'add_signature', False)}, "
                    f"signature_options={getattr(task, 'signature_options', None)}"
                )
            except Exception as log_err:
                logger.debug(f"[POST_FORWARDER] Failed to log monitoring settings for task {task_id}: {log_err}")

            await self.db.update_post_monitoring_task(task_id, status='running')
            
            # Session rotation setup
            session_alias = task.session_alias
            candidates = task.validated_sessions if task.validated_sessions else task.available_sessions
            available_sessions = candidates or [session_alias]
            current_session_idx = available_sessions.index(session_alias) if session_alias in available_sessions else 0
            failed_sessions = list(task.failed_sessions) if task.failed_sessions else []
            
            # Try to connect (with rotation if first fails)
            client = None
            found_initial_session = False
            
            # Ensure index is correct
            if session_alias in available_sessions:
                current_session_idx = available_sessions.index(session_alias)
            else:
                current_session_idx = 0
                
            start_idx = current_session_idx
            
            for i in range(len(available_sessions)):
                idx = (start_idx + i) % len(available_sessions)
                candidate_alias = available_sessions[idx]
                
                if failed_sessions and candidate_alias in failed_sessions:
                    logger.info(f"[POST_FORWARDER] Skipping failed session {candidate_alias} during init (monitoring)")
                    continue
                
                logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Trying session {candidate_alias}...")
                client = await self._get_client(candidate_alias, task.use_proxy)
                
                if client:
                    session_alias = candidate_alias
                    current_session_idx = idx
                    found_initial_session = True
                    logger.info(f"[POST_FORWARDER] Monitoring connected to session {session_alias}")
                    break
                else:
                    logger.warning(f"[POST_FORWARDER] Failed to connect to session {candidate_alias}")
                    if candidate_alias not in failed_sessions:
                        failed_sessions.append(candidate_alias)
                        
            if not found_initial_session:
                error_msg = f"Не удалось подключить ни одну из {len(available_sessions)} сессий для мониторинга"
                await self.db.update_post_monitoring_task(
                    task_id, status='failed', error_message=error_msg, failed_sessions=failed_sessions
                )
                await self._notify_user(
                    task.user_id,
                    f"❌ **Задача мониторинга постов остановлена**\n\n{error_msg}"
                )
                return
            
            # Update current session in DB
            await self.db.update_post_monitoring_task(
                task_id,
                current_session=session_alias,
                failed_sessions=failed_sessions
            )
            
            forwarded_count = task.forwarded_count
            posts_since_rotate = 0
            
            # Media group buffering
            media_group_buffer: Dict[str, List[PyrogramMessage]] = {}
            media_group_timers: Dict[str, asyncio.Task] = {}
            processed_message_ids = set()  # Deduplication
            
            async def send_buffered_group(media_group_id: str):
                """Send a buffered media group as one post."""
                nonlocal forwarded_count, posts_since_rotate, client, session_alias, current_session_idx
                
                if media_group_id not in media_group_buffer:
                    return
                
                messages = media_group_buffer[media_group_id]
                if not messages:
                    return
                
                # Check if already processed (deduplication)
                first_msg_id = messages[0].id
                if first_msg_id in processed_message_ids:
                    logger.debug(f"[POST_FORWARDER] Monitoring task {task_id}: Media group {media_group_id} already processed, skipping")
                    del media_group_buffer[media_group_id]
                    if media_group_id in media_group_timers:
                        del media_group_timers[media_group_id]
                    return
                
                # Mark as processed
                for msg in messages:
                    processed_message_ids.add(msg.id)
                
                try:
                    # Get native forwarding settings
                    use_native_forward = getattr(task, 'use_native_forward', False)
                    check_content_if_native = getattr(task, 'check_content_if_native', True)
                    
                    # Content check for media group (native mode): skip empty groups if check_content_if_native
                    if use_native_forward and check_content_if_native:
                        first_msg = messages[0]
                        if not self._message_has_content(first_msg):
                            logger.info(
                                f"[POST_FORWARDER] Monitoring task {task_id}: Skipped media group {media_group_id} "
                                f"(причина: native_forward + check_content_if_native=True, no content)"
                            )
                            # Cleanup
                            if media_group_id in media_group_buffer:
                                del media_group_buffer[media_group_id]
                            if media_group_id in media_group_timers:
                                media_group_timers[media_group_id].cancel()
                                del media_group_timers[media_group_id]
                            return
                    
                    # Apply keyword filtering (both modes)
                    keywords_whitelist = getattr(task, 'keywords_whitelist', [])
                    keywords_blacklist = getattr(task, 'keywords_blacklist', [])
                    
                    should_check_keywords = True
                    if use_native_forward and not check_content_if_native:
                        should_check_keywords = False
                    
                    if should_check_keywords and (keywords_whitelist or keywords_blacklist):
                        if not self._check_keywords(messages, keywords_whitelist, keywords_blacklist):
                            logger.info(
                                f"[POST_FORWARDER] Monitoring task {task_id}: Skipped media group {media_group_id} "
                                f"(причина: keyword_filter)"
                            )
                            # Cleanup
                            if media_group_id in media_group_buffer:
                                del media_group_buffer[media_group_id]
                            if media_group_id in media_group_timers:
                                media_group_timers[media_group_id].cancel()
                                del media_group_timers[media_group_id]
                            return

                    # Apply media filter (only in copy mode)
                    if not use_native_forward:
                         media_filter = getattr(task, 'media_filter', 'all')
                         # Check first message (representative)
                         if self._should_skip_message(messages[0], media_filter):
                             logger.info(
                                 f"[POST_FORWARDER] Monitoring task {task_id}: Skipped media group {media_group_id} "
                                 f"(причина: media_filter={media_filter})"
                             )
                             # Cleanup
                             if media_group_id in media_group_buffer:
                                 del media_group_buffer[media_group_id]
                             if media_group_id in media_group_timers:
                                 media_group_timers[media_group_id].cancel()
                                 del media_group_timers[media_group_id]
                             return

                    # Forward media group
                    if use_native_forward:
                        # Native forwarding: check for skip_on_contacts (all messages in group)
                        skip_on_contacts = getattr(task, 'skip_on_contacts', False)
                        if skip_on_contacts and self._post_has_contacts(messages):
                            self._log_post_content_preview(task_id, messages)
                            logger.info(
                                f"[POST_FORWARDER] Monitoring task {task_id}: Skipped media group {media_group_id} "
                                f"(reason: native_forward + skip_on_contacts=True)"
                            )
                            if media_group_id in media_group_buffer:
                                del media_group_buffer[media_group_id]
                            if media_group_id in media_group_timers:
                                media_group_timers[media_group_id].cancel()
                                del media_group_timers[media_group_id]
                            return

                        # Native forwarding mode: use forward_messages
                        forward_show_source = getattr(task, 'forward_show_source', True)
                        message_ids = [msg.id for msg in messages]
                        
                        # Forward messages natively
                        await client.forward_messages(
                            chat_id=task.target_id,
                            from_chat_id=task.source_id,
                            message_ids=message_ids
                        )
                        
                        logger.debug(
                            f"[POST_FORWARDER] Monitoring task {task_id}: Native forwarded media group {media_group_id} "
                            f"({len(messages)} items, show_source={forward_show_source})"
                        )
                    else:
                        # Copy mode: use existing copy logic
                        # Apply media filter (only in copy mode)
                        media_filter = getattr(task, 'media_filter', 'all')
                        # Check first message (representative)
                        if self._should_skip_message(messages[0], media_filter):
                            logger.info(
                                f"[POST_FORWARDER] Monitoring task {task_id}: Skipped media group {media_group_id} "
                                f"(причина: media_filter={media_filter})"
                            )
                            # Cleanup
                            if media_group_id in media_group_buffer:
                                del media_group_buffer[media_group_id]
                            if media_group_id in media_group_timers:
                                media_group_timers[media_group_id].cancel()
                                del media_group_timers[media_group_id]
                            return
                            
                        # Check for contacts and skip if skip_on_contacts is enabled (only in copy mode)
                        skip_on_contacts = getattr(task, 'skip_on_contacts', False)
                        if skip_on_contacts and self._post_has_contacts(messages):
                            logger.info(
                                f"[POST_FORWARDER] Monitoring task {task_id}: Skipped media group {media_group_id} "
                                f"(причина: skip_on_contacts=True, обнаружены контакты в тексте)"
                            )
                            # Cleanup
                            if media_group_id in media_group_buffer:
                                del media_group_buffer[media_group_id]
                            if media_group_id in media_group_timers:
                                media_group_timers[media_group_id].cancel()
                                del media_group_timers[media_group_id]
                            return

                        await self._forward_media_group(
                            client, messages, task.target_id,
                            task.filter_contacts, task.remove_contacts,
                            getattr(task, 'add_signature', False),
                            task.source_title,
                            getattr(task, 'source_username', None),
                            signature_options=getattr(task, 'signature_options', None)
                        )
                    
                    # Increment counter ONLY after successful forward (one post = one media group)
                    forwarded_count += 1
                    posts_since_rotate += 1
                    
                    await self.db.update_post_monitoring_task(
                        task_id,
                        forwarded_count=forwarded_count,
                        last_action_time=datetime.now().isoformat()
                    )
                    
                    logger.info(
                        f"[POST_FORWARDER] Monitoring task {task_id}: Forwarded media group "
                        f"({len(messages)} items) as post #{forwarded_count}"
                    )
                    
                    # Check limit
                    if task.limit and forwarded_count >= task.limit:
                        logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Limit reached ({forwarded_count} posts)")
                        self._stop_flags[task_id] = True
                        return
                    
                    # Session rotation by posts
                    if task.rotate_sessions and task.rotate_every > 0 and posts_since_rotate >= task.rotate_every:
                        logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Rotation triggered (limit {task.rotate_every} posts)")
                        await self._release_client(session_alias)
                        
                        found_new_session = False
                        start_idx = (current_session_idx + 1) % len(available_sessions)
                        
                        # Try all sessions starting from next
                        for i in range(len(available_sessions)):
                            idx = (start_idx + i) % len(available_sessions)
                            candidate_alias = available_sessions[idx]
                            
                            if candidate_alias in failed_sessions:
                                continue
                                
                            logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Trying session {candidate_alias} for rotation...")
                            new_client = await self._get_client(candidate_alias, task.use_proxy)
                            if new_client:
                                current_session_idx = idx
                                session_alias = candidate_alias
                                client = new_client
                                posts_since_rotate = 0
                                found_new_session = True
                                
                                await self.db.update_post_monitoring_task(
                                    task_id,
                                    current_session=session_alias
                                )
                                logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Rotated to session {session_alias}")
                                break
                            else:
                                 if candidate_alias not in failed_sessions:
                                     failed_sessions.append(candidate_alias)
                        
                        if not found_new_session:
                            error_msg = "Не удалось подключить ни одну сессию при ротации (мониторинг)"
                            await self.db.update_post_monitoring_task(
                                task_id, status='failed', error_message=error_msg, failed_sessions=failed_sessions
                            )
                            await self._notify_user(task.user_id, f"❌ **Задача мониторинга остановлена**\n\n{error_msg}")
                            self._stop_flags[task_id] = True
                            return
                    
                    if task.delay_seconds > 0:
                        await asyncio.sleep(task.delay_seconds)
                
                except Exception as e:
                    err_str = str(e)
                    if "MEDIA_EMPTY" in err_str:
                        logger.info(
                            f"[POST_FORWARDER] Monitoring task {task_id}: Skipped media group {media_group_id} "
                            f"(причина: invalid_or_empty_media)"
                        )
                    else:
                        logger.error(f"[POST_FORWARDER] Error forwarding media group: {e}")
                        
                        # Rotate session on error (Robust iteration)
                        if task.rotate_sessions and len(available_sessions) > 0:
                            logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Error forwarding ({e}), attempting rotation...")
                            
                            # Mark current as failed
                            if session_alias not in failed_sessions:
                                failed_sessions.append(session_alias)
                            
                            await self._release_client(session_alias)
                            
                            found_recovery_session = False
                            start_idx = (current_session_idx + 1) % len(available_sessions)
                            
                            # Iterate all sessions to find a working one
                            for i in range(len(available_sessions)):
                                idx = (start_idx + i) % len(available_sessions)
                                candidate_alias = available_sessions[idx]
                                
                                # Skip already failed unless we want to retry logic (but usually failed means failed)
                                if candidate_alias in failed_sessions:
                                    continue
                                
                                logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Trying recovery session {candidate_alias}...")
                                new_client = await self._get_client(candidate_alias, task.use_proxy)
                                if new_client:
                                    current_session_idx = idx
                                    session_alias = candidate_alias
                                    client = new_client
                                    found_recovery_session = True
                                    
                                    await self.db.update_post_monitoring_task(
                                        task_id,
                                        current_session=session_alias,
                                        failed_sessions=failed_sessions
                                    )
                                    logger.info(f"[POST_FORWARDER] Recovered with session {session_alias}")
                                    break
                                else:
                                    # Failed to connect
                                    if candidate_alias not in failed_sessions:
                                        failed_sessions.append(candidate_alias)
                            
                            if not found_recovery_session:
                                # All failed
                                error_msg = f"Все сессии недоступны или дали ошибку. Посл. ошибка: {e}"
                                await self.db.update_post_monitoring_task(
                                    task_id,
                                    status='failed',
                                    error_message=error_msg,
                                    failed_sessions=failed_sessions
                                )
                                await self._notify_user(
                                    task.user_id,
                                    f"❌ **Задача мониторинга постов остановлена**\n\n{error_msg}"
                                )
                                self._stop_flags[task_id] = True
                                return
                
                finally:
                    # Cleanup buffer
                    if media_group_id in media_group_buffer:
                        del media_group_buffer[media_group_id]
                    if media_group_id in media_group_timers:
                        del media_group_timers[media_group_id]
            
            # Create message handler
            async def new_message_handler(_, message: PyrogramMessage):
                nonlocal forwarded_count, posts_since_rotate, client, session_alias, current_session_idx
                
                # Если задача уже остановлена/отменена, игнорируем любые новые сообщения
                if self._stop_flags.get(task_id):
                    logger.info(
                        f"[POST_FORWARDER] Monitoring task {task_id}: "
                        f"Received message {getattr(message, 'id', 'unknown')} after stop flag set, skipping"
                    )
                    return
                
                # Deduplication check
                if message.id in processed_message_ids:
                    logger.debug(f"[POST_FORWARDER] Monitoring task {task_id}: Message {message.id} already processed")
                    return
                
                # Skip service messages
                if getattr(message, 'service', False):
                    logger.info(
                        f"[POST_FORWARDER] Monitoring task {task_id}: Skipped msg {message.id} "
                        f"(причина: service_message)"
                    )
                    return
                
                # Get native forwarding settings
                use_native_forward = getattr(task, 'use_native_forward', False)
                check_content_if_native = getattr(task, 'check_content_if_native', True)
                
                # Content checking logic
                if use_native_forward:
                    # Native forwarding mode: check content only if enabled
                    if check_content_if_native and not self._message_has_content(message):
                        logger.info(
                            f"[POST_FORWARDER] Monitoring task {task_id}: Skipped msg {message.id} "
                            f"(причина: native_forward + check_content_if_native=True, no content)"
                        )
                        self._log_message_fields(message, task_id, context="monitoring")
                        return
                else:
                    # Copy mode: always check content
                    if not self._message_has_content(message):
                        logger.info(
                            f"[POST_FORWARDER] Monitoring task {task_id}: Skipped msg {message.id} "
                            f"(причина: copy_mode, service_message_or_empty)"
                        )
                        self._log_message_fields(message, task_id, context="monitoring")
                        return
                
                # Apply keyword filtering (both modes)
                keywords_whitelist = getattr(task, 'keywords_whitelist', [])
                keywords_blacklist = getattr(task, 'keywords_blacklist', [])
                
                should_check_keywords = True
                if use_native_forward and not check_content_if_native:
                    should_check_keywords = False
                
                if should_check_keywords and (keywords_whitelist or keywords_blacklist):
                    if not self._check_keywords([message], keywords_whitelist, keywords_blacklist):
                        logger.info(
                            f"[POST_FORWARDER] Monitoring task {task_id}: Skipped msg {message.id} "
                            f"(причина: keyword_filter)"
                        )
                        return

                # Check media filter (only in copy mode)
                if not use_native_forward:
                    media_filter = getattr(task, 'media_filter', 'all')
                    if self._should_skip_message(message, media_filter):
                        logger.info(
                            f"[POST_FORWARDER] Monitoring task {task_id}: Skipped msg {message.id} "
                            f"(причина: media_filter={media_filter})"
                        )
                        return

                    # Apply contact filter only in copy mode
                    # Check for contacts and skip if skip_on_contacts is enabled (only in copy mode)
                    text = message.text or message.caption or ""
                    skip_on_contacts = getattr(task, 'skip_on_contacts', False)
                    
                    if skip_on_contacts and self._has_contacts(text, self._get_message_entities(message)):
                        self._log_post_content_preview(task_id, [message])
                        logger.info(
                            f"[POST_FORWARDER] Monitoring task {task_id}: Skipped msg {message.id} "
                            f"(причина: skip_on_contacts=True, обнаружены контакты в тексте)"
                        )
                        return
                
                # Handle media groups
                if message.media_group_id:
                    media_group_id = message.media_group_id
                    
                    # Add to buffer
                    if media_group_id not in media_group_buffer:
                        media_group_buffer[media_group_id] = []
                    media_group_buffer[media_group_id].append(message)
                    
                    # Cancel existing timer if any
                    if media_group_id in media_group_timers:
                        media_group_timers[media_group_id].cancel()
                    
                    # Set timer to send group after 3 seconds
                    async def delayed_send():
                        await asyncio.sleep(3.0)
                        await send_buffered_group(media_group_id)
                    
                    media_group_timers[media_group_id] = asyncio.create_task(delayed_send())
                    
                    logger.debug(
                        f"[POST_FORWARDER] Monitoring task {task_id}: Buffered message {message.id} "
                        f"for media group {media_group_id} ({len(media_group_buffer[media_group_id])} items)"
                    )
                    return
                
                # Handle single messages (not part of media group)
                # Mark as processed
                processed_message_ids.add(message.id)
                
                try:
                    # Check if native forwarding is enabled
                    if use_native_forward:
                        # Native forwarding: check for skip_on_contacts
                        skip_on_contacts = getattr(task, 'skip_on_contacts', False)
                        if skip_on_contacts and self._has_contacts(message.caption or message.text or "", self._get_message_entities(message)):
                            self._log_post_content_preview(task_id, [message])
                            logger.info(
                                f"[POST_FORWARDER] Monitoring task {task_id}: Skipped msg {message.id} "
                                f"(reason: native_forward + skip_on_contacts=True)"
                            )
                            return

                        # Native forwarding mode: use forward_messages
                        forward_show_source = getattr(task, 'forward_show_source', True)
                        
                        # Forward message natively
                        await client.forward_messages(
                            chat_id=task.target_id,
                            from_chat_id=task.source_id,
                            message_ids=[message.id]
                        )
                        
                        logger.debug(
                            f"[POST_FORWARDER] Monitoring task {task_id}: Native forwarded msg {message.id} "
                            f"(show_source={forward_show_source})"
                        )
                    else:
                        # Copy mode: use existing copy logic
                        await self._forward_message(
                            client, message, task.target_id,
                            task.filter_contacts, task.remove_contacts,
                            getattr(task, 'add_signature', False),
                            task.source_title,
                            getattr(task, 'source_username', None),
                            signature_options=getattr(task, 'signature_options', None)
                        )
                    
                    # Increment counter ONLY after successful forward
                    forwarded_count += 1
                    posts_since_rotate += 1
                    
                    await self.db.update_post_monitoring_task(
                        task_id,
                        forwarded_count=forwarded_count,
                        last_action_time=datetime.now().isoformat()
                    )
                    
                    logger.info(
                        f"[POST_FORWARDER] Monitoring task {task_id}: Forwarded single message "
                        f"as post #{forwarded_count}"
                    )
                    
                    # Check limit AFTER successful forward
                    if task.limit and forwarded_count >= task.limit:
                        logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Limit reached ({forwarded_count} posts)")
                        self._stop_flags[task_id] = True
                        return
                    
                    # Session rotation by posts
                    if task.rotate_sessions and task.rotate_every > 0 and posts_since_rotate >= task.rotate_every:
                        logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Rotation triggered (limit {task.rotate_every} posts)")
                        await self._release_client(session_alias)
                        
                        found_new_session = False
                        start_idx = (current_session_idx + 1) % len(available_sessions)
                        
                        for i in range(len(available_sessions)):
                            idx = (start_idx + i) % len(available_sessions)
                            candidate_alias = available_sessions[idx]
                            
                            if candidate_alias in failed_sessions:
                                continue
                                
                            logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Trying session {candidate_alias} for rotation...")
                            new_client = await self._get_client(candidate_alias, task.use_proxy)
                            if new_client:
                                current_session_idx = idx
                                session_alias = candidate_alias
                                client = new_client
                                posts_since_rotate = 0
                                found_new_session = True
                                
                                await self.db.update_post_monitoring_task(
                                    task_id,
                                    current_session=session_alias
                                    # failed_sessions not updated here typically
                                )
                                logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Rotated to session {session_alias}")
                                break
                            else:
                                if candidate_alias not in failed_sessions:
                                    failed_sessions.append(candidate_alias)
                        
                        if not found_new_session:
                            error_msg = "Не удалось подключить ни одну сессию при ротации (мониторинг)"
                            await self.db.update_post_monitoring_task(
                                task_id, status='failed', error_message=error_msg, failed_sessions=failed_sessions
                            )
                            await self._notify_user(task.user_id, f"❌ **Задача мониторинга остановлена**\n\n{error_msg}")
                            self._stop_flags[task_id] = True
                            return
                    
                    if task.delay_seconds > 0:
                        await self.db.update_post_monitoring_task(task_id, worker_phase='sleeping')
                        await asyncio.sleep(task.delay_seconds)
                        await self.db.update_post_monitoring_task(task_id, worker_phase='monitoring')
                
                except Exception as e:
                    err_str = str(e)
                    if "MEDIA_EMPTY" in err_str:
                        logger.info(
                            f"[POST_FORWARDER] Monitoring task {task_id}: Skipped msg {message.id} "
                            f"(причина: invalid_or_empty_media)"
                        )
                    else:
                        logger.error(f"[POST_FORWARDER] Error in monitoring handler: {e}")
                        
                        # Rotate session on error (Robust iteration)
                        if task.rotate_sessions and len(available_sessions) > 0:
                            logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Error forwarding ({e}), attempting rotation...")
                            
                            if session_alias not in failed_sessions:
                                failed_sessions.append(session_alias)
                            
                            await self._release_client(session_alias)
                            
                            found_recovery_session = False
                            start_idx = (current_session_idx + 1) % len(available_sessions)
                            
                            for i in range(len(available_sessions)):
                                idx = (start_idx + i) % len(available_sessions)
                                candidate_alias = available_sessions[idx]
                                
                                if candidate_alias in failed_sessions:
                                    continue
                                
                                logger.info(f"[POST_FORWARDER] Monitoring task {task_id}: Trying recovery session {candidate_alias}...")
                                new_client = await self._get_client(candidate_alias, task.use_proxy)
                                if new_client:
                                    current_session_idx = idx
                                    session_alias = candidate_alias
                                    client = new_client
                                    found_recovery_session = True
                                    
                                    await self.db.update_post_monitoring_task(
                                        task_id,
                                        current_session=session_alias,
                                        failed_sessions=failed_sessions
                                    )
                                    logger.info(f"[POST_FORWARDER] Recovered with session {session_alias}")
                                    break
                                else:
                                    if candidate_alias not in failed_sessions:
                                        failed_sessions.append(candidate_alias)
                            
                            if not found_recovery_session:
                                error_msg = f"Все сессии недоступны или дали ошибку. Посл. ошибка: {e}"
                                await self.db.update_post_monitoring_task(
                                    task_id,
                                    status='failed',
                                    error_message=error_msg,
                                    failed_sessions=failed_sessions
                                )
                                await self._notify_user(
                                    task.user_id,
                                    f"❌ **Задача мониторинга постов остановлена**\n\n{error_msg}"
                                )
                                self._stop_flags[task_id] = True
                                return
            
            # Register handler
            from pyrogram import filters
            from pyrogram.handlers import MessageHandler

            # Используем низкоуровневый MessageHandler, чтобы иметь точный объект для remove_handler
            message_handler = MessageHandler(
                new_message_handler,
                filters.chat(task.source_id) & ~filters.service
            )
            client.add_handler(message_handler)
            # Сохраняем и клиента, и сам handler-объект
            self._monitoring_handlers[task_id] = {
                "client": client,
                "handler": message_handler,
            }
            
            logger.info(f"[POST_FORWARDER] Monitoring started for source {task.source_id}")
            
            # Keep running until stopped
            await self.db.update_post_monitoring_task(task_id, worker_phase='monitoring')
            while not self._stop_flags.get(task_id):
                # Update heartbeat
                await self._update_heartbeat_monitoring(task_id)
                await asyncio.sleep(1)
                
                # Check if limit reached
                if task.limit and forwarded_count >= task.limit:
                    break
            
            # Cleanup pending media group timers
            for timer in media_group_timers.values():
                timer.cancel()
            
            # Completed (лимит достигнут) — обновляем статус и уведомляем; при остановке пользователем статус уже paused
            if task.limit and forwarded_count >= task.limit:
                await self.db.update_post_monitoring_task(task_id, status='completed')
                logger.info(f"[POST_FORWARDER] Monitoring task {task_id} completed (limit reached)")
                mon_task = await self.db.get_post_monitoring_task(task_id)
                if mon_task:
                    await self._notify_user(
                        mon_task.user_id,
                        f"✅ **Задача мониторинга постов завершена**\n\n"
                        f"📊 Переслано постов: {forwarded_count}\n"
                        f"📂 Источник: {getattr(mon_task, 'source_title', '')}\n\n"
                        f"🎯 Достигнут лимит постов."
                    )
            else:
                logger.info(f"[POST_FORWARDER] Monitoring task {task_id} stopped (paused or cancelled)")
        
        except asyncio.CancelledError:
            logger.info(f"[POST_FORWARDER] Monitoring task {task_id} cancelled")
        except Exception as e:
            logger.error(f"[POST_FORWARDER] Monitoring task {task_id} failed: {e}")
            await self.db.update_post_monitoring_task(
                task_id, status='failed', error_message=str(e)
            )
            try:
                mon_task = await self.db.get_post_monitoring_task(task_id)
                if mon_task:
                    await self._notify_user(
                        mon_task.user_id,
                        f"❌ **Задача мониторинга постов остановлена**\n\nПричина: {str(e)}"
                    )
            except Exception as notify_err:
                logger.error(f"[POST_FORWARDER] Failed to send monitoring failure notification: {notify_err}")
        finally:
            # Cleanup
            # На всякий случай ещё раз пытаемся снять handler, если задача завершилась сама
            handler_info = self._monitoring_handlers.pop(task_id, None)
            if handler_info:
                h_client = handler_info.get("client")
                h_handler = handler_info.get("handler")
                try:
                    if h_client and h_handler:
                        h_client.remove_handler(h_handler)
                        logger.info(f"[POST_FORWARDER] (finally) Removed monitoring handler for task {task_id}")
                except Exception as e:
                    logger.warning(
                        f"[POST_FORWARDER] (finally) Failed to remove monitoring handler for task {task_id}: {e}"
                    )
            if task_id in self._monitoring_tasks:
                del self._monitoring_tasks[task_id]
            # Явно помечаем задачу как остановленную, чтобы обработчик сообщений
            # больше никогда не пересылал новые посты, даже если хэндлер ещё жив
            self._stop_flags[task_id] = True
            if session_alias:
                await self._release_client(session_alias)



# Global instance
post_forwarder: Optional[PostForwarder] = None


async def init_post_forwarder(db_instance, session_manager: SessionManager = None):
    """Initialize global post forwarder instance."""
    global post_forwarder
    post_forwarder = PostForwarder(db_instance, session_manager)
    return post_forwarder


def get_post_forwarder() -> Optional[PostForwarder]:
    """Get global post forwarder instance."""
    return post_forwarder
