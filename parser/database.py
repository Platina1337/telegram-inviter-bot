# -*- coding: utf-8 -*-
"""
Database module for inviter service.
Handles sessions, tasks, and user data storage.
"""
import os
import logging
import aiosqlite
from typing import List, Dict, Optional, Any
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from models import SessionMeta, InviteTask
from .config import config

logger = logging.getLogger(__name__)


class Database:
    """Async SQLite database for inviter service."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
    
    async def connect(self):
        """Initialize database connection and create tables."""
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self._create_tables()
        logger.info(f"База данных подключена: {self.db_path}")
    
    async def close(self):
        """Close database connection."""
        if self.conn:
            await self.conn.close()
            self.conn = None
            logger.info("Database connection closed")
    
    async def _create_tables(self):
        """Create necessary tables if they don't exist."""
        await self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alias TEXT UNIQUE NOT NULL,
                api_id INTEGER NOT NULL,
                api_hash TEXT NOT NULL,
                phone TEXT,
                session_path TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                user_id INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                proxy TEXT
            );
            
            CREATE TABLE IF NOT EXISTS session_assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                task TEXT NOT NULL,
                UNIQUE(session_id, task),
                FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
            );
            
            CREATE TABLE IF NOT EXISTS invite_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                source_group_id INTEGER NOT NULL,
                source_group_title TEXT,
                source_username TEXT,
                target_group_id INTEGER NOT NULL,
                target_group_title TEXT,
                target_username TEXT,
                session_alias TEXT NOT NULL,
                invite_mode TEXT DEFAULT 'member_list',
                status TEXT DEFAULT 'pending',
                invited_count INTEGER DEFAULT 0,
                invite_limit INTEGER,
                delay_seconds INTEGER DEFAULT 30,
                delay_every INTEGER DEFAULT 1,
                rotate_sessions INTEGER DEFAULT 0,
                rotate_every INTEGER DEFAULT 0,
                available_sessions TEXT,
                current_offset INTEGER DEFAULT 0,
                use_proxy INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                error_message TEXT
            );
            
            CREATE TABLE IF NOT EXISTS user_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                group_id TEXT NOT NULL,
                group_title TEXT,
                username TEXT,
                last_used TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, group_id)
            );
            
            CREATE TABLE IF NOT EXISTS user_target_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                group_id TEXT NOT NULL,
                group_title TEXT,
                username TEXT,
                last_used TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, group_id)
            );
            
            CREATE TABLE IF NOT EXISTS invite_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                user_telegram_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                status TEXT DEFAULT 'success',
                error_message TEXT,
                invited_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(task_id) REFERENCES invite_tasks(id) ON DELETE CASCADE
            );
            
            CREATE TABLE IF NOT EXISTS parse_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                source_group_id INTEGER NOT NULL,
                source_group_title TEXT,
                source_username TEXT,
                session_alias TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                parsed_count INTEGER DEFAULT 0,
                parse_limit INTEGER,
                delay_seconds INTEGER DEFAULT 2,
                rotate_sessions INTEGER DEFAULT 0,
                rotate_every INTEGER DEFAULT 0,
                available_sessions TEXT,
                failed_sessions TEXT,
                current_offset INTEGER DEFAULT 0,
                use_proxy INTEGER DEFAULT 1,
                filter_admins INTEGER DEFAULT 0,
                filter_inactive INTEGER DEFAULT 0,
                inactive_threshold_days INTEGER DEFAULT 30,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                error_message TEXT
            );
        """)
        
        # Migration: add delay_every to invite_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE invite_tasks ADD COLUMN delay_every INTEGER DEFAULT 1")
        except:
            pass  # Already exists
        
        # Migration: add rotate_every to invite_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE invite_tasks ADD COLUMN rotate_every INTEGER DEFAULT 0")
        except:
            pass  # Already exists

        # Migration: add source_username to invite_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE invite_tasks ADD COLUMN source_username TEXT")
        except:
            pass

        # Migration: add target_username to invite_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE invite_tasks ADD COLUMN target_username TEXT")
        except:
            pass

        # Migration: add invite_mode to invite_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE invite_tasks ADD COLUMN invite_mode TEXT DEFAULT 'member_list'")
        except:
            pass

        # Migration: add proxy to sessions if missing
        try:
            await self.conn.execute("ALTER TABLE sessions ADD COLUMN proxy TEXT")
        except:
            pass

        # Migration: add use_proxy to invite_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE invite_tasks ADD COLUMN use_proxy INTEGER DEFAULT 0")
        except:
            pass

        # Migration: add failed_sessions to invite_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE invite_tasks ADD COLUMN failed_sessions TEXT")
        except:
            pass

        # Migration: add filter_mode to invite_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE invite_tasks ADD COLUMN filter_mode TEXT DEFAULT 'all'")
        except:
            pass

        # Migration: add inactive_threshold_days to invite_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE invite_tasks ADD COLUMN inactive_threshold_days INTEGER")
        except:
            pass

        # Migration: add file_source to invite_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE invite_tasks ADD COLUMN file_source TEXT")
        except:
            pass

        # Migration: add delay_every to parse_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN delay_every INTEGER DEFAULT 1")
        except:
            pass

        # Migration: add save_every to parse_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN save_every INTEGER DEFAULT 0")
        except:
            pass

        # Migration: add saved_count to parse_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN saved_count INTEGER DEFAULT 0")
        except:
            pass

        # Migration: add parse_mode to parse_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN parse_mode TEXT DEFAULT 'member_list'")
        except:
            pass

        # Migration: add keyword_filter to parse_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN keyword_filter TEXT")
        except:
            pass

        # Migration: add exclude_keywords to parse_tasks if missing
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN exclude_keywords TEXT")
        except:
            pass

        # Migration: add messages_limit to parse_tasks if missing (for message_based mode)
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN messages_limit INTEGER")
        except:
            pass

        # Migration: add delay_every_requests to parse_tasks if missing (for message_based mode)
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN delay_every_requests INTEGER DEFAULT 1")
        except:
            pass

        # Migration: add rotate_every_requests to parse_tasks if missing (for message_based mode)
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN rotate_every_requests INTEGER DEFAULT 0")
        except:
            pass

        # Migration: add save_every_users to parse_tasks if missing (for message_based mode)
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN save_every_users INTEGER DEFAULT 0")
        except:
            pass

        # Migration: add messages_offset to parse_tasks if missing (for message_based mode resume)
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN messages_offset INTEGER DEFAULT 0")
        except:
            pass

        # Migration: add source_type to parse_tasks if missing (channel = комментарии под постами, group = группа)
        try:
            await self.conn.execute("ALTER TABLE parse_tasks ADD COLUMN source_type TEXT DEFAULT 'group'")
        except:
            pass

        await self.conn.commit()

    
    # ============== Sessions ==============
    
    async def create_session(self, session: SessionMeta) -> int:
        """Create a new session record."""
        cursor = await self.conn.execute("""
            INSERT INTO sessions (alias, api_id, api_hash, phone, session_path, is_active, user_id, proxy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (session.alias, session.api_id, session.api_hash, session.phone, 
              session.session_path, 1 if session.is_active else 0, session.user_id, session.proxy))
        await self.conn.commit()
        return cursor.lastrowid
    
    async def get_all_sessions(self) -> List[SessionMeta]:
        """Get all sessions from database."""
        cursor = await self.conn.execute("""
            SELECT id, alias, api_id, api_hash, phone, session_path, is_active, user_id, created_at, proxy
            FROM sessions ORDER BY created_at DESC
        """)
        rows = await cursor.fetchall()
        sessions = []
        for row in rows:
            sessions.append(SessionMeta(
                id=row['id'],
                alias=row['alias'],
                api_id=row['api_id'],
                api_hash=row['api_hash'],
                phone=row['phone'] or '',
                session_path=row['session_path'],
                is_active=bool(row['is_active']),
                user_id=row['user_id'],
                created_at=row['created_at'],
                proxy=row['proxy'] if 'proxy' in row.keys() else None
            ))
        return sessions
    
    async def get_session_by_alias(self, alias: str) -> Optional[SessionMeta]:
        """Get session by alias."""
        cursor = await self.conn.execute("""
            SELECT id, alias, api_id, api_hash, phone, session_path, is_active, user_id, created_at, proxy
            FROM sessions WHERE alias = ?
        """, (alias,))
        row = await cursor.fetchone()
        if row:
            return SessionMeta(
                id=row['id'],
                alias=row['alias'],
                api_id=row['api_id'],
                api_hash=row['api_hash'],
                phone=row['phone'] or '',
                session_path=row['session_path'],
                is_active=bool(row['is_active']),
                user_id=row['user_id'],
                created_at=row['created_at'],
                proxy=row['proxy'] if 'proxy' in row.keys() else None
            )
        return None
    
    async def update_session(self, session_id: int, **kwargs):
        """Update session fields."""
        if not kwargs:
            return
        
        set_parts = []
        values = []
        for key, value in kwargs.items():
            set_parts.append(f"{key} = ?")
            values.append(value)
        
        values.append(session_id)
        await self.conn.execute(
            f"UPDATE sessions SET {', '.join(set_parts)} WHERE id = ?",
            values
        )
        await self.conn.commit()
    
    async def delete_session(self, session_id: int):
        """Delete session from database."""
        await self.conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
        await self.conn.commit()
    
    async def import_existing_sessions(self, session_dir: str):
        """Import existing .session files from directory."""
        if not os.path.exists(session_dir):
            return
        
        for filename in os.listdir(session_dir):
            if filename.endswith('.session') and not filename.endswith('-journal'):
                alias = filename.replace('.session', '')
                existing = await self.get_session_by_alias(alias)
                if not existing:
                    # Create with default API credentials (user must configure)
                    session = SessionMeta(
                        id=0,
                        alias=alias,
                        api_id=config.API_ID,
                        api_hash=config.API_HASH,
                        phone='',
                        session_path=alias,
                        is_active=True
                    )
                    await self.create_session(session)
                    logger.info(f"Импортирована сессия: {alias}")
    
    # ============== Session Assignments ==============
    
    async def add_session_assignment(self, session_id: int, task: str):
        """Add task assignment to session."""
        await self.conn.execute("""
            INSERT OR IGNORE INTO session_assignments (session_id, task) VALUES (?, ?)
        """, (session_id, task))
        await self.conn.commit()
    
    async def remove_session_assignment(self, session_id: int, task: str):
        """Remove task assignment from session."""
        await self.conn.execute("""
            DELETE FROM session_assignments WHERE session_id = ? AND task = ?
        """, (session_id, task))
        await self.conn.commit()
    
    async def get_sessions_for_task(self, task: str) -> List[SessionMeta]:
        """Get all sessions assigned to a task."""
        cursor = await self.conn.execute("""
            SELECT s.id, s.alias, s.api_id, s.api_hash, s.phone, s.session_path, 
                   s.is_active, s.user_id, s.created_at, s.proxy
            FROM sessions s
            JOIN session_assignments sa ON s.id = sa.session_id
            WHERE sa.task = ? AND s.is_active = 1
            ORDER BY s.created_at
        """, (task,))
        rows = await cursor.fetchall()
        return [SessionMeta(
            id=row['id'],
            alias=row['alias'],
            api_id=row['api_id'],
            api_hash=row['api_hash'],
            phone=row['phone'] or '',
            session_path=row['session_path'],
            is_active=bool(row['is_active']),
            user_id=row['user_id'],
            created_at=row['created_at'],
            proxy=row['proxy'] if 'proxy' in row.keys() else None
        ) for row in rows]
    
    async def get_assignments(self) -> Dict[str, List[str]]:
        """Get all assignments as task -> [session_aliases]."""
        cursor = await self.conn.execute("""
            SELECT sa.task, s.alias
            FROM session_assignments sa
            JOIN sessions s ON s.id = sa.session_id
            ORDER BY sa.task, s.alias
        """)
        rows = await cursor.fetchall()
        assignments = {}
        for row in rows:
            task = row['task']
            alias = row['alias']
            if task not in assignments:
                assignments[task] = []
            assignments[task].append(alias)
        return assignments
    
    # ============== Invite Tasks ==============
    
    async def create_invite_task(self, task: InviteTask) -> int:
        """Create a new invite task."""
        available_sessions_str = ','.join(task.available_sessions) if task.available_sessions else ''
        failed_sessions_str = ','.join(task.failed_sessions) if task.failed_sessions else ''
        cursor = await self.conn.execute("""
            INSERT INTO invite_tasks (
                user_id, source_group_id, source_group_title, source_username,
                target_group_id, target_group_title, target_username,
                session_alias, invite_mode, file_source, status, invited_count, invite_limit, delay_seconds, delay_every,
                rotate_sessions, rotate_every, use_proxy, available_sessions, failed_sessions, current_offset, filter_mode, inactive_threshold_days
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            task.user_id, task.source_group_id, task.source_group_title, task.source_username,
            task.target_group_id, task.target_group_title, task.target_username,
            task.session_alias, task.invite_mode, task.file_source, task.status, task.invited_count, task.limit, task.delay_seconds, task.delay_every,
            1 if task.rotate_sessions else 0, task.rotate_every, 1 if task.use_proxy else 0, available_sessions_str, failed_sessions_str, task.current_offset, task.filter_mode, task.inactive_threshold_days
        ))
        await self.conn.commit()
        return cursor.lastrowid

    
    async def get_invite_task(self, task_id: int) -> Optional[InviteTask]:
        """Get invite task by ID."""
        cursor = await self.conn.execute("""
            SELECT * FROM invite_tasks WHERE id = ?
        """, (task_id,))
        row = await cursor.fetchone()
        if row:
            return self._row_to_invite_task(row)
        return None
    
    async def get_user_invite_tasks(self, user_id: int, status: str = None) -> List[InviteTask]:
        """Get invite tasks for a user."""
        if status:
            cursor = await self.conn.execute("""
                SELECT * FROM invite_tasks WHERE user_id = ? AND status = ?
                ORDER BY created_at DESC
            """, (user_id, status))
        else:
            cursor = await self.conn.execute("""
                SELECT * FROM invite_tasks WHERE user_id = ?
                ORDER BY created_at DESC
            """, (user_id,))
        rows = await cursor.fetchall()
        return [self._row_to_invite_task(row) for row in rows]
    
    async def get_running_tasks(self) -> List[InviteTask]:
        """Get all running tasks."""
        cursor = await self.conn.execute("""
            SELECT * FROM invite_tasks WHERE status = 'running'
            ORDER BY created_at ASC
        """)
        rows = await cursor.fetchall()
        return [self._row_to_invite_task(row) for row in rows]
    
    async def update_invite_task(self, task_id: int, **kwargs):
        """Update invite task fields."""
        if self.conn is None:
            logger.warning(f"Cannot update invite task {task_id}: database connection is closed")
            return
            
        if not kwargs:
            return
        
        kwargs['updated_at'] = datetime.now().isoformat()
        
        set_parts = []
        values = []
        for key, value in kwargs.items():
            if key == 'available_sessions' and isinstance(value, list):
                value = ','.join(value)
            elif key == 'failed_sessions' and isinstance(value, list):
                value = ','.join(value)
            elif key == 'filter_mode':
                value = str(value)
            elif key == 'inactive_threshold_days':
                value = int(value) if value is not None else None
            set_parts.append(f"{key} = ?")
            values.append(value)
        
        values.append(task_id)
        await self.conn.execute(
            f"UPDATE invite_tasks SET {', '.join(set_parts)} WHERE id = ?",
            values
        )
        await self.conn.commit()
    
    async def delete_invite_task(self, task_id: int):
        """Delete invite task."""
        await self.conn.execute("DELETE FROM invite_tasks WHERE id = ?", (task_id,))
        await self.conn.commit()
    
    def _row_to_invite_task(self, row) -> InviteTask:
        """Convert database row to InviteTask object."""
        available_sessions = row['available_sessions'].split(',') if row['available_sessions'] else []
        failed_sessions = row['failed_sessions'].split(',') if ('failed_sessions' in row.keys() and row['failed_sessions']) else []
        # Удаляем пустые строки из списков
        available_sessions = [s for s in available_sessions if s]
        failed_sessions = [s for s in failed_sessions if s]
        return InviteTask(
            id=row['id'],
            user_id=row['user_id'],
            source_group_id=row['source_group_id'],
            source_group_title=row['source_group_title'] or '',
            source_username=row['source_username'] if 'source_username' in row.keys() else None,
            target_group_id=row['target_group_id'],
            target_group_title=row['target_group_title'] or '',
            target_username=row['target_username'] if 'target_username' in row.keys() else None,
            session_alias=row['session_alias'],
            invite_mode=row['invite_mode'] if 'invite_mode' in row.keys() else 'member_list',
            file_source=row['file_source'] if 'file_source' in row.keys() else None,
            status=row['status'],
            invited_count=row['invited_count'],
            limit=row['invite_limit'],
            delay_seconds=row['delay_seconds'],
            delay_every=row['delay_every'],
            rotate_sessions=bool(row['rotate_sessions']),
            rotate_every=row['rotate_every'] if 'rotate_every' in row.keys() else 0,
            use_proxy=bool(row['use_proxy']) if 'use_proxy' in row.keys() else False,
            available_sessions=available_sessions,
            failed_sessions=failed_sessions,
            current_offset=row['current_offset'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            error_message=row['error_message'],
            filter_mode=row['filter_mode'] if 'filter_mode' in row.keys() else 'all',
            inactive_threshold_days=row['inactive_threshold_days'] if 'inactive_threshold_days' in row.keys() else None
        )

    
    # ============== User Groups ==============
    
    async def get_user_groups(self, user_id: int) -> List[Dict]:
        """Get user's source group history."""
        cursor = await self.conn.execute("""
            SELECT group_id, group_title, username, last_used
            FROM user_groups WHERE user_id = ?
            ORDER BY last_used DESC LIMIT 10
        """, (user_id,))
        rows = await cursor.fetchall()
        return [{'id': row['group_id'], 'title': row['group_title'], 
                 'username': row['username']} for row in rows]
    
    async def add_user_group(self, user_id: int, group_id: str, title: str, username: str = None):
        """Add or update user's source group."""
        await self.conn.execute("""
            INSERT INTO user_groups (user_id, group_id, group_title, username, last_used)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id, group_id) DO UPDATE SET
                group_title = excluded.group_title,
                username = excluded.username,
                last_used = CURRENT_TIMESTAMP
        """, (user_id, group_id, title, username))
        await self.conn.commit()
    
    async def get_user_target_groups(self, user_id: int) -> List[Dict]:
        """Get user's target group history."""
        cursor = await self.conn.execute("""
            SELECT group_id, group_title, username, last_used
            FROM user_target_groups WHERE user_id = ?
            ORDER BY last_used DESC LIMIT 10
        """, (user_id,))
        rows = await cursor.fetchall()
        return [{'id': row['group_id'], 'title': row['group_title'], 
                 'username': row['username']} for row in rows]
    
    async def add_user_target_group(self, user_id: int, group_id: str, title: str, username: str = None):
        """Add or update user's target group."""
        await self.conn.execute("""
            INSERT INTO user_target_groups (user_id, group_id, group_title, username, last_used)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id, group_id) DO UPDATE SET
                group_title = excluded.group_title,
                username = excluded.username,
                last_used = CURRENT_TIMESTAMP
        """, (user_id, group_id, title, username))
        await self.conn.commit()
    
    async def update_user_group_last_used(self, user_id: int, group_id: str):
        """Update last used timestamp for source group."""
        await self.conn.execute("""
            UPDATE user_groups SET last_used = CURRENT_TIMESTAMP
            WHERE user_id = ? AND group_id = ?
        """, (user_id, group_id))
        await self.conn.commit()
    
    async def update_user_target_group_last_used(self, user_id: int, group_id: str):
        """Update last used timestamp for target group."""
        await self.conn.execute("""
            UPDATE user_target_groups SET last_used = CURRENT_TIMESTAMP
            WHERE user_id = ? AND group_id = ?
        """, (user_id, group_id))
        await self.conn.commit()
    
    # ============== Invite History ==============
    
    async def add_invite_record(self, task_id: int, user_telegram_id: int, 
                                username: str = None, first_name: str = None,
                                status: str = 'success', error_message: str = None):
        """Add invite history record."""
        if self.conn is None:
            logger.warning(f"Cannot add invite record for task {task_id}: database connection is closed")
            return
            
        await self.conn.execute("""
            INSERT INTO invite_history (task_id, user_telegram_id, username, first_name, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (task_id, user_telegram_id, username, first_name, status, error_message))
        await self.conn.commit()
    
    async def get_task_invite_history(self, task_id: int) -> List[Dict]:
        """Get invite history for a task."""
        cursor = await self.conn.execute("""
            SELECT * FROM invite_history WHERE task_id = ?
            ORDER BY invited_at DESC
        """, (task_id,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    
    async def get_invited_user_ids(self, source_group_id: int, target_group_id: int) -> set:
        """Get set of already invited user IDs for this source->target pair."""
        cursor = await self.conn.execute("""
            SELECT ih.user_telegram_id
            FROM invite_history ih
            JOIN invite_tasks it ON ih.task_id = it.id
            WHERE it.source_group_id = ? AND it.target_group_id = ? AND ih.status = 'success'
        """, (source_group_id, target_group_id))
        rows = await cursor.fetchall()
        return {row['user_telegram_id'] for row in rows}
    
    # ============== Parse Tasks ==============
    
    async def create_parse_task(self, task: 'ParseTask') -> int:
        """Create a new parse task."""
        from shared.models import ParseTask
        
        cursor = await self.conn.execute("""
            INSERT INTO parse_tasks (
                user_id, file_name, source_group_id, source_group_title, source_username,
                session_alias, status, parsed_count, saved_count, parse_limit, delay_seconds, delay_every,
                save_every, rotate_sessions, rotate_every, available_sessions, failed_sessions,
                current_offset, use_proxy, filter_admins, filter_inactive,
                inactive_threshold_days, error_message, parse_mode, keyword_filter, exclude_keywords,
                messages_limit, delay_every_requests, rotate_every_requests, save_every_users, messages_offset,
                source_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            task.user_id, task.file_name, task.source_group_id, task.source_group_title,
            task.source_username, task.session_alias, task.status, task.parsed_count,
            task.saved_count, task.limit, task.delay_seconds, task.delay_every, task.save_every,
            1 if task.rotate_sessions else 0, task.rotate_every, ','.join(task.available_sessions),
            ','.join(task.failed_sessions), task.current_offset,
            1 if task.use_proxy else 0, 1 if task.filter_admins else 0,
            1 if task.filter_inactive else 0, task.inactive_threshold_days,
            task.error_message, task.parse_mode,
            ','.join(task.keyword_filter) if task.keyword_filter else '',
            ','.join(task.exclude_keywords) if task.exclude_keywords else '',
            task.messages_limit, task.delay_every_requests, task.rotate_every_requests,
            task.save_every_users, task.messages_offset,
            getattr(task, 'source_type', 'group')
        ))
        await self.conn.commit()
        return cursor.lastrowid
    
    async def get_parse_task(self, task_id: int) -> Optional['ParseTask']:
        """Get parse task by ID."""
        cursor = await self.conn.execute("""
            SELECT * FROM parse_tasks WHERE id = ?
        """, (task_id,))
        row = await cursor.fetchone()
        
        if not row:
            return None
        
        return self._row_to_parse_task(row)
    
    async def update_parse_task(self, task_id: int, **kwargs):
        """Update parse task fields."""
        # Check if connection is still alive
        if self.conn is None:
            logger.warning(f"Cannot update parse task {task_id}: database connection is closed")
            return
        
        allowed_fields = {
            'status', 'parsed_count', 'saved_count', 'parse_limit', 'delay_seconds',
            'save_every', 'rotate_sessions', 'rotate_every', 'available_sessions',
            'failed_sessions', 'current_offset', 'use_proxy',
            'session_alias', 'error_message', 'messages_limit', 'delay_every_requests',
            'rotate_every_requests', 'save_every_users', 'messages_offset'
        }
        
        updates = []
        values = []
        
        for key, value in kwargs.items():
            if key in allowed_fields:
                if key in ['rotate_sessions', 'use_proxy']:
                    value = 1 if value else 0
                elif key in ['available_sessions', 'failed_sessions']:
                    value = ','.join(value) if isinstance(value, list) else value
                
                updates.append(f"{key} = ?")
                values.append(value)
        
        if not updates:
            return
        
        updates.append("updated_at = CURRENT_TIMESTAMP")
        values.append(task_id)
        
        query = f"UPDATE parse_tasks SET {', '.join(updates)} WHERE id = ?"
        await self.conn.execute(query, values)
        await self.conn.commit()
    
    async def delete_parse_task(self, task_id: int):
        """Delete a parse task."""
        await self.conn.execute("DELETE FROM parse_tasks WHERE id = ?", (task_id,))
        await self.conn.commit()
    
    async def get_user_parse_tasks(self, user_id: int, status: str = None) -> List['ParseTask']:
        """Get all parse tasks for a user."""
        if status:
            cursor = await self.conn.execute("""
                SELECT * FROM parse_tasks WHERE user_id = ? AND status = ?
                ORDER BY created_at DESC
            """, (user_id, status))
        else:
            cursor = await self.conn.execute("""
                SELECT * FROM parse_tasks WHERE user_id = ?
                ORDER BY created_at DESC
            """, (user_id,))
        
        rows = await cursor.fetchall()
        return [self._row_to_parse_task(row) for row in rows]
    
    async def get_running_parse_tasks(self) -> List['ParseTask']:
        """Get all running parse tasks."""
        cursor = await self.conn.execute("""
            SELECT * FROM parse_tasks WHERE status = 'running'
        """)
        rows = await cursor.fetchall()
        return [self._row_to_parse_task(row) for row in rows]
    
    def _row_to_parse_task(self, row) -> 'ParseTask':
        """Convert database row to ParseTask object."""
        from shared.models import ParseTask
        
        available_sessions = row['available_sessions'].split(',') if row['available_sessions'] else []
        failed_sessions = row['failed_sessions'].split(',') if row['failed_sessions'] else []
        
        # Parse keyword_filter and exclude_keywords
        keyword_filter = []
        if 'keyword_filter' in row.keys() and row['keyword_filter']:
            keyword_filter = [k.strip() for k in row['keyword_filter'].split(',') if k.strip()]
        
        exclude_keywords = []
        if 'exclude_keywords' in row.keys() and row['exclude_keywords']:
            exclude_keywords = [k.strip() for k in row['exclude_keywords'].split(',') if k.strip()]
        
        return ParseTask(
            id=row['id'],
            user_id=row['user_id'],
            file_name=row['file_name'],
            source_group_id=row['source_group_id'],
            source_group_title=row['source_group_title'],
            source_username=row['source_username'] if 'source_username' in row.keys() else None,
            source_type=row['source_type'] if 'source_type' in row.keys() and row['source_type'] else 'group',
            session_alias=row['session_alias'],
            status=row['status'],
            parsed_count=row['parsed_count'],
            saved_count=row['saved_count'] if 'saved_count' in row.keys() else 0,
            limit=row['parse_limit'],
            delay_seconds=row['delay_seconds'],
            delay_every=row['delay_every'] if 'delay_every' in row.keys() else 1,
            save_every=row['save_every'] if 'save_every' in row.keys() else 0,
            rotate_sessions=bool(row['rotate_sessions']),
            rotate_every=row['rotate_every'],
            use_proxy=bool(row['use_proxy']),
            available_sessions=available_sessions,
            failed_sessions=failed_sessions,
            current_offset=row['current_offset'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            error_message=row['error_message'],
            filter_admins=bool(row['filter_admins']),
            filter_inactive=bool(row['filter_inactive']),
            inactive_threshold_days=row['inactive_threshold_days'],
            parse_mode=row['parse_mode'] if 'parse_mode' in row.keys() else 'member_list',
            keyword_filter=keyword_filter,
            exclude_keywords=exclude_keywords,
            messages_limit=row['messages_limit'] if 'messages_limit' in row.keys() else None,
            delay_every_requests=row['delay_every_requests'] if 'delay_every_requests' in row.keys() else 1,
            rotate_every_requests=row['rotate_every_requests'] if 'rotate_every_requests' in row.keys() else 0,
            save_every_users=row['save_every_users'] if 'save_every_users' in row.keys() else 0,
            messages_offset=row['messages_offset'] if 'messages_offset' in row.keys() else 0
        )

