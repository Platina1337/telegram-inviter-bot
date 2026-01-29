# -*- coding: utf-8 -*-
"""
Typed user state management for the inviter bot.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime


@dataclass
class GroupInfo:
    """Information about a Telegram group."""
    id: int
    title: str
    username: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'title': self.title,
            'username': self.username
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'GroupInfo':
        return cls(
            id=data.get('id', 0),
            title=data.get('title', ''),
            username=data.get('username')
        )


@dataclass 
class InviteSettings:
    """Settings for invite task."""
    delay_seconds: int = 30
    delay_every: int = 1
    limit: Optional[int] = None
    rotate_sessions: bool = False
    rotate_every: int = 0
    use_proxy: bool = False
    filter_mode: str = 'all'
    inactive_threshold_days: Optional[int] = None
    
    def to_dict(self) -> dict:
        return {
            'delay_seconds': self.delay_seconds,
            'delay_every': self.delay_every,
            'limit': self.limit,
            'rotate_sessions': self.rotate_sessions,
            'rotate_every': self.rotate_every,
            'use_proxy': self.use_proxy,
            'filter_mode': self.filter_mode,
            'inactive_threshold_days': self.inactive_threshold_days
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'InviteSettings':
        return cls(
            delay_seconds=data.get('delay_seconds', 30),
            delay_every=data.get('delay_every', 1),
            limit=data.get('limit'),
            rotate_sessions=data.get('rotate_sessions', False),
            rotate_every=data.get('rotate_every', 0),
            use_proxy=data.get('use_proxy', False),
            filter_mode=data.get('filter_mode', 'all'),
            inactive_threshold_days=data.get('inactive_threshold_days')
        )


@dataclass
class ParseSettings:
    """Settings for parse task."""
    limit: Optional[int] = None
    delay_seconds: int = 2
    delay_every: int = 1
    rotate_sessions: bool = False
    rotate_every: int = 0
    save_every: int = 0
    use_proxy: bool = True
    filter_admins: bool = False
    filter_inactive: bool = False
    inactive_threshold_days: int = 30
    parse_mode: str = 'member_list'
    keyword_filter: List[str] = field(default_factory=list)
    exclude_keywords: List[str] = field(default_factory=list)
    
    # Message-based mode settings
    messages_limit: Optional[int] = None
    delay_every_requests: int = 1
    rotate_every_requests: int = 0
    save_every_users: int = 0
    
    def to_dict(self) -> dict:
        return {
            'limit': self.limit,
            'delay_seconds': self.delay_seconds,
            'delay_every': self.delay_every,
            'rotate_sessions': self.rotate_sessions,
            'rotate_every': self.rotate_every,
            'save_every': self.save_every,
            'use_proxy': self.use_proxy,
            'filter_admins': self.filter_admins,
            'filter_inactive': self.filter_inactive,
            'inactive_threshold_days': self.inactive_threshold_days,
            'parse_mode': self.parse_mode,
            'keyword_filter': self.keyword_filter,
            'exclude_keywords': self.exclude_keywords,
            'messages_limit': self.messages_limit,
            'delay_every_requests': self.delay_every_requests,
            'rotate_every_requests': self.rotate_every_requests,
            'save_every_users': self.save_every_users
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'ParseSettings':
        return cls(
            limit=data.get('limit'),
            delay_seconds=data.get('delay_seconds', 2),
            delay_every=data.get('delay_every', 1),
            rotate_sessions=data.get('rotate_sessions', False),
            rotate_every=data.get('rotate_every', 0),
            save_every=data.get('save_every', 0),
            use_proxy=data.get('use_proxy', True),
            filter_admins=data.get('filter_admins', False),
            filter_inactive=data.get('filter_inactive', False),
            inactive_threshold_days=data.get('inactive_threshold_days', 30),
            parse_mode=data.get('parse_mode', 'member_list'),
            keyword_filter=data.get('keyword_filter', []),
            exclude_keywords=data.get('exclude_keywords', []),
            messages_limit=data.get('messages_limit'),
            delay_every_requests=data.get('delay_every_requests', 1),
            rotate_every_requests=data.get('rotate_every_requests', 0),
            save_every_users=data.get('save_every_users', 0)
        )


@dataclass
class UserState:
    """
    Typed user state container.
    
    Provides type hints and validation for user state management.
    Can be converted to/from dict for backward compatibility.
    """
    state: Optional[str] = None
    
    # Source and target groups
    source_group: Optional[GroupInfo] = None
    target_group: Optional[GroupInfo] = None
    
    # File selection
    source_file: Optional[str] = None
    
    # Session selection
    session_alias: Optional[str] = None
    selected_sessions: List[str] = field(default_factory=list)
    
    # Task tracking
    task_id: Optional[int] = None
    
    # Settings
    invite_settings: InviteSettings = field(default_factory=InviteSettings)
    parse_settings: ParseSettings = field(default_factory=ParseSettings)
    
    # File manager state
    fm_selected_file: Optional[str] = None
    fm_page: int = 0
    fm_filter_mode: Optional[str] = None
    
    # Parse file name
    parse_file_name: Optional[str] = None
    
    # Tasks page for pagination
    tasks_page: int = 0
    
    def to_dict(self) -> dict:
        """Convert to dict for backward compatibility."""
        result = {
            'state': self.state,
            'source_file': self.source_file,
            'session_alias': self.session_alias,
            'selected_sessions': self.selected_sessions,
            'task_id': self.task_id,
            'invite_settings': self.invite_settings.to_dict(),
            'parse_settings': self.parse_settings.to_dict(),
            'fm_selected_file': self.fm_selected_file,
            'fm_page': self.fm_page,
            'fm_filter_mode': self.fm_filter_mode,
            'parse_file_name': self.parse_file_name,
            'tasks_page': self.tasks_page
        }
        
        if self.source_group:
            result['source_group'] = self.source_group.to_dict()
        if self.target_group:
            result['target_group'] = self.target_group.to_dict()
        
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> 'UserState':
        """Create from dict for backward compatibility."""
        source_group = None
        if data.get('source_group'):
            source_group = GroupInfo.from_dict(data['source_group'])
        
        target_group = None
        if data.get('target_group'):
            target_group = GroupInfo.from_dict(data['target_group'])
        
        invite_settings = InviteSettings.from_dict(data.get('invite_settings', {}))
        parse_settings = ParseSettings.from_dict(data.get('parse_settings', {}))
        
        return cls(
            state=data.get('state'),
            source_group=source_group,
            target_group=target_group,
            source_file=data.get('source_file'),
            session_alias=data.get('session_alias'),
            selected_sessions=data.get('selected_sessions', []),
            task_id=data.get('task_id'),
            invite_settings=invite_settings,
            parse_settings=parse_settings,
            fm_selected_file=data.get('fm_selected_file'),
            fm_page=data.get('fm_page', 0),
            fm_filter_mode=data.get('fm_filter_mode'),
            parse_file_name=data.get('parse_file_name'),
            tasks_page=data.get('tasks_page', 0)
        )


def get_user_state(user_states: Dict[int, Dict], user_id: int) -> Dict:
    """
    Get or create user state dictionary.
    
    This is a helper that ensures the user has a state entry.
    Returns the raw dict for backward compatibility.
    """
    if user_id not in user_states:
        user_states[user_id] = {
            'state': None,
            'invite_settings': {},
            'parse_settings': {},
            'selected_sessions': [],
            'fm_page': 0,
            'tasks_page': 0
        }
    return user_states[user_id]


def clear_user_state(user_states: Dict[int, Dict], user_id: int, keep_settings: bool = True):
    """
    Clear user state, optionally keeping settings.
    
    Args:
        user_states: The global user states dict
        user_id: User ID to clear
        keep_settings: If True, preserve invite_settings and parse_settings
    """
    if user_id in user_states:
        if keep_settings:
            old_invite_settings = user_states[user_id].get('invite_settings', {})
            old_parse_settings = user_states[user_id].get('parse_settings', {})
            user_states[user_id] = {
                'state': None,
                'invite_settings': old_invite_settings,
                'parse_settings': old_parse_settings,
                'selected_sessions': [],
                'fm_page': 0,
                'tasks_page': 0
            }
        else:
            user_states[user_id] = {
                'state': None,
                'invite_settings': {},
                'parse_settings': {},
                'selected_sessions': [],
                'fm_page': 0,
                'tasks_page': 0
            }
