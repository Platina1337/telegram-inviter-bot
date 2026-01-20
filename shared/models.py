# -*- coding: utf-8 -*-
"""
Shared models for the inviter bot.
"""
from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime


@dataclass
class SessionMeta:
    """Metadata for a Telegram session."""
    id: int
    alias: str
    api_id: int
    api_hash: str
    phone: str
    session_path: str
    is_active: bool = True
    user_id: Optional[int] = None
    created_at: Optional[str] = None
    assigned_tasks: List[str] = field(default_factory=list)


@dataclass
class InviteTask:
    """Represents an invite task."""
    id: int
    user_id: int
    source_group_id: int
    source_group_title: str
    target_group_id: int
    target_group_title: str
    session_alias: str
    source_username: Optional[str] = None
    target_username: Optional[str] = None
    invite_mode: str = "member_list"  # member_list (default) or message_based
    status: str = "pending"  # pending, running, paused, completed, failed
    invited_count: int = 0
    limit: Optional[int] = None
    delay_seconds: int = 30
    delay_every: int = 1
    rotate_sessions: bool = False
    rotate_every: int = 0  # 0 means disabled/only on error
    available_sessions: List[str] = field(default_factory=list)
    current_offset: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class InviteSettings:
    """Settings for inviting users."""
    delay_seconds: int = 30
    delay_every: int = 1
    limit: Optional[int] = None
    rotate_sessions: bool = False
    rotate_every: int = 0
    session_aliases: List[str] = field(default_factory=list)
