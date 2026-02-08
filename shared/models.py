# -*- coding: utf-8 -*-
"""
Shared models for the inviter bot.
"""
from dataclasses import dataclass, field
from typing import Optional, List, Literal
from datetime import datetime


@dataclass
class FilterMode:
    """Defines how users are filtered during inviting."""
    mode: Literal["all", "exclude_admins", "exclude_inactive", "exclude_admins_and_inactive"] = "all"
    inactive_threshold_days: Optional[int] = None  # Days, for "exclude_inactive" and "exclude_admins_and_inactive"


@dataclass
class InviteFilterSettings:
    """Settings related to filtering users during an invite task."""
    filter_mode: Literal["all", "exclude_admins", "exclude_inactive", "exclude_admins_and_inactive"] = "all"
    inactive_threshold_days: Optional[int] = None


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
    proxy: Optional[str] = None


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
    invite_mode: str = "member_list"  # member_list (default), message_based, or from_file
    file_source: Optional[str] = None  # File name for from_file mode
    status: str = "pending"  # pending, running, paused, completed, failed
    invited_count: int = 0
    limit: Optional[int] = None
    delay_seconds: int = 30
    delay_every: int = 1
    rotate_sessions: bool = False
    rotate_every: int = 0  # 0 means disabled/only on error
    use_proxy: bool = False
    available_sessions: List[str] = field(default_factory=list)
    failed_sessions: List[str] = field(default_factory=list)  # Сессии с критическими ошибками для этой задачи
    current_offset: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    error_message: Optional[str] = None
    # Filter settings
    filter_mode: Literal["all", "exclude_admins", "exclude_inactive", "exclude_admins_and_inactive"] = "all"
    inactive_threshold_days: Optional[int] = None
    # Timing and session tracking
    last_action_time: Optional[str] = None  # ISO timestamp of last invite action
    current_session: Optional[str] = None  # Currently active session (for rotation tracking)



@dataclass
class InviteSettings:
    """Settings for inviting users."""
    delay_seconds: int = 30
    delay_every: int = 1
    limit: Optional[int] = None
    rotate_sessions: bool = False
    rotate_every: int = 0
    use_proxy: bool = False
    session_aliases: List[str] = field(default_factory=list)


@dataclass
class ParseTask:
    """Represents a parsing task."""
    id: int
    user_id: int
    file_name: str
    source_group_id: int
    source_group_title: str
    session_alias: str
    source_username: Optional[str] = None
    source_type: str = "group"  # "group" or "channel" - determines parsing source type
    status: str = "pending"  # pending, running, paused, completed, failed
    parsed_count: int = 0
    saved_count: int = 0  # Track how many users have been saved to file
    limit: Optional[int] = None
    delay_seconds: int = 2
    delay_every: int = 1  # Apply delay after every N parsed users
    save_every: int = 0  # Save to file after every N users (0 = only at end)
    rotate_sessions: bool = False
    rotate_every: int = 0  # 0 means disabled/only on error
    use_proxy: bool = True
    available_sessions: List[str] = field(default_factory=list)
    failed_sessions: List[str] = field(default_factory=list)
    current_offset: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    error_message: Optional[str] = None
    # Filter settings
    filter_admins: bool = False
    filter_inactive: bool = False
    inactive_threshold_days: int = 30
    # Parse mode: member_list (default), message_based, or channel_comments
    parse_mode: str = "member_list"
    # Keyword filter for message_based mode - only include users who wrote messages containing these keywords
    keyword_filter: List[str] = field(default_factory=list)
    # Exclude keywords - exclude users who wrote messages containing these words
    exclude_keywords: List[str] = field(default_factory=list)
    # Message-based mode specific settings
    messages_limit: Optional[int] = None  # Limit by number of messages to process (for message_based mode)
    delay_every_requests: int = 1  # Apply delay after every N API requests (for message_based mode)
    rotate_every_requests: int = 0  # Rotate session after every N API requests (for message_based mode)
    save_every_users: int = 0  # Save to file after every N unique users found (for message_based mode, 0 = only at end)
    messages_offset: int = 0  # Offset for message history (for message_based mode resume)
    # Timing and session tracking
    last_action_time: Optional[str] = None  # ISO timestamp of last parse action
    current_session: Optional[str] = None  # Currently active session (for rotation tracking)


@dataclass
class PostParseTask:
    """Represents a post parsing task (parsing posts from channel/group to another channel/group)."""
    id: int
    user_id: int
    source_id: int  # Source channel/group ID
    source_title: str
    source_username: Optional[str] = None
    source_type: str = "channel"  # "channel" or "group"
    target_id: int = 0  # Target channel/group ID
    target_title: str = ""
    target_username: Optional[str] = None
    target_type: str = "channel"  # "channel" or "group"
    session_alias: str = ""
    status: str = "pending"  # pending, running, paused, completed, failed
    forwarded_count: int = 0  # Number of posts forwarded (counting media groups as 1)
    limit: Optional[int] = None  # Stop after N posts
    delay_seconds: int = 2  # Delay between posts
    delay_every: int = 1  # Apply delay after every N posts
    rotate_sessions: bool = False  # Enable session rotation
    rotate_every: int = 0  # Rotate session every N posts (0 = only on error)
    use_proxy: bool = True
    available_sessions: List[str] = field(default_factory=list)
    failed_sessions: List[str] = field(default_factory=list)
    current_offset: int = 0  # Current message ID offset for parsing
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    error_message: Optional[str] = None
    # Post filter settings
    filter_contacts: bool = False  # Filter out posts with contacts (usernames, phones, links)
    remove_contacts: bool = False  # Remove contacts from posts instead of skipping
    skip_on_contacts: bool = False  # Skip posts with contacts entirely (don't forward at all)
    # Direction: backward = from newest to oldest, forward = from oldest to newest
    parse_direction: str = "backward"
    # Media filter: all, media_only, text_only
    media_filter: str = "all"
    # Native forwarding settings
    use_native_forward: bool = False  # Use native Telegram forwarding (forward_messages) instead of copying
    check_content_if_native: bool = True  # Check for content before forwarding (only when use_native_forward=True)
    forward_show_source: bool = True  # Show "Forwarded from" in forwarded messages (only when use_native_forward=True)
    # Timing and session tracking
    last_action_time: Optional[str] = None
    current_session: Optional[str] = None
    # Last processed message ID for resume
    last_message_id: Optional[int] = None
    # Keyword filters
    keywords_whitelist: List[str] = field(default_factory=list)
    keywords_blacklist: List[str] = field(default_factory=list)

@dataclass
class PostMonitoringTask:
    """Represents a post monitoring task (real-time forwarding of new posts)."""
    id: int
    user_id: int
    source_id: int  # Source channel/group ID
    source_title: str
    source_username: Optional[str] = None
    source_type: str = "channel"  # "channel" or "group"
    target_id: int = 0  # Target channel/group ID
    target_title: str = ""
    target_username: Optional[str] = None
    target_type: str = "channel"  # "channel" or "group"
    session_alias: str = ""
    status: str = "pending"  # pending, running, paused, completed, failed
    forwarded_count: int = 0  # Number of posts forwarded (counting media groups as 1)
    limit: Optional[int] = None  # Stop after N posts (0 = unlimited)
    delay_seconds: int = 0  # Delay between forwarding
    rotate_sessions: bool = False  # Enable session rotation
    rotate_every: int = 0  # Rotate session every N posts (0 = only on error)
    use_proxy: bool = True
    available_sessions: List[str] = field(default_factory=list)
    failed_sessions: List[str] = field(default_factory=list)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    error_message: Optional[str] = None
    # Post filter settings
    filter_contacts: bool = False  # Skip posts with contacts (usernames, phones, links)
    remove_contacts: bool = False  # Remove contacts from posts instead of skipping
    skip_on_contacts: bool = False  # Skip posts with contacts entirely (don't forward at all)
    media_filter: str = "all"  # Media filter: all, media_only, text_only
    # Native forwarding settings
    use_native_forward: bool = False  # Use native Telegram forwarding (forward_messages) instead of copying
    check_content_if_native: bool = True  # Check for content before forwarding (only when use_native_forward=True)
    forward_show_source: bool = True  # Show "Forwarded from" in forwarded messages (only when use_native_forward=True)
    # Timing and session tracking
    last_action_time: Optional[str] = None
    current_session: Optional[str] = None
    # Keyword filters
    keywords_whitelist: List[str] = field(default_factory=list)
    keywords_blacklist: List[str] = field(default_factory=list)

