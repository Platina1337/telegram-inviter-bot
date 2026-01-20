# -*- coding: utf-8 -*-
"""
States and keyboards for the inviter bot.
"""
import re
from typing import Dict, List, Optional
from pyrogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, 
    InlineKeyboardMarkup, InlineKeyboardButton, 
    ReplyKeyboardRemove
)

from bot.api_client import api_client

# ============== Global State Storage ==============
user_states: Dict[int, Dict] = {}

# ============== FSM States ==============
FSM_MAIN_MENU = "main_menu"
FSM_NONE = None

# Invite flow states
FSM_INVITE_SOURCE_GROUP = "invite_source_group"
FSM_INVITE_TARGET_GROUP = "invite_target_group"
FSM_INVITE_SESSION_SELECT = "invite_session_select"
FSM_INVITE_MENU = "invite_menu"
FSM_INVITE_SETTINGS = "invite_settings"
FSM_INVITE_RUNNING = "invite_running"

# Settings input states
FSM_SETTINGS_DELAY = "settings_delay"
FSM_SETTINGS_DELAY_EVERY = "settings_delay_every"
FSM_SETTINGS_LIMIT = "settings_limit"
FSM_SETTINGS_ROTATE_EVERY = "settings_rotate_every"

# Session management states
FSM_SESSION_NAME = "session_name"
FSM_SESSION_API_ID = "session_api_id"
FSM_SESSION_API_HASH = "session_api_hash"
FSM_SESSION_PHONE = "session_phone"
FSM_SESSION_CODE = "session_code"
FSM_SESSION_PASSWORD = "session_password"


# ============== Channel/Group Parsing ==============

GROUP_BUTTON_PATTERN = re.compile(
    r"(?P<title>.+?) \(ID: (?P<id>-?\d+)(?:, @(?P<username>\w+))?\)$"
)


def parse_group_button(text: str) -> Optional[Dict]:
    """Parse group button text to extract title, id, username."""
    if not text:
        return None
    
    match = GROUP_BUTTON_PATTERN.match(text.strip())
    if not match:
        return None
    
    return {
        "title": match.group("title").strip(),
        "id": match.group("id"),
        "username": match.group("username")
    }


def format_group_button(title: str, group_id, username: str = None) -> str:
    """Format group button text."""
    clean_title = (title or "").strip()
    clean_id = str(group_id) if group_id else "?"
    
    if username:
        return f"{clean_title} (ID: {clean_id}, @{username})"
    return f"{clean_title} (ID: {clean_id})"


def normalize_group_input(text: str) -> str:
    """Normalize user input for group identification."""
    text = (text or "").strip()
    
    if text.startswith("https://"):
        text = text[8:]
    elif text.startswith("http://"):
        text = text[7:]
    
    if text.startswith("t.me/"):
        username = text[5:]
        if "?" in username:
            username = username.split("?")[0]
        return username
    
    if text.startswith("@"):
        return text[1:]
    
    # Check for button pattern
    match = re.search(r'\(ID:\s*(-?\d+)', text)
    if match:
        return match.group(1)
    
    return text


# ============== Keyboards ==============

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Main menu keyboard."""
    return ReplyKeyboardMarkup([
        [KeyboardButton("👥 Инвайтинг")],
        [KeyboardButton("📊 Статус задач")],
        [KeyboardButton("🔐 Сессии")]
    ], resize_keyboard=True)


async def get_group_history_keyboard(user_id: int) -> Optional[ReplyKeyboardMarkup]:
    """Keyboard with user's source group history."""
    groups = await api_client.get_user_groups(user_id)
    
    if not groups:
        return None
    
    buttons = []
    for group in groups:
        title = group.get('title', '')
        group_id = group.get('id', '')
        username = group.get('username', '')
        btn_text = format_group_button(title, group_id, username)
        buttons.append([KeyboardButton(btn_text)])
    
    buttons.append([KeyboardButton("🔙 Назад")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


async def get_target_group_history_keyboard(user_id: int) -> Optional[ReplyKeyboardMarkup]:
    """Keyboard with user's target group history."""
    groups = await api_client.get_user_target_groups(user_id)
    
    if not groups:
        return None
    
    buttons = []
    for group in groups:
        title = group.get('title', '')
        group_id = group.get('id', '')
        username = group.get('username', '')
        btn_text = format_group_button(title, group_id, username)
        buttons.append([KeyboardButton(btn_text)])
    
    buttons.append([KeyboardButton("🔙 Назад")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


def get_invite_menu_keyboard(task_id: int = None) -> InlineKeyboardMarkup:
    """Invite menu with action buttons."""
    buttons = [
        [InlineKeyboardButton("🚀 Запустить инвайтинг", callback_data=f"invite_start:{task_id or 0}")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="invite_settings")],
        [InlineKeyboardButton("📊 Статус задач", callback_data="invite_status")],
        [InlineKeyboardButton("🔙 Назад", callback_data="invite_back")]
    ]
    return InlineKeyboardMarkup(buttons)


def get_invite_running_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for running invite task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏸️ Приостановить", callback_data=f"invite_pause:{task_id}")],
        [InlineKeyboardButton("⏹️ Остановить", callback_data=f"invite_stop:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"invite_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"invite_refresh:{task_id}")]
    ])


def get_invite_paused_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for paused invite task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Продолжить", callback_data=f"invite_resume:{task_id}")],
        [InlineKeyboardButton("⏹️ Остановить", callback_data=f"invite_stop:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"invite_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"invite_refresh:{task_id}")]
    ])


def get_settings_keyboard(current_settings: Dict = None) -> InlineKeyboardMarkup:
    """Settings menu keyboard."""
    settings = current_settings or {}
    
    delay = settings.get('delay_seconds', 30)
    delay_every = settings.get('delay_every', 1)
    limit = settings.get('limit')
    rotate = settings.get('rotate_sessions', False)
    rotate_every = settings.get('rotate_every', 0)
    
    limit_text = str(limit) if limit else "Без лимита"
    rotate_text = "✅" if rotate else "❌"
    rotate_every_text = f"По кругу ({rotate_every} инв.)" if rotate and rotate_every > 0 else "При ошибке"
    
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⏱️ Задержка: {delay} сек", callback_data="settings_delay")],
        [InlineKeyboardButton(f"🔢 Каждые {delay_every} инвайта", callback_data="settings_delay_every")],
        [InlineKeyboardButton(f"🔢 Лимит: {limit_text}", callback_data="settings_limit")],
        [InlineKeyboardButton(f"🔄 Ротация сессий: {rotate_text}", callback_data="settings_rotate")],
        [InlineKeyboardButton(f"🔄 Ротация каждые: {rotate_every} инв.", callback_data="settings_rotate_every")],
        [InlineKeyboardButton("🔐 Выбор сессий", callback_data="settings_sessions")],
        [InlineKeyboardButton("🔙 Назад", callback_data="settings_back")]
    ])


async def get_session_select_keyboard(selected_aliases: List[str] = None) -> InlineKeyboardMarkup:
    """Keyboard for selecting sessions for inviting."""
    selected = selected_aliases or []
    
    result = await api_client.list_sessions()
    sessions = result.get('sessions', [])
    
    buttons = []
    for session in sessions:
        alias = session.get('alias', '')
        phone = session.get('phone', '')
        is_selected = alias in selected
        
        prefix = "✅" if is_selected else "⬜"
        btn_text = f"{prefix} {alias} ({phone})"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"toggle_session:{alias}")])
    
    buttons.append([InlineKeyboardButton("✅ Готово", callback_data="sessions_done")])
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="sessions_back")])
    
    return InlineKeyboardMarkup(buttons)


# ============== Session Management Keyboards ==============

def get_sessions_menu_keyboard() -> InlineKeyboardMarkup:
    """Session management menu."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить сессию", callback_data="add_session")],
        [InlineKeyboardButton("🔄 Назначить сессию", callback_data="assign_session")],
        [InlineKeyboardButton("❌ Удалить сессию", callback_data="delete_session")],
        [InlineKeyboardButton("🔙 Назад", callback_data="sessions_menu_back")]
    ])


async def get_session_list_keyboard(action: str = "select") -> InlineKeyboardMarkup:
    """Keyboard with list of sessions."""
    result = await api_client.list_sessions()
    sessions = result.get('sessions', [])
    
    buttons = []
    for session in sessions:
        alias = session.get('alias', '')
        phone = session.get('phone', '')
        is_active = session.get('is_active', False)
        
        status = "🟢" if is_active else "🔴"
        btn_text = f"{status} {alias} ({phone})"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"{action}_session:{alias}")])
    
    buttons.append([InlineKeyboardButton("🔙 Отмена", callback_data="cancel_session_action")])
    return InlineKeyboardMarkup(buttons)


def get_task_assignment_keyboard(session_alias: str) -> InlineKeyboardMarkup:
    """Keyboard for assigning session to task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Инвайтинг", callback_data=f"assign_task:inviting:{session_alias}")],
        [InlineKeyboardButton("❌ Убрать из инвайтинга", callback_data=f"remove_task:inviting:{session_alias}")],
        [InlineKeyboardButton("🔙 Отмена", callback_data="cancel_session_action")]
    ])


# ============== Formatting Functions ==============

def format_invite_status(task_data: Dict) -> str:
    """Format invite task status message."""
    status_icons = {
        'pending': '⏳',
        'running': '🚀',
        'paused': '⏸️',
        'completed': '✅',
        'failed': '❌'
    }
    
    status = task_data.get('status', 'pending')
    icon = status_icons.get(status, '❓')
    
    invited = task_data.get('invited_count', 0)
    limit = task_data.get('limit')
    limit_text = f"/{limit}" if limit else ""
    
    rotate_info = 'Да' if task_data.get('rotate_sessions') else 'Нет'
    if task_data.get('rotate_sessions') and task_data.get('rotate_every', 0) > 0:
        rotate_info += f" (каждые {task_data['rotate_every']} инв.)"
    
    text = f"""
{icon} **Статус инвайтинга**

📤 Источник: {task_data.get('source_group', 'N/A')}
📥 Цель: {task_data.get('target_group', 'N/A')}

👥 Приглашено: {invited}{limit_text}
⏱️ Задержка: ~{task_data.get('delay_seconds', 30)} сек (каждые {task_data.get('delay_every', 1)} инв.)
🔐 Сессия: {task_data.get('session', 'N/A')}
🔄 Ротация: {rotate_info}

📋 Статус: {status.capitalize()}
"""
    
    if task_data.get('error_message'):
        text += f"\n⚠️ Ошибка: {task_data['error_message']}"
    
    return text.strip()


def format_sessions_list(sessions: List[Dict], assignments: Dict) -> str:
    """Format sessions list message."""
    text = "📱 **Управление сессиями**\n\n"
    text += "**Доступные сессии:**\n"
    
    if sessions:
        for session in sessions:
            alias = session.get('alias', '')
            phone = session.get('phone', '')
            is_active = session.get('is_active', False)
            status = '🟢' if is_active else '🔴'
            text += f"- {status} **{alias}** | `{phone}`\n"
    else:
        text += "Нет доступных сессий.\n"
    
    text += "\n**Назначения:**\n"
    inviting_sessions = assignments.get('inviting', [])
    if inviting_sessions:
        text += f"- Инвайтинг: {', '.join(inviting_sessions)}\n"
    else:
        text += "- Инвайтинг: не назначено\n"
    
    return text
