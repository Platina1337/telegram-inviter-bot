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
FSM_SETTINGS_FILTER_MODE = "settings_filter_mode"
FSM_SETTINGS_INACTIVE_THRESHOLD_DAYS = "settings_inactive_threshold_days"

# Session management states
FSM_SESSION_NAME = "session_name"
FSM_SESSION_API_ID = "session_api_id"
FSM_SESSION_API_HASH = "session_api_hash"
FSM_SESSION_PHONE = "session_phone"
FSM_SESSION_CODE = "session_code"
FSM_SESSION_PASSWORD = "session_password"
FSM_SESSION_PROXY = "session_proxy"

# Parsing to file states
FSM_PARSE_FILE_NAME = "parse_file_name"
FSM_PARSE_SOURCE_TYPE = "parse_source_type"  # New: select between channel or group
FSM_PARSE_SOURCE_GROUP = "parse_source_group"
FSM_PARSE_SETTINGS = "parse_settings"
FSM_PARSE_INACTIVE_DAYS = "parse_inactive_days"
FSM_PARSE_SETTINGS_LIMIT = "parse_settings_limit"
FSM_PARSE_SETTINGS_DELAY = "parse_settings_delay"
FSM_PARSE_SETTINGS_ROTATE_EVERY = "parse_settings_rotate_every"
FSM_PARSE_SETTINGS_SAVE_EVERY = "parse_settings_save_every"
FSM_PARSE_SESSION_SELECT = "parse_session_select"
FSM_PARSE_KEYWORD_FILTER = "parse_keyword_filter"
FSM_PARSE_EXCLUDE_KEYWORDS = "parse_exclude_keywords"
# Message-based mode specific states
FSM_PARSE_MSG_LIMIT = "parse_msg_limit"
FSM_PARSE_MSG_DELAY_EVERY = "parse_msg_delay_every"
FSM_PARSE_MSG_ROTATE_EVERY = "parse_msg_rotate_every"
FSM_PARSE_MSG_SAVE_EVERY = "parse_msg_save_every"

# Inviting from file states
FSM_INVITE_FILE_SELECT = "invite_file_select"
FSM_INVITE_FROM_FILE_TARGET = "invite_from_file_target"

# File Manager states
FSM_FILE_MANAGER = "file_manager"
FSM_FILE_MANAGER_ACTION = "file_manager_action"
FSM_FILE_MANAGER_COPY_NAME = "file_manager_copy_name"
FSM_FILE_MANAGER_RENAME = "file_manager_rename"
FSM_FILE_MANAGER_FILTER_KEYWORD = "file_manager_filter_keyword"

# ============== User State Keys (standardized) ==============
# Use these constants instead of raw strings for consistency
STATE_KEY = 'state'
STATE_KEY_SOURCE_FILE = 'source_file'  # Primary key for file selection
STATE_KEY_SOURCE_GROUP = 'source_group'
STATE_KEY_TARGET_GROUP = 'target_group'
STATE_KEY_SESSION_ALIAS = 'session_alias'
STATE_KEY_TASK_ID = 'task_id'
STATE_KEY_INVITE_SETTINGS = 'invite_settings'
STATE_KEY_PARSE_SETTINGS = 'parse_settings'
STATE_KEY_FM_SELECTED_FILE = 'fm_selected_file'
STATE_KEY_FM_PAGE = 'fm_page'
STATE_KEY_FM_FILTER_MODE = 'fm_filter_mode'

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
        [KeyboardButton("🔍 Парсинг в файл")],
        [KeyboardButton("📁 Менеджер файлов")],
        [KeyboardButton("📊 Статус задач")],
        [KeyboardButton("🔐 Сессии")]
    ], resize_keyboard=True)




async def get_group_history_keyboard(user_id: int) -> Optional[ReplyKeyboardMarkup]:
    """Keyboard with user's source group history and file options."""
    groups = await api_client.get_user_groups(user_id)
    
    # Get available user files
    from shared.user_files_manager import UserFilesManager
    manager = UserFilesManager()
    files = manager.list_user_files()
    
    buttons = []
    
    # Add file buttons first
    if files:
        for file_info in files[:5]:  # Limit to 5 files
            file_name = file_info['name']
            count = file_info['count']
            btn_text = f"📁 {file_name} ({count} юзеров)"
            buttons.append([KeyboardButton(btn_text)])
    
    # Add group history
    if groups:
        for group in groups:
            title = group.get('title', '')
            group_id = group.get('id', '')
            username = group.get('username', '')
            btn_text = format_group_button(title, group_id, username)
            buttons.append([KeyboardButton(btn_text)])
    
    if not buttons:
        return None
    
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


async def get_parse_source_group_history_keyboard(user_id: int) -> Optional[ReplyKeyboardMarkup]:
    """Keyboard with user's source group history for parsing (without files)."""
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


def get_parse_running_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for running parse task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏸️ Приостановить", callback_data=f"parse_stop:{task_id}")],
        [InlineKeyboardButton("⏹️ Остановить", callback_data=f"parse_stop:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"parse_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"parse_refresh:{task_id}")]
    ])


def get_parse_paused_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for paused parse task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Продолжить", callback_data=f"parse_resume:{task_id}")],
        [InlineKeyboardButton("⏹️ Остановить", callback_data=f"parse_stop:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"parse_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"parse_refresh:{task_id}")]
    ])


def get_settings_keyboard(current_settings: Dict = None) -> InlineKeyboardMarkup:
    """Settings menu keyboard."""
    settings = current_settings or {}
    
    delay = settings.get('delay_seconds', 30)
    delay_every = settings.get('delay_every', 1)
    limit = settings.get('limit')
    rotate = settings.get('rotate_sessions', False)
    rotate = settings.get('rotate_sessions', False)
    rotate_every = settings.get('rotate_every', 0)
    use_proxy = settings.get('use_proxy', True)
    
    limit_text = str(limit) if limit else "Без лимита"
    rotate_text = "✅" if rotate else "❌"
    proxy_text = "✅" if use_proxy else "❌"
    rotate_every_text = f"По кругу ({rotate_every} инв.)" if rotate and rotate_every > 0 else "При ошибке"

    filter_mode = settings.get('filter_mode', 'all')
    inactive_threshold_days = settings.get('inactive_threshold_days')

    filter_mode_text = {
        "all": "Всех",
        "exclude_admins": "Кроме админов",
        "exclude_inactive": "Кроме неактивных",
        "exclude_admins_and_inactive": "Кроме админов и неактивных"
    }.get(filter_mode, "Всех")

    inactive_threshold_text = f"{inactive_threshold_days} дн." if inactive_threshold_days is not None else "Выкл."
    
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⏱️ Задержка: {delay} сек", callback_data="settings_delay")],
        [InlineKeyboardButton(f"🔢 Каждые {delay_every} инвайта", callback_data="settings_delay_every")],
        [InlineKeyboardButton(f"🔢 Лимит: {limit_text}", callback_data="settings_limit")],
        [InlineKeyboardButton(f"🔄 Ротация сессий: {rotate_text}", callback_data="settings_rotate")],
        [InlineKeyboardButton(f"🔄 Ротация каждые: {rotate_every} инв.", callback_data="settings_rotate_every")],
        [InlineKeyboardButton(f"🌐 Использовать прокси: {proxy_text}", callback_data="settings_proxy")],
        [InlineKeyboardButton(f"👥 Фильтр: {filter_mode_text}", callback_data="settings_filter_mode")],
        [InlineKeyboardButton(f"🛌 Неактивен >: {inactive_threshold_text}", callback_data="settings_inactive_threshold_days")],
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
        [InlineKeyboardButton("🔍 Парсинг", callback_data=f"assign_task:parsing:{session_alias}")],
        [InlineKeyboardButton("❌ Убрать из парсинга", callback_data=f"remove_task:parsing:{session_alias}")],
        [InlineKeyboardButton("🌐 Настроить прокси", callback_data=f"set_proxy:{session_alias}")],
        [InlineKeyboardButton("🧪 Проверить прокси", callback_data=f"test_proxy:{session_alias}")],
        [InlineKeyboardButton("🗑️ Удалить прокси", callback_data=f"remove_proxy:{session_alias}")],
        [InlineKeyboardButton("📋 Копировать прокси", callback_data=f"copy_proxy:{session_alias}")],
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
    
    proxy_info = 'Да' if task_data.get('use_proxy') else 'Нет'

    filter_mode = task_data.get('filter_mode', 'all')
    inactive_threshold_days = task_data.get('inactive_threshold_days')

    filter_mode_text = {
        "all": "Всех",
        "exclude_admins": "Кроме админов",
        "exclude_inactive": "Кроме неактивных",
        "exclude_admins_and_inactive": "Кроме админов и неактивных"
    }.get(filter_mode, "Всех")

    inactive_threshold_text = f"{inactive_threshold_days} дн." if inactive_threshold_days is not None else "Выкл."
    
    # Format available sessions list
    available_sessions = task_data.get('available_sessions', [])
    if available_sessions:
        sessions_text = ', '.join(available_sessions)
    else:
        # Fallback to current session if available_sessions is empty
        current_session = task_data.get('session', 'N/A')
        sessions_text = current_session
    
    source_display = task_data.get('source_group', 'N/A')
    if (not source_display or source_display == 'N/A') and task_data.get('file_source'):
        source_display = f"📄 {task_data['file_source']}"

    text = f"""
{icon} **Статус инвайтинга**

📤 Источник: {source_display}
📥 Цель: {task_data.get('target_group', 'N/A')}

👥 Приглашено: {invited}{limit_text}
⏱️ Задержка: ~{task_data.get('delay_seconds', 30)} сек (каждые {task_data.get('delay_every', 1)} инв.)
🔐 Сессия: {task_data.get('session', 'N/A')}
📋 Сессии: {sessions_text}
🔄 Ротация: {rotate_info}
🌐 Прокси: {proxy_info}
👥 Фильтр: {filter_mode_text}
🛌 Неактивен >: {inactive_threshold_text}

📋 Статус: {status.capitalize()}
"""
    
    if task_data.get('error_message'):
        text += f"\n⚠️ Ошибка: {task_data['error_message']}"
    
    return text.strip()


def format_parse_status(task_data: Dict) -> str:
    """Format parse task status message."""
    status = task_data.get('status', 'unknown')
    status_icons = {
        'pending': '⏳',
        'running': '🚀',
        'paused': '⏸️',
        'completed': '✅',
        'failed': '❌'
    }
    icon = status_icons.get(status, '❓')
    
    parse_mode = task_data.get('parse_mode', 'member_list')
    source_type = task_data.get('source_type', 'group')
    if source_type == 'channel':
        mode_text = "Из комментариев канала"
    elif parse_mode == 'member_list':
        mode_text = "По участникам"
    else:
        mode_text = "По сообщениям"
    
    parsed = task_data.get('parsed_count', 0)
    saved = task_data.get('saved_count', 0)
    
    proxy_info = "Используется" if task_data.get('use_proxy') else "Выкл"
    
    # Session info
    available_sessions = task_data.get('available_sessions', [])
    if available_sessions:
        sessions_text = ', '.join(available_sessions)
    else:
        sessions_text = task_data.get('session', 'N/A')
        
    # Filters
    filters = []
    if task_data.get('filter_admins'): filters.append("Админы")
    if task_data.get('filter_inactive'): 
        days = task_data.get('inactive_threshold_days', 30)
        filters.append(f"Неактивные (> {days} дн.)")
    filter_text = ", ".join(filters) if filters else "Нет"
    
    # Build text based on mode
    text = f"""
{icon} **Статус парсинга**

📝 Файл: **{task_data.get('file_name', 'N/A')}**
📤 Источник: {task_data.get('source_group', 'N/A')}
📋 Режим: {mode_text}
"""
    
    if parse_mode == 'message_based' or source_type == 'channel':
        # Message-based / channel comments mode
        messages_offset = task_data.get('messages_offset', 0)
        messages_limit = task_data.get('messages_limit')
        messages_limit_text = f"/{messages_limit}" if messages_limit else " (без лимита)"
        
        delay_every_requests = task_data.get('delay_every_requests', 1)
        rotate_every_requests = task_data.get('rotate_every_requests', 0)
        save_every_users = task_data.get('save_every_users', 0)
        
        save_every_text = f"каждые {save_every_users} польз." if save_every_users > 0 else "в конце"
        
        rotate = task_data.get('rotate_sessions', False)
        rotate_info = "Выкл"
        if rotate:
            rotate_info = f"каждые {rotate_every_requests} запр." if rotate_every_requests > 0 else "только при ошибках"
        
        msg_label = "Обработано постов" if source_type == 'channel' else "Обработано сообщений"
        text += f"""
📨 {msg_label}: {messages_offset}{messages_limit_text}
👥 Найдено пользователей: {parsed}
💾 Сохранено в файл: {saved}
📥 Сохранение: {save_every_text}
⏱️ Задержка: {task_data.get('delay_seconds', 2)} сек каждые {delay_every_requests} запр.
🔐 Текущая сессия: {task_data.get('session', 'N/A')}
📋 Все сессии: {sessions_text}
🔄 Ротация: {rotate_info}
🌐 Прокси: {proxy_info}
"""
        if source_type != 'channel':
            text += f"🚫 Исключать: {filter_text}\n"
    else:
        # Member list mode info (original)
        limit = task_data.get('limit')
        limit_text = f"/{limit}" if limit else " (без лимита)"
        
        save_every = task_data.get('save_every', 0)
        save_every_text = f"каждые {save_every} польз." if save_every > 0 else "в конце"
        
        rotate = task_data.get('rotate_sessions', False)
        rotate_every = task_data.get('rotate_every', 0)
        rotate_info = "Выкл"
        if rotate:
            rotate_info = "Да" if rotate_every == 0 else f"каждые {rotate_every} польз."
        
        text += f"""
👥 Спаршено: {parsed}{limit_text}
💾 Сохранено в файл: {saved}
📥 Сохранение: {save_every_text}
⏱️ Задержка: {task_data.get('delay_seconds', 2)} сек
🔐 Текущая сессия: {task_data.get('session', 'N/A')}
📋 Все сессии: {sessions_text}
🔄 Ротация: {rotate_info}
🌐 Прокси: {proxy_info}
🚫 Исключать: {filter_text}
"""
    
    text += f"\n📋 Статус: {status.capitalize()}"
    
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
            proxy = session.get('proxy', '')
            status = '🟢' if is_active else '🔴'
            proxy_status = '🌐' if proxy else '❌'
            text += f"- {status} **{alias}** | `{phone}` {proxy_status}\n"
    else:
        text += "Нет доступных сессий.\n"

    text += "\n**Назначения:**\n"
    inviting_sessions = assignments.get('inviting', [])
    if inviting_sessions:
        text += f"- Инвайтинг: {', '.join(inviting_sessions)}\n"
    else:
        text += "- Инвайтинг: не назначено\n"
    
    parsing_sessions = assignments.get('parsing', [])
    if parsing_sessions:
        text += f"- Парсинг: {', '.join(parsing_sessions)}\n"
    else:
        text += "- Парсинг: не назначено\n"

    return text


# ============== Parsing to File Keyboards ==============

def get_parse_settings_keyboard(current_settings: Dict = None) -> InlineKeyboardMarkup:
    """Settings menu keyboard for parsing."""
    settings = current_settings or {}
    
    limit = settings.get('limit')
    delay = settings.get('delay_seconds', 2)
    save_every = settings.get('save_every', 0)
    rotate = settings.get('rotate_sessions', False)
    rotate_every = settings.get('rotate_every', 0)
    use_proxy = settings.get('use_proxy', True)
    filter_admins = settings.get('filter_admins', False)
    filter_inactive = settings.get('filter_inactive', False)
    inactive_days = settings.get('inactive_days', 30)
    
    # New message-based mode settings
    parse_mode = settings.get('parse_mode', 'member_list')
    keyword_filter = settings.get('keyword_filter', [])
    exclude_keywords = settings.get('exclude_keywords', [])
    
    limit_text = str(limit) if limit else "Без лимита"
    save_every_text = f"{save_every} польз." if save_every > 0 else "В конце"
    rotate_text = "✅" if rotate else "❌"
    proxy_text = "✅" if use_proxy else "❌"
    rotate_every_text = f"{rotate_every} польз." if rotate and rotate_every > 0 else "При ошибке"
    filter_admins_text = "✅" if filter_admins else "❌"
    filter_inactive_text = "✅" if filter_inactive else "❌"
    
    # Mode display
    mode_text = "По участникам" if parse_mode == 'member_list' else "По сообщениям"
    keywords_text = f"{len(keyword_filter)} слов" if keyword_filter else "Нет"
    exclude_text = f"{len(exclude_keywords)} слов" if exclude_keywords else "Нет"
    
    # Check source type to determine if mode selection should be shown
    source_type = settings.get('source_type', 'group')
    
    # Common buttons
    buttons = []
    
    # Only show mode selection button for groups (not for channels)
    if source_type == 'group':
        buttons.append([InlineKeyboardButton(f"📋 Режим: {mode_text}", callback_data="parse_mode_select")])
    
    if parse_mode == 'message_based':
        # Message-based mode specific buttons
        messages_limit = settings.get('messages_limit')
        messages_limit_text = str(messages_limit) if messages_limit else "Без лимита"
        delay_every_requests = settings.get('delay_every_requests', 1)
        rotate_every_requests = settings.get('rotate_every_requests', 0)
        save_every_users = settings.get('save_every_users', 0)
        save_every_users_text = f"{save_every_users} польз." if save_every_users > 0 else "В конце"
        rotate_every_requests_text = f"{rotate_every_requests} запр." if rotate and rotate_every_requests > 0 else "При ошибке"
        
        limit_label = "Лимит постов" if source_type == 'channel' else "Лимит сообщений"
        
        buttons.extend([
            [InlineKeyboardButton(f"🔢 {limit_label}: {messages_limit_text}", callback_data="parse_msg_limit")],
            [InlineKeyboardButton(f"⏱️ Задержка: {delay} сек каждые {delay_every_requests} запр.", callback_data="parse_msg_delay")],
            [InlineKeyboardButton(f"⏱️ Задержка каждые: {delay_every_requests} запр.", callback_data="parse_msg_delay_every")],
            [InlineKeyboardButton(f"💾 Сохранять каждые: {save_every_users_text}", callback_data="parse_msg_save_every")],
            [InlineKeyboardButton(f"🔄 Ротация сессий: {rotate_text}", callback_data="parse_settings_rotate")],
            [InlineKeyboardButton(f"🔄 Ротация каждые: {rotate_every_requests_text}", callback_data="parse_msg_rotate_every")],
            [InlineKeyboardButton(f"🌐 Использовать прокси: {proxy_text}", callback_data="parse_settings_proxy")],
        ])

        # Filter options only for groups (not for channels)
        if source_type != 'channel':
            buttons.extend([
                [InlineKeyboardButton(f"🚫 Исключить админов: {filter_admins_text}", callback_data="parse_filter_admins")],
                [InlineKeyboardButton(f"🛌 Исключить неактивных: {filter_inactive_text}", callback_data="parse_filter_inactive")],
                [InlineKeyboardButton(f"📅 Неактивен более: {inactive_days} дн.", callback_data="parse_inactive_days")],
            ])

        buttons.extend([
            [InlineKeyboardButton(f"🔑 Ключевые слова: {keywords_text}", callback_data="parse_keyword_filter")],
            [InlineKeyboardButton(f"🚫 Исключить слова: {exclude_text}", callback_data="parse_exclude_keywords")],
        ])
    else:
        # Member list mode buttons (original)
        buttons.extend([
            [InlineKeyboardButton(f"🔢 Лимит: {limit_text}", callback_data="parse_settings_limit")],
            [InlineKeyboardButton(f"⏱️ Задержка: {delay} сек", callback_data="parse_settings_delay")],
            [InlineKeyboardButton(f"💾 Сохранять каждые: {save_every_text}", callback_data="parse_settings_save_every")],
            [InlineKeyboardButton(f"🔄 Ротация сессий: {rotate_text}", callback_data="parse_settings_rotate")],
            [InlineKeyboardButton(f"🔄 Ротация каждые: {rotate_every_text}", callback_data="parse_settings_rotate_every")],
            [InlineKeyboardButton(f"🌐 Использовать прокси: {proxy_text}", callback_data="parse_settings_proxy")],
            [InlineKeyboardButton(f"🚫 Исключить админов: {filter_admins_text}", callback_data="parse_filter_admins")],
            [InlineKeyboardButton(f"🛌 Исключить неактивных: {filter_inactive_text}", callback_data="parse_filter_inactive")],
            [InlineKeyboardButton(f"📅 Неактивен более: {inactive_days} дн.", callback_data="parse_inactive_days")],
        ])
    
    buttons.extend([
        [InlineKeyboardButton("🔐 Выбор сессий", callback_data="parse_settings_sessions")],
        [InlineKeyboardButton("🚀 Начать парсинг", callback_data="parse_start")],
        [InlineKeyboardButton("🔙 Назад", callback_data="parse_back")]
    ])
    
    return InlineKeyboardMarkup(buttons)




async def get_user_files_keyboard() -> InlineKeyboardMarkup:
    """Keyboard with list of user files."""
    from shared.user_files_manager import UserFilesManager
    
    manager = UserFilesManager()
    files = manager.list_user_files()
    
    buttons = []
    for file_info in files[:10]:  # Show max 10 files
        name = file_info['name']
        count = file_info['count']
        btn_text = f"📄 {name} ({count} юзеров)"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"select_file:{name}")])
    
    if not buttons:
        buttons.append([InlineKeyboardButton("❌ Нет файлов", callback_data="no_files")])
    
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="files_back")])
    return InlineKeyboardMarkup(buttons)


# ============== File Manager Keyboards ==============

FILES_PER_PAGE = 8  # Number of files per page in file manager

# File index mapping for callback_data (to avoid 64 byte limit)
# Maps short index -> filename
_file_index_map: Dict[str, str] = {}
_file_reverse_map: Dict[str, str] = {}  # filename -> index


def _generate_file_index(filename: str) -> str:
    """Generate or get existing short index for filename."""
    if filename in _file_reverse_map:
        return _file_reverse_map[filename]
    
    # Generate new index
    idx = len(_file_index_map)
    short_id = f"f{idx}"
    _file_index_map[short_id] = filename
    _file_reverse_map[filename] = short_id
    return short_id


def get_filename_by_index(index: str) -> Optional[str]:
    """Get filename by its short index."""
    return _file_index_map.get(index)


def _clear_file_index():
    """Clear file index mapping (call on refresh)."""
    global _file_index_map, _file_reverse_map
    _file_index_map = {}
    _file_reverse_map = {}


def truncate_callback_data(data: str, max_len: int = 64) -> str:
    """Truncate callback_data to fit Telegram's limit."""
    if len(data.encode('utf-8')) <= max_len:
        return data
    # Truncate by characters until it fits
    while len(data.encode('utf-8')) > max_len:
        data = data[:-1]
    return data

async def get_file_manager_list_keyboard(page: int = 0) -> InlineKeyboardMarkup:
    """
    Keyboard with list of user files for file manager with pagination.
    
    Args:
        page: Current page number (0-indexed)
    """
    from shared.user_files_manager import UserFilesManager
    
    # Clear index on first page (refresh)
    if page == 0:
        _clear_file_index()
    
    manager = UserFilesManager()
    files = manager.list_user_files()
    
    total_files = len(files)
    total_pages = max(1, (total_files + FILES_PER_PAGE - 1) // FILES_PER_PAGE)
    
    # Ensure page is within bounds
    page = max(0, min(page, total_pages - 1))
    
    # Get files for current page
    start_idx = page * FILES_PER_PAGE
    end_idx = start_idx + FILES_PER_PAGE
    page_files = files[start_idx:end_idx]
    
    buttons = []
    for file_info in page_files:
        name = file_info['name']
        count = file_info['count']
        # Use short index for callback_data to avoid 64 byte limit
        file_idx = _generate_file_index(name)
        # Truncate display name for button text
        display_name = name[:20] + "..." if len(name) > 20 else name
        btn_text = f"📄 {display_name} ({count})"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"fm_s:{file_idx}")])
    
    if not buttons:
        buttons.append([InlineKeyboardButton("❌ Нет файлов", callback_data="fm_no_files")])
    
    # Pagination buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"fm_page:{page - 1}"))
    
    if total_pages > 1:
        nav_buttons.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="fm_page_info"))
    
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"fm_page:{page + 1}"))
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    buttons.append([InlineKeyboardButton("🔄 Обновить", callback_data="fm_refresh")])
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="fm_back")])
    return InlineKeyboardMarkup(buttons)


def get_file_actions_keyboard(filename: str) -> InlineKeyboardMarkup:
    """Keyboard with actions for selected file (uses short index)."""
    # Get or create short index for this filename
    file_idx = _generate_file_index(filename)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика", callback_data=f"fm_st:{file_idx}")],
        [InlineKeyboardButton("📋 Копировать файл", callback_data=f"fm_cp:{file_idx}")],
        [InlineKeyboardButton("✏️ Переименовать", callback_data=f"fm_rn:{file_idx}")],
        [
            InlineKeyboardButton("🗑️ Удалить", callback_data=f"fm_del:{file_idx}"),
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"fm_dc:{file_idx}")
        ],
        [InlineKeyboardButton("🔧 Фильтрация пользователей", callback_data=f"fm_fl:{file_idx}")],
        [InlineKeyboardButton("🔙 К списку файлов", callback_data="fm_list")]
    ])


def get_file_filter_keyboard(filename: str) -> InlineKeyboardMarkup:
    """Keyboard with filter options for file (uses short index)."""
    file_idx = _generate_file_index(filename)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏷️ Оставить только с username", callback_data=f"fm_fa:{file_idx}:ku")],
        [InlineKeyboardButton("❌ Удалить без username", callback_data=f"fm_fa:{file_idx}:nu")],
        [InlineKeyboardButton("❌ Удалить без first_name", callback_data=f"fm_fa:{file_idx}:nf")],
        [InlineKeyboardButton("🔄 Удалить дубликаты", callback_data=f"fm_fa:{file_idx}:rd")],
        [InlineKeyboardButton("🔍 Удалить по ключевому слову", callback_data=f"fm_fk:{file_idx}:r")],
        [InlineKeyboardButton("✅ Оставить по ключевому слову", callback_data=f"fm_fk:{file_idx}:k")],
        [InlineKeyboardButton("🔙 Назад к файлу", callback_data=f"fm_s:{file_idx}")]
    ])


def format_file_stats(stats: Dict) -> str:
    """Format file statistics for display."""
    if not stats:
        return "❌ Не удалось получить статистику"
    
    # Format file size
    size_bytes = stats.get('size_bytes', 0)
    if size_bytes > 1024 * 1024:
        size_str = f"{size_bytes / (1024*1024):.2f} MB"
    elif size_bytes > 1024:
        size_str = f"{size_bytes / 1024:.2f} KB"
    else:
        size_str = f"{size_bytes} bytes"
    
    # Format metadata
    metadata = stats.get('metadata', {})
    source_group = metadata.get('source_group_title', 'Не указано')
    
    text = f"""📊 **Статистика файла: {stats.get('name')}**

📁 **Общая информация:**
• Размер: {size_str}
• Создан: {stats.get('created_at', 'Неизвестно')[:19] if stats.get('created_at') else 'Неизвестно'}
• Обновлен: {stats.get('updated_at', 'Неизвестно')[:19] if stats.get('updated_at') else 'Неизвестно'}

👥 **Пользователи:**
• Всего: **{stats.get('total_users', 0)}**
• С username: {stats.get('with_username', 0)} ({stats.get('without_username', 0)} без)
• С именем: {stats.get('with_first_name', 0)}
• С фамилией: {stats.get('with_last_name', 0)}
• Уникальных ID: {stats.get('unique_ids', 0)}
• Дубликатов: {stats.get('duplicates', 0)}

📤 **Источник:**
• Группа: {source_group}
"""
    return text
