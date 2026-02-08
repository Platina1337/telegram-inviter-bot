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

# Post Forwarding states (post parse and post monitoring)
FSM_POST_FORWARD_SOURCE_TYPE = "post_forward_source_type"  # channel or group
FSM_POST_FORWARD_SOURCE = "post_forward_source"  # enter source channel/group
FSM_POST_FORWARD_TARGET_TYPE = "post_forward_target_type"  # channel or group
FSM_POST_FORWARD_TARGET = "post_forward_target"  # enter target channel/group
FSM_POST_FORWARD_SESSION_SELECT = "post_forward_session_select"
FSM_POST_FORWARD_MODE_SELECT = "post_forward_mode_select"  # parse or monitoring
FSM_POST_FORWARD_SETTINGS = "post_forward_settings"
FSM_POST_FORWARD_SETTINGS_LIMIT = "post_forward_settings_limit"
FSM_POST_FORWARD_SETTINGS_DELAY = "post_forward_settings_delay"
FSM_POST_FORWARD_SETTINGS_DELAY_EVERY = "post_forward_settings_delay_every"
FSM_POST_FORWARD_SETTINGS_ROTATE_EVERY = "post_forward_settings_rotate_every"
FSM_POST_FORWARD_SETTINGS_NATIVE = "post_forward_settings_native"
FSM_POST_FORWARD_SETTINGS_KEYWORDS_WHITELIST = "post_forward_settings_keywords_whitelist"
FSM_POST_FORWARD_SETTINGS_KEYWORDS_BLACKLIST = "post_forward_settings_keywords_blacklist"
FSM_POST_FORWARD_SIGNATURE_LABEL_POST = "post_forward_signature_label_post"
FSM_POST_FORWARD_SIGNATURE_LABEL_SOURCE = "post_forward_signature_label_source"
FSM_POST_FORWARD_SIGNATURE_LABEL_AUTHOR = "post_forward_signature_label_author"
FSM_PP_EDIT_SESSION_SELECT = "pp_edit_session_select"  # выбор сессий при редактировании PP/PM из статуса

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
        [KeyboardButton("📨 Пересылка постов")],
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
        [InlineKeyboardButton("⚙️ Изменить настройки", callback_data=f"invite_settings_from_status:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"invite_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"invite_refresh:{task_id}")]
    ])


def get_invite_paused_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for paused invite task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Продолжить", callback_data=f"invite_resume:{task_id}")],
        [InlineKeyboardButton("⚙️ Изменить настройки", callback_data=f"invite_settings_from_status:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"invite_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"invite_refresh:{task_id}")]
    ])


def get_parse_running_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for running parse task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏸️ Приостановить", callback_data=f"parse_pause:{task_id}")],
        [InlineKeyboardButton("⚙️ Изменить настройки", callback_data=f"parse_settings_from_status:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"parse_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"parse_refresh:{task_id}")]
    ])


def get_parse_paused_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for paused parse task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Продолжить", callback_data=f"parse_resume:{task_id}")],
        [InlineKeyboardButton("⚙️ Изменить настройки", callback_data=f"parse_settings_from_status:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"parse_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"parse_refresh:{task_id}")]
    ])


def get_settings_keyboard(current_settings: Dict = None, edit_mode: bool = False) -> InlineKeyboardMarkup:
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
    
    buttons = [
        [InlineKeyboardButton(f"⏱️ Задержка: {delay} сек", callback_data="settings_delay")],
        [InlineKeyboardButton(f"🔢 Каждые {delay_every} инвайта", callback_data="settings_delay_every")],
        [InlineKeyboardButton(f"🔢 Лимит: {limit_text}", callback_data="settings_limit")],
        [InlineKeyboardButton(f"🔄 Ротация сессий: {rotate_text}", callback_data="settings_rotate")],
        [InlineKeyboardButton(f"🔄 Ротация каждые: {rotate_every} инв.", callback_data="settings_rotate_every")],
        [InlineKeyboardButton(f"🌐 Использовать прокси: {proxy_text}", callback_data="settings_proxy")],
        [InlineKeyboardButton(f"👥 Фильтр: {filter_mode_text}", callback_data="settings_filter_mode")],
        [InlineKeyboardButton(f"🛌 Неактивен >: {inactive_threshold_text}", callback_data="settings_inactive_threshold_days")],
        [InlineKeyboardButton("🔐 Выбор сессий", callback_data="settings_sessions")],
    ]
    
    if edit_mode:
        buttons.append([InlineKeyboardButton("💾 Сохранить", callback_data="invite_settings_save")])
        buttons.append([InlineKeyboardButton("❌ Отмена", callback_data="invite_settings_cancel")])
    else:
        buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="settings_back")])
    
    return InlineKeyboardMarkup(buttons)


async def get_session_select_keyboard(
    selected_aliases: List[str] = None,
    done_callback: str = "sessions_done",
    back_callback: str = "sessions_back",
) -> InlineKeyboardMarkup:
    """Keyboard for selecting sessions (invite/parse/create or PP/PM edit)."""
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
    
    buttons.append([InlineKeyboardButton("✅ Готово", callback_data=done_callback)])
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data=back_callback)])
    
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
        [InlineKeyboardButton("📥 Парсинг постов", callback_data=f"assign_task:post_parsing:{session_alias}")],
        [InlineKeyboardButton("❌ Убрать из парсинга постов", callback_data=f"remove_task:post_parsing:{session_alias}")],
        [InlineKeyboardButton("🔄 Мониторинг постов", callback_data=f"assign_task:post_monitoring:{session_alias}")],
        [InlineKeyboardButton("❌ Убрать из мониторинга постов", callback_data=f"remove_task:post_monitoring:{session_alias}")],
        [InlineKeyboardButton("🌐 Настроить прокси", callback_data=f"set_proxy:{session_alias}")],
        [InlineKeyboardButton("🧪 Проверить прокси", callback_data=f"test_proxy:{session_alias}")],
        [InlineKeyboardButton("🗑️ Удалить прокси", callback_data=f"remove_proxy:{session_alias}")],
        [InlineKeyboardButton("📋 Копировать прокси", callback_data=f"copy_proxy:{session_alias}")],
        [InlineKeyboardButton("🔙 Отмена", callback_data="cancel_session_action")]
    ])


# ============== Formatting Functions ==============

def format_invite_status(task_data: Dict) -> str:
    """Format invite task status message."""
    from datetime import datetime, timedelta
    
    status_icons = {
        'pending': '⏳',
        'running': '🚀',
        'paused': '⏸️',
        'completed': '✅',
        'failed': '❌'
    }
    
    status_names = {
        'pending': 'Ожидание',
        'running': 'Выполняется',
        'paused': 'Приостановлено',
        'completed': 'Завершено',
        'failed': 'Ошибка'
    }
    
    status = task_data.get('status', 'pending')
    icon = status_icons.get(status, '❓')
    status_text = status_names.get(status, status.capitalize())
    
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

    # Calculate time until next action
    time_until_next = ""
    last_action_time = task_data.get('last_action_time')
    delay_seconds = task_data.get('delay_seconds', 30)
    delay_every = task_data.get('delay_every', 1)
    
    if status == 'running' and last_action_time and invited > 0:
        try:
            last_action = datetime.fromisoformat(last_action_time)
            now = datetime.now()
            elapsed = (now - last_action).total_seconds()
            
            # Calculate when next delay will be applied
            # Delay is applied every delay_every invites
            invites_since_last_delay = invited % delay_every
            
            if invites_since_last_delay == 0:
                # Just had a delay, show remaining time
                remaining = max(0, delay_seconds - elapsed)
                if remaining > 0:
                    time_until_next = f"\n⏱️ След. действие через: {int(remaining)} сек"
                else:
                    time_until_next = f"\n⏱️ Готов к действию"
            else:
                # No delay applied yet, show small delay or ready
                # Small delay is 2-5 seconds between invites
                small_delay = 5  # max small delay
                remaining = max(0, small_delay - elapsed)
                if remaining > 0:
                    time_until_next = f"\n⏱️ След. действие через: {int(remaining)} сек"
                else:
                    time_until_next = f"\n⏱️ Готов к действию"
        except:
            pass
    
    # Эффективная активная сессия: если текущая не в списке выбранных (настройки сменили) — показываем первую из списка
    effective_session = task_data.get('session') or task_data.get('current_session') or 'N/A'
    if available_sessions and effective_session not in available_sessions:
        effective_session = available_sessions[0]
    # Доп. строка «Активная сессия» только при ротации
    current_session_info = ""
    if task_data.get('rotate_sessions') and task_data.get('current_session'):
        current_session_info = f"\n🔐 Активная сессия: {task_data['current_session']}"

    text = f"""
{icon} **Статус инвайтинга**

📤 Источник: {source_display}
📥 Цель: {task_data.get('target_group', 'N/A')}

👥 Приглашено: {invited}{limit_text}
⏱️ Задержка: ~{task_data.get('delay_seconds', 30)} сек (каждые {task_data.get('delay_every', 1)} инв.){time_until_next}
🔐 Сессия: {effective_session}
📋 Сессии: {sessions_text}{current_session_info}
🔄 Ротация: {rotate_info}
🌐 Прокси: {proxy_info}
👥 Фильтр: {filter_mode_text}
🛌 Неактивен >: {inactive_threshold_text}

📋 Статус: {status_text}
"""
    
    if task_data.get('error_message'):
        text += f"\n⚠️ Ошибка: {task_data['error_message']}"
    
    return text.strip()


def format_parse_status(task_data: Dict) -> str:
    """Format parse task status message."""
    from datetime import datetime
    
    status = task_data.get('status', 'unknown')
    status_icons = {
        'pending': '⏳',
        'running': '🚀',
        'paused': '⏸️',
        'completed': '✅',
        'failed': '❌'
    }
    status_names = {
        'pending': 'Ожидание',
        'running': 'Выполняется',
        'paused': 'Приостановлено',
        'completed': 'Завершено',
        'failed': 'Ошибка'
    }
    icon = status_icons.get(status, '❓')
    status_text = status_names.get(status, status.capitalize())
    
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
    
    # Calculate time until next action
    time_until_next = ""
    last_action_time = task_data.get('last_action_time')
    delay_seconds = task_data.get('delay_seconds', 2)
    delay_every = task_data.get('delay_every', 1)
    
    if status == 'running' and last_action_time and parsed > 0:
        try:
            last_action = datetime.fromisoformat(last_action_time)
            now = datetime.now()
            elapsed = (now - last_action).total_seconds()
            
            # Calculate when next delay will be applied
            parses_since_last_delay = parsed % delay_every
            
            if parses_since_last_delay == 0:
                # Just had a delay, show remaining time
                remaining = max(0, delay_seconds - elapsed)
                if remaining > 0:
                    time_until_next = f" (через {int(remaining)} сек)"
                else:
                    time_until_next = " (готов)"
            else:
                # No delay applied yet, small delay between requests
                small_delay = 2  # typical small delay
                remaining = max(0, small_delay - elapsed)
                if remaining > 0:
                    time_until_next = f" (через {int(remaining)} сек)"
                else:
                    time_until_next = " (готов)"
        except:
            pass
    
    # Эффективная сессия: если текущая не в списке выбранных — показываем первую из списка
    current_session_display = task_data.get('session') or task_data.get('current_session') or 'N/A'
    if available_sessions and current_session_display not in available_sessions:
        current_session_display = available_sessions[0]
    if task_data.get('rotate_sessions') and task_data.get('current_session'):
        current_session_display = f"{current_session_display} ⚡"
        
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
⏱️ Задержка: {task_data.get('delay_seconds', 2)} сек каждые {delay_every_requests} запр.{time_until_next}
🔐 Текущая сессия: {current_session_display}
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
⏱️ Задержка: {task_data.get('delay_seconds', 2)} сек{time_until_next}
🔐 Текущая сессия: {current_session_display}
📋 Все сессии: {sessions_text}
🔄 Ротация: {rotate_info}
🌐 Прокси: {proxy_info}
🚫 Исключать: {filter_text}
"""
    
    text += f"\n📋 Статус: {status_text}"
    
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
        text += f"- 👥 Инвайтинг: {', '.join(inviting_sessions)}\n"
    else:
        text += "- 👥 Инвайтинг: не назначено\n"
    
    parsing_sessions = assignments.get('parsing', [])
    if parsing_sessions:
        text += f"- 🔍 Парсинг: {', '.join(parsing_sessions)}\n"
    else:
        text += "- 🔍 Парсинг: не назначено\n"
    
    post_parsing_sessions = assignments.get('post_parsing', [])
    if post_parsing_sessions:
        text += f"- 📥 Парсинг постов: {', '.join(post_parsing_sessions)}\n"
    else:
        text += "- 📥 Парсинг постов: не назначено\n"
    
    post_monitoring_sessions = assignments.get('post_monitoring', [])
    if post_monitoring_sessions:
        text += f"- 🔄 Мониторинг постов: {', '.join(post_monitoring_sessions)}\n"
    else:
        text += "- 🔄 Мониторинг постов: не назначено\n"

    return text


# ============== Parsing to File Keyboards ==============

def get_parse_settings_keyboard(current_settings: Dict = None, edit_mode: bool = False) -> InlineKeyboardMarkup:
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
    inactive_days = settings.get('inactive_threshold_days', 30)
    
    # New message-based mode settings
    parse_mode = settings.get('parse_mode', 'member_list')
    keyword_filter = settings.get('keyword_filter', [])
    exclude_keywords = settings.get('exclude_keywords', [])
    
    limit_text = str(limit) if limit else "Без лимита"
    save_every_text = f"{save_every} польз." if save_every > 0 else "В конце"
    rotate_text = "✅" if rotate else "❌"
    proxy_text = "✅" if use_proxy else "❌"
    rotate_every_text = f"{rotate_every} польz." if rotate and rotate_every > 0 else "При ошибке"
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
    
    # Only show mode selection button for groups (not for channels) and not in edit mode
    if source_type == 'group' and not edit_mode:
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
    
    buttons.append([InlineKeyboardButton("🔐 Выбор сессий", callback_data="parse_settings_sessions")])
    
    if edit_mode:
        buttons.append([InlineKeyboardButton("💾 Сохранить", callback_data="parse_settings_save")])
        buttons.append([InlineKeyboardButton("❌ Отмена", callback_data="parse_settings_cancel")])
    else:
        buttons.append([InlineKeyboardButton("🚀 Начать парсинг", callback_data="parse_start")])
        buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="parse_settings_back")])
    
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


# ============== Post Forwarding Keyboards ==============

def get_post_forward_main_keyboard() -> InlineKeyboardMarkup:
    """Main keyboard for post forwarding feature."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Парсинг постов", callback_data="post_parse_start")],
        [InlineKeyboardButton("🔄 Мониторинг постов", callback_data="post_monitor_start")],
        [InlineKeyboardButton("📋 Мои задачи", callback_data="post_forward_tasks")],
        [InlineKeyboardButton("🔙 Назад", callback_data="post_forward_back")]
    ])


def get_post_forward_source_type_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for selecting source type (channel or group)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Канал", callback_data="pf_source_type:channel")],
        [InlineKeyboardButton("👥 Группа", callback_data="pf_source_type:group")],
        [InlineKeyboardButton("🔙 Назад", callback_data="pf_back")]
    ])


def get_post_forward_target_type_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for selecting target type (channel or group)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Канал", callback_data="pf_target_type:channel")],
        [InlineKeyboardButton("👥 Группа", callback_data="pf_target_type:group")],
        [InlineKeyboardButton("🔙 Назад", callback_data="pf_back")]
    ])


def get_post_forward_mode_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for selecting forwarding mode (parse historic or monitor live)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Парсинг (исторические посты)", callback_data="pf_mode:parse")],
        [InlineKeyboardButton("🔄 Мониторинг (в реальном времени)", callback_data="pf_mode:monitor")],
        [InlineKeyboardButton("🔙 Назад", callback_data="pf_back")]
    ])


def get_post_forward_settings_message_text(
    mode: str,
    source: Dict,
    target: Dict,
    settings: Dict,
    sessions_count: Optional[int] = None,
) -> str:
    """Текст сообщения настроек пересылки с отображением «С источником»/«Без источника» по режиму."""
    mode_name = "Парсинг постов" if mode == 'parse' else "Мониторинг постов"
    mode_icon = "📥" if mode == 'parse' else "🔄"
    use_native = settings.get('use_native_forward', False)
    display_line = "👀 Отображение: С источником" if use_native else "👀 Отображение: Без источника"
    lines = [
        f"{mode_icon} **{mode_name}**\n",
        "✅ **Настройки пересылки**\n",
        f"📤 Источник: {source.get('title', 'N/A')}\n",
        f"📥 Цель: {target.get('title', 'N/A')}\n",
    ]
    if sessions_count is not None:
        lines.append(f"🔐 Сессий выбрано: {sessions_count}\n")
    if settings.get('add_signature'):
        lines.append(f"✍️ Подпись: Включена\n")
    lines.append(f"\n{display_line}\n\n")
    lines.append("Настройте параметры или нажмите 🚀 Запустить:")
    return "".join(lines)


def get_post_forward_settings_keyboard(current_settings: Dict = None, mode: str = "parse", edit_mode: bool = False, task_id: int = None) -> InlineKeyboardMarkup:
    """Settings keyboard for post forwarding task."""
    settings = current_settings or {}
    
    limit = settings.get('limit')
    delay = settings.get('delay_seconds', 2 if mode == 'parse' else 0)
    delay_every = settings.get('delay_every', 1)
    rotate = settings.get('rotate_sessions', False)
    rotate_every = settings.get('rotate_every', 0)
    use_proxy = settings.get('use_proxy', True)
    
    # Native settings
    use_native_forward = settings.get('use_native_forward', False)
    check_content_if_native = settings.get('check_content_if_native', True)
    forward_show_source = settings.get('forward_show_source', True)
    
    # Determine contact action mode
    skip_on_contacts = settings.get('skip_on_contacts', False)
    remove_contacts = settings.get('remove_contacts', False)
    
    if skip_on_contacts:
        contact_action_text = "🚫 Пропускать"
    elif remove_contacts:
        contact_action_text = "✏️ Редактировать"
    else:
        contact_action_text = "➖ Игнорировать"

    if use_native_forward:
        if skip_on_contacts:
            contact_action_text = "🚫 Пропускать"
        else:
            contact_action_text = "➖ Игнорировать"
        # Media filter not applicable in native mode
        media_text = "🔒 Все (Нативная)"
    else:
        # Media filter applicable for both parse and monitoring in copy mode
        media_filter = settings.get('media_filter', 'all')
        media_text = {"all": "Все", "media_only": "Только медиа", "text_only": "Только текст"}.get(media_filter, "Все")

    
    limit_text = str(limit) if limit else "Без лимита"
    rotate_text = "✅" if rotate else "❌"
    proxy_text = "✅" if use_proxy else "❌"
    rotate_every_text = f"{rotate_every} пост." if rotate and rotate_every > 0 else "При ошибке"
    signature_text = "✅" if settings.get('add_signature') else "❌"

    # Keywords info
    whitelist = settings.get('keywords_whitelist', [])
    blacklist = settings.get('keywords_blacklist', [])
    whitelist_text = f"{len(whitelist)} слов" if whitelist else "Нет"
    blacklist_text = f"{len(blacklist)} слов" if blacklist else "Нет"
    
    buttons = [
        [InlineKeyboardButton(f"🔢 Лимит постов: {limit_text}", callback_data="pf_settings_limit")],
    ]
    
    # Delay only for parse mode
    if mode == "parse":
        buttons.append([InlineKeyboardButton(f"⏱️ Задержка: {delay} сек", callback_data="pf_settings_delay")])
        buttons.append([InlineKeyboardButton(f"🔢 Каждые {delay_every} пост.", callback_data="pf_settings_delay_every")])
        
        parse_direction = settings.get('parse_direction', 'backward')
        direction_text = "⬅️ Старые первыми" if parse_direction == 'backward' else "➡️ Новые первыми"
        buttons.append([InlineKeyboardButton(f"📋 Направление: {direction_text}", callback_data="pf_settings_direction")])
        
        
    buttons.append([InlineKeyboardButton(f"🎬 Фильтр: {media_text}", callback_data="pf_settings_media_filter")])
    
    buttons.extend([
        [InlineKeyboardButton(f"✅ Включая слова: {whitelist_text}", callback_data="pf_settings_whitelist")],
        [InlineKeyboardButton(f"🚫 Исключая слова: {blacklist_text}", callback_data="pf_settings_blacklist")],
        [InlineKeyboardButton(f"🔄 Ротация сессий: {rotate_text}", callback_data="pf_settings_rotate")],
        [InlineKeyboardButton(f"🔄 Ротация каждые: {rotate_every_text}", callback_data="pf_settings_rotate_every")],
        [InlineKeyboardButton(f"🌐 Использовать прокси: {proxy_text}", callback_data="pf_settings_proxy")],
    ])

    # Show signature option only if native is NOT enabled
    if not use_native_forward:
        buttons.append([InlineKeyboardButton(f"✍️ Добавлять подпись: {signature_text}", callback_data="pf_settings_signature")])
        if settings.get('add_signature'):
            buttons.append([InlineKeyboardButton("✏️ Настроить подпись", callback_data="pf_signature_menu")])

    # Native & content settings
    native_text = "✅ Вкл" if use_native_forward else "❌ Выкл"
    buttons.append([InlineKeyboardButton(f"⚡ Нативная пересылка: {native_text}", callback_data="pf_native_toggle")])

    if use_native_forward:
        # Native ON: only "Проверять контент". Отображение с/без источника — только в тексте сообщения.
        check_text = "✅ Да" if check_content_if_native else "❌ Нет"
        buttons.append([InlineKeyboardButton(f"📝 Проверять контент: {check_text}", callback_data="pf_native_check")])

    buttons.append([InlineKeyboardButton(f"📞 При контактах: {contact_action_text}", callback_data="pf_settings_contact_action")])
    
    # Выбор сессий при редактировании задачи (PP/PM из статуса задач)
    if edit_mode and task_id is not None:
        sessions_callback = f"pp_settings_sessions:{task_id}" if mode == "parse" else f"pm_settings_sessions:{task_id}"
        buttons.append([InlineKeyboardButton("🔐 Выбор сессий", callback_data=sessions_callback)])
    
    # Bottom buttons depend on edit_mode
    if edit_mode:
        # In edit mode: Save (back to details), Restart (reset progress & start), Cancel
        if mode == "parse":
            buttons.append([InlineKeyboardButton("💾 Сохранить", callback_data=f"pp_settings_save:{task_id}")])
            buttons.append([InlineKeyboardButton("🔄 Запустить заново", callback_data=f"pp_settings_restart:{task_id}")])
            buttons.append([InlineKeyboardButton("❌ Отмена", callback_data=f"pp_settings_cancel:{task_id}")])
        else:  # monitor mode
            buttons.append([InlineKeyboardButton("💾 Сохранить", callback_data=f"pm_settings_save:{task_id}")])
            buttons.append([InlineKeyboardButton("🔄 Запустить заново", callback_data=f"pm_settings_restart:{task_id}")])
            buttons.append([InlineKeyboardButton("❌ Отмена", callback_data=f"pm_settings_cancel:{task_id}")])
    else:
        # In create mode: Start and Back buttons
        buttons.append([InlineKeyboardButton("🚀 Запустить", callback_data="pf_start_task")])
        buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="pf_settings_back")])
    
    return InlineKeyboardMarkup(buttons)


def get_default_signature_options() -> Dict:
    """Default options when enabling signature."""
    return {
        'include_post': True,
        'include_source': False,
        'include_author': True,
        'label_post': 'Ссылка на пост',
        'label_source': 'Источник',
        'label_author': 'Обращаться по объявлению сюда:'
    }


def get_signature_options_keyboard(settings: Dict) -> InlineKeyboardMarkup:
    """Keyboard for signature options sub-menu."""
    opts = settings.get('signature_options') or get_default_signature_options()
    inc_post = opts.get('include_post', True)
    inc_src = opts.get('include_source', False)
    inc_author = opts.get('include_author', True)
    label_post = (opts.get('label_post') or opts.get('label_source') or 'Ссылка на пост')[:25]
    label_src = (opts.get('label_source') or 'Источник')[:25]
    label_author = (opts.get('label_author') or 'Обращаться...')[:25]
    buttons = [
        [InlineKeyboardButton(f"📎 Ссылка на пост: {'✅' if inc_post else '❌'}", callback_data="pf_sig_include_post")],
        [InlineKeyboardButton(f"📂 Ссылка на источник (канал): {'✅' if inc_src else '❌'}", callback_data="pf_sig_include_source")],
        [InlineKeyboardButton(f"👤 Ссылка на автора: {'✅' if inc_author else '❌'}", callback_data="pf_sig_include_author")],
        [InlineKeyboardButton(f"🏷 Текст для поста: «{label_post}»", callback_data="pf_sig_label_post")],
        [InlineKeyboardButton(f"🏷 Текст для источника: «{label_src}»", callback_data="pf_sig_label_source")],
        [InlineKeyboardButton(f"🏷 Текст для автора: «{label_author}»", callback_data="pf_sig_label_author")],
        [InlineKeyboardButton("✅ Готово", callback_data="pf_sig_done")]
    ]
    return InlineKeyboardMarkup(buttons)


def get_signature_options_message_text(settings: Dict) -> str:
    """Message text for signature options sub-menu."""
    return (
        "✏️ **Настройка подписи**\n\n"
        "Выберите, что добавлять в конец поста:\n"
        "• **Ссылка на пост** — прямая ссылка на сообщение\n"
        "• **Ссылка на источник** — ссылка на канал/группу\n"
        "• **Ссылка на автора** — ссылка на отправителя\n\n"
        "Для каждого типа ссылки можно задать свой текст (по умолчанию: «Ссылка на пост», «Источник», «Обращаться по объявлению сюда:»).\n\n"
        "Нажмите **Готово**, чтобы вернуться к настройкам."
    )


async def get_post_forward_session_keyboard(selected_aliases: List[str] = None, sessions: List[Dict] = None) -> InlineKeyboardMarkup:
    """Keyboard for selecting sessions for post forwarding task.
    
    Args:
        selected_aliases: List of already selected session aliases
        sessions: Optional list of sessions (to avoid API call if already fetched)
    """
    selected = selected_aliases or []
    
    if sessions is None:
        result = await api_client.list_sessions()
        sessions = result.get('sessions', [])
    
    if not sessions:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("⚠️ Нет доступных сессий", callback_data="pf_no_sessions")],
            [InlineKeyboardButton("🔙 Назад", callback_data="pf_sessions_back")]
        ])
    
    buttons = []
    for session in sessions:
        alias = session.get('alias', '')
        phone = session.get('phone', '')
        is_active = session.get('is_active', False)
        is_selected = alias in selected
        
        # Show status indicator based on is_active field
        status_icon = "🟢" if is_active else "🔴"
        
        prefix = "✅" if is_selected else "⬜"
        btn_text = f"{prefix} {status_icon} {alias} ({phone})"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"pf_toggle_session:{alias}")])
    
    # Show selected count
    count_text = f"Выбрано: {len(selected)}" if selected else "Выберите хотя бы одну сессию"
    buttons.append([InlineKeyboardButton(f"📊 {count_text}", callback_data="pf_sessions_info")])
    
    if selected:
        buttons.append([InlineKeyboardButton("✅ Готово", callback_data="pf_sessions_done")])
    
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="pf_sessions_back")])
    
    return InlineKeyboardMarkup(buttons)


def format_session_error_message(error: str, session_alias: str = None) -> str:
    """Format user-friendly error message for session issues."""
    error_lower = error.lower() if error else ""
    
    if "клиент недоступен" in error_lower or "client unavailable" in error_lower:
        session_info = f" ({session_alias})" if session_alias else ""
        return (
            f"❌ **Сессия недоступна{session_info}**\n\n"
            "Возможные причины:\n"
            "• Сессия отключена или заблокирована\n"
            "• Проблемы с авторизацией\n"
            "• Сессия требует повторного входа\n\n"
            "Проверьте сессию в меню 🔐 **Сессии**"
        )
    
    if "peer" in error_lower or "not found" in error_lower:
        return (
            "❌ **Канал/группа не найден(а)**\n\n"
            "Возможные причины:\n"
            "• Неверная ссылка или ID\n"
            "• Канал/группа закрытый и сессия не является участником\n"
            "• Канал/группа был удалён\n\n"
            "Проверьте ссылку и попробуйте снова."
        )
    
    if "flood" in error_lower:
        return (
            "⏳ **Слишком много запросов (FloodWait)**\n\n"
            "Telegram ограничил частоту запросов для этой сессии.\n"
            "Подождите несколько минут и попробуйте снова."
        )
    
    if "banned" in error_lower or "blocked" in error_lower:
        session_info = f" ({session_alias})" if session_alias else ""
        return (
            f"🚫 **Сессия заблокирована{session_info}**\n\n"
            "Эта сессия была заблокирована Telegram.\n"
            "Используйте другую сессию или создайте новую."
        )
    
    # Default error message
    return f"❌ **Ошибка:** {error}"


def get_post_parse_running_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for running post parse task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏸️ Приостановить", callback_data=f"pp_pause:{task_id}")],
        [InlineKeyboardButton("⚙️ Изменить настройки", callback_data=f"pp_settings:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"pp_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"pp_refresh:{task_id}")]
    ])


def get_post_parse_paused_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for paused post parse task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Продолжить", callback_data=f"pp_resume:{task_id}")],
        [InlineKeyboardButton("⚙️ Изменить настройки", callback_data=f"pp_settings:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"pp_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"pp_refresh:{task_id}")]
    ])


def get_post_monitor_running_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for running post monitoring task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏸️ Приостановить", callback_data=f"pm_pause:{task_id}")],
        [InlineKeyboardButton("⚙️ Изменить настройки", callback_data=f"pm_settings:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"pm_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"pm_refresh:{task_id}")]
    ])


def get_post_monitor_paused_keyboard(task_id: int) -> InlineKeyboardMarkup:
    """Keyboard for paused post monitoring task."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Продолжить", callback_data=f"pm_resume:{task_id}")],
        [InlineKeyboardButton("⚙️ Изменить настройки", callback_data=f"pm_settings:{task_id}")],
        [InlineKeyboardButton("🗑️ Удалить задачу", callback_data=f"pm_delete:{task_id}")],
        [InlineKeyboardButton("🔄 Обновить статус", callback_data=f"pm_refresh:{task_id}")]
    ])


def format_post_parse_status(task_data: Dict) -> str:
    """Format post parse task status message."""
    status_icons = {
        'pending': '⏳',
        'running': '🚀',
        'paused': '⏸️',
        'completed': '✅',
        'failed': '❌'
    }
    status_names = {
        'pending': 'Ожидание',
        'running': 'Выполняется',
        'paused': 'Приостановлено',
        'completed': 'Завершено',
        'failed': 'Ошибка'
    }
    
    status = task_data.get('status', 'pending')
    icon = status_icons.get(status, '❓')
    status_text = status_names.get(status, status.capitalize())
    
    forwarded = task_data.get('forwarded_count', 0)
    limit = task_data.get('limit')
    limit_text = f"/{limit}" if limit else ""
    
    rotate = task_data.get('rotate_sessions', False)
    rotate_every = task_data.get('rotate_every', 0)
    rotate_info = 'Да' if rotate else 'Нет'
    if rotate and rotate_every > 0:
        rotate_info += f" (каждые {rotate_every} пост.)"
    
    proxy_info = 'Да' if task_data.get('use_proxy') else 'Нет'
    filter_contacts_info = 'Да' if task_data.get('filter_contacts', False) else 'Нет'
    remove_contacts_info = 'Да' if task_data.get(
        'remove_contacts') else 'Нет'
    add_signature_info = 'Да' if task_data.get('add_signature') else 'Нет'

    direction = task_data.get('parse_direction', 'backward')
    direction_text = "Старые первыми" if direction == 'backward' else "Новые первыми"
    
    media_filter = task_data.get('media_filter', 'all')
    media_text = {"all": "Все", "media_only": "Только медиа", "text_only": "Только текст"}.get(media_filter, "Все")
    
    available_sessions = task_data.get('available_sessions', [])
    sessions_text = ', '.join(available_sessions) if available_sessions else task_data.get('session', 'N/A')
    effective_session = task_data.get('session') or task_data.get('current_session') or 'N/A'
    if available_sessions and effective_session not in available_sessions:
        effective_session = available_sessions[0]
    
    text = f"""
{icon} **Статус парсинга постов**

📤 Источник: {task_data.get('source_title', 'N/A')} ({task_data.get('source_type', 'channel')})
📥 Цель: {task_data.get('target_title', 'N/A')} ({task_data.get('target_type', 'channel')})

📨 Переслано: {forwarded}{limit_text}
📋 Направление: {direction_text}
🎬 Фильтр медиа: {media_text}
⏱️ Задержка: {task_data.get('delay_seconds', 2)} сек (каждые {task_data.get('delay_every', 1)} пост.)
🔐 Сессия: {effective_session}
📋 Сессии: {sessions_text}
🔄 Ротация: {rotate_info}
🌐 Прокси: {proxy_info}
📞 Фильтр контактов: {filter_contacts_info}
🗑️ Удалять контакты: {remove_contacts_info}
✍️ Добавлять подпись: {add_signature_info}

📋 Статус: {status_text}
"""
    
    if task_data.get('error_message'):
        text += f"\n⚠️ Ошибка: {task_data['error_message']}"
    
    return text.strip()


def format_post_monitor_status(task_data: Dict) -> str:
    """Format post monitoring task status message."""
    status_icons = {
        'pending': '⏳',
        'running': '🚀',
        'paused': '⏸️',
        'completed': '✅',
        'failed': '❌'
    }
    status_names = {
        'pending': 'Ожидание',
        'running': 'Выполняется',
        'paused': 'Приостановлено',
        'completed': 'Завершено',
        'failed': 'Ошибка'
    }
    
    status = task_data.get('status', 'pending')
    icon = status_icons.get(status, '❓')
    status_text = status_names.get(status, status.capitalize())
    
    forwarded = task_data.get('forwarded_count', 0)
    limit = task_data.get('limit')
    limit_text = f"/{limit}" if limit else " (без лимита)"
    
    rotate = task_data.get('rotate_sessions', False)
    rotate_every = task_data.get('rotate_every', 0)
    rotate_info = 'Да' if rotate else 'Нет'
    if rotate and rotate_every > 0:
        rotate_info += f" (каждые {rotate_every} пост.)"
    
    proxy_info = 'Да' if task_data.get('use_proxy') else 'Нет'
    filter_contacts_info = 'Да' if task_data.get('filter_contacts') else 'Нет'
    remove_contacts_info = 'Да' if task_data.get('remove_contacts') else 'Нет'
    add_signature_info = 'Да' if task_data.get('add_signature') else 'Нет'
    
    available_sessions = task_data.get('available_sessions', [])
    sessions_text = ', '.join(available_sessions) if available_sessions else task_data.get('session', 'N/A')
    effective_session = task_data.get('session') or task_data.get('current_session') or 'N/A'
    if available_sessions and effective_session not in available_sessions:
        effective_session = available_sessions[0]
    
    text = f"""
{icon} **Статус мониторинга постов**

📤 Источник: {task_data.get('source_title', 'N/A')} ({task_data.get('source_type', 'channel')})
📥 Цель: {task_data.get('target_title', 'N/A')} ({task_data.get('target_type', 'channel')})

📨 Переслано: {forwarded}{limit_text}
⏱️ Задержка: {task_data.get('delay_seconds', 0)} сек
🔐 Сессия: {effective_session}
📋 Сессии: {sessions_text}
🔄 Ротация: {rotate_info}
🌐 Прокси: {proxy_info}
📞 Фильтр контактов: {filter_contacts_info}
🗑️ Удалять контакты: {remove_contacts_info}
✍️ Добавлять подпись: {add_signature_info}

📋 Статус: {status_text}
"""
    
    if task_data.get('error_message'):
        text += f"\n⚠️ Ошибка: {task_data['error_message']}"
    
    return text.strip()
