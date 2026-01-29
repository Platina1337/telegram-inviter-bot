# -*- coding: utf-8 -*-
"""
Main main handlers for the inviter bot.
"""
import logging
from typing import Dict
from pyrogram import Client
from pyrogram.types import Message, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import MessageNotModified

from bot.api_client import api_client
from bot.states import (
    user_states,
    FSM_MAIN_MENU, FSM_INVITE_SOURCE_GROUP, FSM_INVITE_TARGET_GROUP,
    FSM_INVITE_SESSION_SELECT, FSM_INVITE_MENU, FSM_INVITE_SETTINGS,
    FSM_SETTINGS_DELAY, FSM_SETTINGS_DELAY_EVERY, FSM_SETTINGS_LIMIT,
    FSM_SETTINGS_ROTATE_EVERY, FSM_SESSION_PROXY, FSM_SETTINGS_FILTER_MODE, FSM_SETTINGS_INACTIVE_THRESHOLD_DAYS,
    FSM_PARSE_FILE_NAME, FSM_PARSE_SOURCE_TYPE, FSM_PARSE_SOURCE_GROUP, FSM_PARSE_SETTINGS, FSM_PARSE_INACTIVE_DAYS,
    FSM_PARSE_SETTINGS_LIMIT, FSM_PARSE_SETTINGS_DELAY, FSM_PARSE_SETTINGS_ROTATE_EVERY, FSM_PARSE_SETTINGS_SAVE_EVERY,
    FSM_PARSE_SESSION_SELECT, FSM_INVITE_FILE_SELECT, FSM_INVITE_FROM_FILE_TARGET,
    FSM_PARSE_KEYWORD_FILTER, FSM_PARSE_EXCLUDE_KEYWORDS,
    FSM_PARSE_MSG_LIMIT, FSM_PARSE_MSG_DELAY_EVERY, FSM_PARSE_MSG_ROTATE_EVERY, FSM_PARSE_MSG_SAVE_EVERY,
    FSM_FILE_MANAGER, FSM_FILE_MANAGER_ACTION, FSM_FILE_MANAGER_COPY_NAME, 
    FSM_FILE_MANAGER_RENAME, FSM_FILE_MANAGER_FILTER_KEYWORD,
    get_main_keyboard, get_group_history_keyboard, get_target_group_history_keyboard,
    get_parse_source_group_history_keyboard,
    get_invite_menu_keyboard, get_settings_keyboard, get_session_select_keyboard,
    get_invite_running_keyboard, get_invite_paused_keyboard,
    get_parse_running_keyboard, get_parse_paused_keyboard,
    get_parse_settings_keyboard, get_user_files_keyboard,
    get_file_manager_list_keyboard, get_file_actions_keyboard, get_file_filter_keyboard, format_file_stats,
    get_filename_by_index,
    parse_group_button, normalize_group_input, format_group_button,
    format_invite_status, format_parse_status
)

from bot.session_handlers import sessions_command
from bot.config import ADMIN_IDS

logger = logging.getLogger(__name__)



async def safe_answer_callback(callback_query, text: str = None, show_alert: bool = False):
    """Safely answer callback query ignoring potential errors."""
    try:
        await callback_query.answer(text, show_alert=show_alert)
    except Exception:
        # Ignore errors like QueryIdInvalid if user clicked too fast or timeout happened
        pass


async def safe_edit_message_reply_markup(callback_query, reply_markup):
    """Safely edit message reply markup, ignoring MessageNotModified errors."""
    try:
        await callback_query.edit_message_reply_markup(reply_markup=reply_markup)
    except Exception as e:
        # Ignore MessageNotModified error - means the markup is already correct
        if "MESSAGE_NOT_MODIFIED" not in str(e):
            raise


async def show_main_menu(client: Client, message: Message, text: str = None):
    """Show main menu."""
    user_id = message.from_user.id
    user_states[user_id] = {"state": FSM_MAIN_MENU}
    
    if text is None:
        text = "🏠 **Главное меню**\n\nВыберите действие:"
    
    await message.reply(text, reply_markup=get_main_keyboard())


async def start_command(client: Client, message: Message):
    """Handle /start command."""
    logger.info(f"[START] User {message.from_user.id} started bot")
    
    # Check if sessions exist, if not prompt to add
    try:
        response = await api_client.list_sessions()
        sessions = response.get("sessions", [])
        if not sessions and (not ADMIN_IDS or message.from_user.id in ADMIN_IDS):
            await message.reply(
                "⚠️ **Важное уведомление**\n\n"
                "Система не обнаружила активных сессий.\n"
                "Для работы бота необходимо добавить хотя бы одну Telegram сессию.\n\n"
                "Пожалуйста, перейдите в меню 🔐 **Сессии** и нажмите 'Добавить сессию'.",
                reply_markup=get_main_keyboard()
            )
            return
    except Exception as e:
        logger.debug(f"API check failed during start: {e}")  # Fail silently if API not up

    await show_main_menu(client, message, 
        "👋 Привет! Я бот для инвайтинга пользователей из одной группы в другую.\n\n"
        "Выберите действие из меню:"
    )


async def text_handler(client: Client, message: Message):
    """Handle all text messages."""
    user_id = message.from_user.id
    text = message.text.strip()
    
    if user_id not in user_states:
        user_states[user_id] = {}
    
    state = user_states[user_id].get('state')
    

    
    # Main menu
    if state == FSM_MAIN_MENU or state is None:
        if text == "👥 Инвайтинг":
            await start_invite_flow(client, message)
            return
        elif text == "🔍 Парсинг в файл":
            await start_parse_to_file_flow(client, message)
            return
        elif text == "📁 Менеджер файлов":
            await start_file_manager(client, message)
            return
        elif text == "📊 Статус задач":
            await show_tasks_status(client, message)
            return
        elif text == "🔐 Сессии":
            await sessions_command(client, message)
            return
        else:

            await show_main_menu(client, message)
            return

    
    # Invite source group selection
    if state == FSM_INVITE_SOURCE_GROUP:
        if text == "🔙 Назад":
            await show_main_menu(client, message)
            return
        
        await handle_source_group_input(client, message, text)
        return
    
    # Invite target group selection
    if state == FSM_INVITE_TARGET_GROUP:
        if text == "🔙 Назад":
            # Go back to source group selection
            await start_invite_flow(client, message)
            return
        
        await handle_target_group_input(client, message, text)
        return
    
    # Settings delay input
    if state == FSM_SETTINGS_DELAY:
        if text == "🔙 Назад":
            await show_invite_settings(client, message)
            return
        
        try:
            delay = int(text)
            if delay < 1:
                delay = 1
            if delay > 3600:
                delay = 3600
            
            user_states[user_id]['invite_settings']['delay_seconds'] = delay
            await message.reply(f"✅ Задержка установлена: {delay} сек")
            await show_invite_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число от 1 до 3600")
        return
    
    # Settings delay every input
    if state == FSM_SETTINGS_DELAY_EVERY:
        if text == "🔙 Назад":
            await show_invite_settings(client, message)
            return
        
        try:
            every = int(text)
            if every < 1:
                every = 1
            if every > 100:
                every = 100
            
            user_states[user_id]['invite_settings']['delay_every'] = every
            await message.reply(f"✅ Задержка будет каждые {every} инвайта")
            await show_invite_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число от 1 до 100")
        return
    
    # Settings limit input
    if state == FSM_SETTINGS_LIMIT:
        if text == "🔙 Назад" or text.lower() == "нет" or text == "0":
            user_states[user_id]['invite_settings']['limit'] = None
            await message.reply("✅ Лимит убран")
            await show_invite_settings(client, message)
            return
        
        try:
            limit = int(text)
            if limit < 1:
                limit = None
            
            user_states[user_id]['invite_settings']['limit'] = limit
            await message.reply(f"✅ Лимит установлен: {limit}")
            await show_invite_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число или 'нет' для снятия лимита")
        return
        
    # Settings rotate every input
    if state == FSM_SETTINGS_ROTATE_EVERY:
        if text == "🔙 Назад":
            await show_invite_settings(client, message)
            return
            
        try:
            val = int(text)
            if val < 0:
                val = 0
            
            user_states[user_id]['invite_settings']['rotate_every'] = val
            msg = f"✅ Ротация каждые {val} инвайтов" if val > 0 else "✅ Ротация только при ошибках"
            await message.reply(msg)
            await show_invite_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число (0 для ротации только при ошибках)")
            await message.reply("❌ Введите число (0 для ротации только при ошибках)")
        return
    
    # Settings inactive threshold days input
    if state == FSM_SETTINGS_INACTIVE_THRESHOLD_DAYS:
        if text == "🔙 Назад":
            await show_invite_settings(client, message)
            return
            
        if text.lower() in ['none', 'нет', 'no', '-', '0']:
            user_states[user_id]['invite_settings']['inactive_threshold_days'] = None
            await message.reply("✅ Фильтр неактивных пользователей выключен")
            await show_invite_settings(client, message)
            return
            
        try:
            days = int(text)
            if days < 1:
                days = 1
            
            user_states[user_id]['invite_settings']['inactive_threshold_days'] = days
            await message.reply(f"✅ Фильтр неактивных установлен: {days} дней")
            await show_invite_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число дней или 'нет' для отключения фильтра")
        return
    
    # Session proxy input
    if state == FSM_SESSION_PROXY:
        session_name = user_states[user_id].get('session_name')
        if not session_name:
            await sessions_command(client, message)
            return
            
        proxy_str = text.strip()
        if proxy_str.lower() in ['none', 'нет', 'no', '-', '0']:
            proxy_str = None
        elif proxy_str:
            from shared.validation import validate_proxy_string
            is_valid, clean_proxy, error_msg = validate_proxy_string(proxy_str)
            if not is_valid:
                await message.reply(f"❌ Ошибка валидации: {error_msg}")
                return
            proxy_str = clean_proxy
            
        try:
            response = await api_client.set_session_proxy(session_name, proxy_str if proxy_str else "none")
            if response.get("success"):
                status = "✅ Прокси установлен" if proxy_str else "🗑️ Прокси удален"
                await message.reply(f"{status} для сессии **{session_name}**!")
            else:
                await message.reply(f"❌ Ошибка: {response.get('error')}")
        except Exception as e:
            await message.reply(f"❌ Ошибка соединения: {e}")
            
        # Clean state and return to sessions
        user_states[user_id] = {}
        await sessions_command(client, message)
        return
    
    # ============== Parsing to File States ==============
    
    # Parse file name input
    if state == FSM_PARSE_FILE_NAME:
        if text == "🔙 Назад":
            await show_main_menu(client, message)
            return
        

        from shared.validation import sanitize_filename
        
        is_valid, clean_name, error_msg = sanitize_filename(text)
        
        if not is_valid:
            await message.reply(f"❌ Ошибка валидации: {error_msg}\nПопробуйте другое имя:")
            return
        
        # Save file name
        user_states[user_id]['parse_file_name'] = clean_name
        user_states[user_id]['state'] = FSM_PARSE_SOURCE_TYPE
        
        # Show source type selection
        from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Из группы", callback_data="parse_source:group")],
            [InlineKeyboardButton("📢 Из канала (комментарии)", callback_data="parse_source:channel")],
            [InlineKeyboardButton("🔙 Назад", callback_data="parse_source:back")]
        ])
        
        await message.reply(
            f"✅ Имя файла: **{text.strip()}**\n\n"
            "📍 Выберите откуда брать пользователей:",
            reply_markup=keyboard
        )
        return

    
    # Parse source group input
    if state == FSM_PARSE_SOURCE_GROUP:
        if text == "🔙 Назад":
            user_states[user_id]['state'] = FSM_PARSE_FILE_NAME
            await message.reply(
                "📝 Введите имя файла для сохранения пользователей:",
                reply_markup=ReplyKeyboardRemove()
            )
            return
        
        await handle_parse_source_group_input(client, message, text)
        return

    # Parse inactive days input
    if state == FSM_PARSE_INACTIVE_DAYS:
        if text == "🔙 Назад":
            await show_parse_settings(client, message)
            return

        try:
            days = int(text)
            if days < 0: days = 0
            user_states[user_id]['parse_settings']['inactive_days'] = days
            await message.reply(f"✅ Установлено: {days} дн. неактивности")
            await show_parse_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число (количество дней)")
        return
    
    # Parse settings limit input
    if state == FSM_PARSE_SETTINGS_LIMIT:
        if text == "🔙 Назад":
            await show_parse_settings(client, message)
            return
        
        try:
            if text.lower() in ['0', 'нет', 'без лимита']:
                user_states[user_id]['parse_settings']['limit'] = None
                await message.reply("✅ Лимит снят")
            else:
                limit = int(text)
                if limit < 1: limit = 1
                user_states[user_id]['parse_settings']['limit'] = limit
                await message.reply(f"✅ Лимит установлен: {limit}")
            await show_parse_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число или '0' для снятия лимита")
        return
    
    # Parse settings delay input
    if state == FSM_PARSE_SETTINGS_DELAY:
        if text == "🔙 Назад":
            await show_parse_settings(client, message)
            return
        
        try:
            delay = int(text)
            if delay < 1: delay = 1
            if delay > 60: delay = 60
            user_states[user_id]['parse_settings']['delay_seconds'] = delay
            await message.reply(f"✅ Задержка установлена: {delay} сек")
            await show_parse_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число от 1 до 60")
        return
    
    # Parse settings rotate every input
    if state == FSM_PARSE_SETTINGS_ROTATE_EVERY:
        if text == "🔙 Назад":
            await show_parse_settings(client, message)
            return
        
        try:
            rotate_every = int(text)
            if rotate_every < 0: rotate_every = 0
            user_states[user_id]['parse_settings']['rotate_every'] = rotate_every
            await message.reply(f"✅ Ротация каждые: {rotate_every} польз.")
            await show_parse_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число (0 = только при ошибках)")
        return
    
    # Parse settings save every input
    if state == FSM_PARSE_SETTINGS_SAVE_EVERY:
        if text == "🔙 Назад":
            await show_parse_settings(client, message)
            return
        
        try:
            save_every = int(text)
            if save_every < 0: save_every = 0
            user_states[user_id]['parse_settings']['save_every'] = save_every
            if save_every > 0:
                await message.reply(f"✅ Файл будет сохраняться каждые: {save_every} польз.")
            else:
                await message.reply("✅ Файл будет сохранен только в конце задачи")
            await show_parse_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число (0 = только в конце)")
        return
    
    # Parse keyword filter input
    if state == FSM_PARSE_KEYWORD_FILTER:
        if text == "🔙 Назад":
            await show_parse_settings(client, message)
            return
        
        if text.strip() == '0':
            # Clear filter
            user_states[user_id]['parse_settings']['keyword_filter'] = []
            await message.reply("✅ Фильтр по ключевым словам очищен")
        else:
            # Parse keywords from comma-separated input
            keywords = [k.strip() for k in text.split(',') if k.strip()]
            user_states[user_id]['parse_settings']['keyword_filter'] = keywords
            await message.reply(f"✅ Установлено {len(keywords)} ключевых слов:\n`{', '.join(keywords)}`")
        
        user_states[user_id]['state'] = FSM_PARSE_SETTINGS
        await show_parse_settings(client, message)
        return
    
    # Parse exclude keywords input
    if state == FSM_PARSE_EXCLUDE_KEYWORDS:
        if text == "🔙 Назад":
            await show_parse_settings(client, message)
            return
        
        if text.strip() == '0':
            # Clear filter
            user_states[user_id]['parse_settings']['exclude_keywords'] = []
            await message.reply("✅ Фильтр слов для исключения очищен")
        else:
            # Parse keywords from comma-separated input
            excludes = [k.strip() for k in text.split(',') if k.strip()]
            user_states[user_id]['parse_settings']['exclude_keywords'] = excludes
            await message.reply(f"✅ Установлено {len(excludes)} слов для исключения:\n`{', '.join(excludes)}`")
        
        user_states[user_id]['state'] = FSM_PARSE_SETTINGS
        await show_parse_settings(client, message)
        return
    
    # Message-based mode specific input handlers
    if state == FSM_PARSE_MSG_LIMIT:
        if text == "🔙 Назад":
            await show_parse_settings(client, message)
            return
        
        settings = user_states[user_id].get('parse_settings', {})
        source_type = settings.get('source_type', 'group')
        limit_label = "постов" if source_type == 'channel' else "сообщений"
        
        try:
            if text.lower() in ['0', 'нет', 'без лимита']:
                user_states[user_id]['parse_settings']['messages_limit'] = None
                await message.reply(f"✅ Лимит {limit_label} снят")
            else:
                limit = int(text)
                if limit < 1:
                    await message.reply("❌ Лимит должен быть больше 0")
                    return
                user_states[user_id]['parse_settings']['messages_limit'] = limit
                await message.reply(f"✅ Лимит {limit_label} установлен: {limit}")
            await show_parse_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число (или '0' для снятия лимита)")
        return
    
    if state == FSM_PARSE_MSG_DELAY_EVERY:
        if text == "🔙 Назад":
            await show_parse_settings(client, message)
            return
        
        try:
            delay_every = int(text)
            if delay_every < 1:
                await message.reply("❌ Значение должно быть больше 0")
                return
            user_states[user_id]['parse_settings']['delay_every_requests'] = delay_every
            await message.reply(f"✅ Задержка будет каждые {delay_every} запросов")
            await show_parse_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число (минимум 1)")
        return
    
    if state == FSM_PARSE_MSG_ROTATE_EVERY:
        if text == "🔙 Назад":
            await show_parse_settings(client, message)
            return
        
        try:
            rotate_every = int(text)
            if rotate_every < 0:
                await message.reply("❌ Значение не может быть отрицательным")
                return
            user_states[user_id]['parse_settings']['rotate_every_requests'] = rotate_every
            if rotate_every > 0:
                await message.reply(f"✅ Ротация сессий каждые {rotate_every} запросов")
            else:
                await message.reply("✅ Ротация только при ошибках")
            await show_parse_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число (0 = только при ошибках)")
        return
    
    if state == FSM_PARSE_MSG_SAVE_EVERY:
        if text == "🔙 Назад":
            await show_parse_settings(client, message)
            return
        
        try:
            save_every = int(text)
            if save_every < 0:
                await message.reply("❌ Значение не может быть отрицательным")
                return
            user_states[user_id]['parse_settings']['save_every_users'] = save_every
            if save_every > 0:
                await message.reply(f"✅ Файл будет сохраняться каждые {save_every} найденных пользователей")
            else:
                await message.reply("✅ Файл будет сохранен только в конце")
            await show_parse_settings(client, message)
        except ValueError:
            await message.reply("❌ Введите число (0 = только в конце)")
        return
    
    # ============== Inviting from File States ==============

    
    # Invite from file target group input
    if state == FSM_INVITE_FROM_FILE_TARGET:
        if text == "🔙 Назад":
            await start_invite_from_file_flow(client, message)
            return
        
        await handle_invite_from_file_target_input(client, message, text)
        return
    
    # ============== File Manager States ==============
    
    # File manager - copy file name input
    if state == FSM_FILE_MANAGER_COPY_NAME:
        if text == "🔙 Назад" or text.lower() == "отмена":
            await start_file_manager(client, message)
            return
        
        await handle_file_manager_copy_name(client, message, text)
        return
    
    # File manager - rename file input
    if state == FSM_FILE_MANAGER_RENAME:
        if text == "🔙 Назад" or text.lower() == "отмена":
            await start_file_manager(client, message)
            return
        
        await handle_file_manager_rename(client, message, text)
        return
    
    # File manager - filter keyword input
    if state == FSM_FILE_MANAGER_FILTER_KEYWORD:
        if text == "🔙 Назад" or text.lower() == "отмена":
            await start_file_manager(client, message)
            return
        
        await handle_file_manager_filter_keyword(client, message, text)
        return
    
    # Default - show main menu
    await show_main_menu(client, message)


async def start_invite_flow(client: Client, message: Message):
    """Start the invite flow - ask for source group."""
    user_id = message.from_user.id
    
    # Check for sessions first
    result = await api_client.list_sessions()
    sessions = result.get('sessions', [])
    
    if not sessions:
        await message.reply(
            "⚠️ **Нет активных сессий!**\n\n"
            "Для запуска инвайтинга необходимо добавить хотя бы одну сессию.\n"
            "Перейдите в меню 🔐 **Сессии** -> **Добавить сессию**.",
            reply_markup=get_main_keyboard()
        )
        return

    # Initialize invite settings if not present
    if 'invite_settings' not in user_states.get(user_id, {}):
        user_states[user_id] = {'invite_settings': {
            'delay_seconds': 30,
            'delay_every': 1,
            'limit': None,
            'rotate_sessions': False,
            'rotate_every': 0,
            'use_proxy': True,
            'selected_sessions': [],
            'filter_mode': 'all',
            'inactive_threshold_days': None
        }}
    
    user_states[user_id]['state'] = FSM_INVITE_SOURCE_GROUP
    
    kb = await get_group_history_keyboard(user_id)
    
    await message.reply(
        "📤 **Выберите группу-источник**\n\n"
        "Введите ссылку на группу, username или выберите из истории:",
        reply_markup=kb or ReplyKeyboardRemove()
    )


async def handle_source_group_input(client: Client, message: Message, text: str):
    """Handle source group or file input."""
    user_id = message.from_user.id
    
    # Check if this is a file selection
    if text.startswith("📁 "):
        # Extract file name from button text: "📁 filename (N юзеров)"
        import re
        match = re.match(r"📁 (.+?) \((\d+) юзеров\)", text)
        if match:
            file_name = match.group(1)
            user_count = match.group(2)
            
            # Save file as source
            user_states[user_id]['source_file'] = file_name
            user_states[user_id]['source_group'] = None  # Clear group source
            
            # Move to target selection
            user_states[user_id]['state'] = FSM_INVITE_TARGET_GROUP
            
            kb = await get_target_group_history_keyboard(user_id)
            await message.reply(
                f"✅ Источник: 📁 **{file_name}** ({user_count} пользователей)\n\n"
                "📥 Теперь выберите целевую группу (куда добавлять):\n"
                "Введите ссылку на группу, username или выберите из истории:",
                reply_markup=kb or ReplyKeyboardRemove()
            )
            return
    
    # Try to parse as group button
    group_data = parse_group_button(text)
    
    if group_data:
        # From button
        group_id = group_data['id']
        group_title = group_data['title']
        username = group_data.get('username')
    else:
        # User input - need to resolve
        normalized = normalize_group_input(text)
        
        # Get first available session for resolving
        sessions_result = await api_client.list_sessions()
        sessions = sessions_result.get('sessions', [])
        assignments = sessions_result.get('assignments', {})
        
        # Prefer inviting sessions
        inviting_sessions = assignments.get('inviting', [])
        session_alias = inviting_sessions[0] if inviting_sessions else (
            sessions[0]['alias'] if sessions else None
        )
        
        if not session_alias:
            await message.reply(
                "❌ Нет доступных сессий для проверки группы.\n"
                "Добавьте сессию в меню 🔐 Сессии"
            )
            return
        
        # Resolve group
        group_info = await api_client.get_group_info(session_alias, normalized)
        
        if not group_info.get('success') or not group_info.get('id'):
            error_detail = group_info.get('error', 'Неизвестная ошибка')
            if 'Session not available' in str(error_detail):
                await message.reply(
                    f"❌ **Сессия `{session_alias}` недоступна**\\n\\n"
                    "Возможные причины:\\n"
                    "• Сессия не авторизована\\n"
                    "• Сессия заблокирована\\n"
                    "• Неверные API credentials\\n\\n"
                    "Назначьте рабочую сессию на инвайтинг в меню 🔐 **Сессии**\\n\\n"
                    "Отправьте /start чтобы вернуться в главное меню",
                    reply_markup=get_main_keyboard()
                )
                user_states[user_id] = {"state": FSM_MAIN_MENU}
            else:
                await message.reply(
                    "❌ Не удалось найти группу. Проверьте ссылку или ID.\\n"
                    "Попробуйте еще раз или отправьте /start для возврата в меню:"
                )
            return
        
        group_id = str(group_info['id'])
        group_title = group_info.get('title', f'Группа {group_id}')
        username = group_info.get('username')
        
        # Save to history
        await api_client.add_user_group(user_id, group_id, group_title, username)
    
    # Save source group
    user_states[user_id]['source_group'] = {
        'id': int(group_id),
        'title': group_title,
        'username': username
    }
    user_states[user_id]['source_file'] = None  # Clear file source
    
    # Update last used
    await api_client.update_user_group_last_used(user_id, group_id)
    
    # Move to target selection

    user_states[user_id]['state'] = FSM_INVITE_TARGET_GROUP
    
    kb = await get_target_group_history_keyboard(user_id)
    
    await message.reply(
        f"✅ Источник: **{group_title}**\n\n"
        "📥 Теперь выберите целевую группу (куда добавлять):\n"
        "Введите ссылку на группу, username или выберите из истории:",
        reply_markup=kb or ReplyKeyboardRemove()
    )


async def handle_target_group_input(client: Client, message: Message, text: str):
    """Handle target group input."""
    user_id = message.from_user.id
    
    # Try to parse as button
    group_data = parse_group_button(text)
    
    if group_data:
        group_id = group_data['id']
        group_title = group_data['title']
        username = group_data.get('username')
    else:
        normalized = normalize_group_input(text)
        
        # Get session for resolving
        sessions_result = await api_client.list_sessions()
        sessions = sessions_result.get('sessions', [])
        assignments = sessions_result.get('assignments', {})
        
        inviting_sessions = assignments.get('inviting', [])
        session_alias = inviting_sessions[0] if inviting_sessions else (
            sessions[0]['alias'] if sessions else None
        )
        
        if not session_alias:
            await message.reply(
                "❌ Нет доступных сессий для проверки группы.\\n"
                "Добавьте сессию в меню 🔐 Сессии",
                reply_markup=get_main_keyboard()
            )
            user_states[user_id] = {"state": FSM_MAIN_MENU}
            return
        
        group_info = await api_client.get_group_info(session_alias, normalized)
        
        if not group_info.get('success') or not group_info.get('id'):
            error_detail = group_info.get('error', 'Неизвестная ошибка')
            if 'Session not available' in str(error_detail):
                await message.reply(
                    f"❌ **Сессия `{session_alias}` недоступна**\\n\\n"
                    "Возможные причины:\\n"
                    "• Сессия не авторизована\\n"
                    "• Сессия заблокирована\\n"
                    "• Неверные API credentials\\n\\n"
                    "Назначьте рабочую сессию на инвайтинг в меню 🔐 **Сессии**\\n\\n"
                    "Отправьте /start чтобы вернуться в главное меню",
                    reply_markup=get_main_keyboard()
                )
                user_states[user_id] = {"state": FSM_MAIN_MENU}
            else:
                await message.reply(
                    "❌ Не удалось найти группу. Проверьте ссылку или ID.\\n"
                    "Попробуйте еще раз или отправьте /start для возврата в меню:"
                )
            return
        
        group_id = str(group_info['id'])
        group_title = group_info.get('title', f'Группа {group_id}')
        username = group_info.get('username')
        
        await api_client.add_user_target_group(user_id, group_id, group_title, username)
    
    # Save target group
    user_states[user_id]['target_group'] = {
        'id': int(group_id),
        'title': group_title,
        'username': username
    }
    
    await api_client.update_user_target_group_last_used(user_id, group_id)
    
    # Check if source is a file
    source_file = user_states[user_id].get('source_file')
    
    if source_file:
        # File-based inviting - skip mode selection, go directly to settings
        user_states[user_id]['invite_settings']['invite_mode'] = 'from_file'
        await show_invite_settings(client, message)
    else:
        # Group-based inviting - show mode selection
        source = user_states[user_id]['source_group']
        
        mode_buttons = [
            [InlineKeyboardButton("📋 По списку участников", callback_data="mode_member_list")],
            [InlineKeyboardButton("💬 По сообщениям в группе", callback_data="mode_message_based")],
            [InlineKeyboardButton("🔙 Назад", callback_data="mode_back")]
        ]
        
        await message.reply(
            f"✅ Источник: **{source['title']}**\n"
            f"✅ Цель: **{group_title}**\n\n"
            "🎯 **Выберите режим инвайтинга:**\n\n"
            "📋 **По списку участников** - классический режим, добавляет пользователей по списку участников группы-источника\n\n"
            "💬 **По сообщениям** - умный режим, проходит по истории сообщений в группе-источнике и добавляет их авторов в целевую группу. "
            "Полезно когда список участников скрыт или анонимен.",
            reply_markup=InlineKeyboardMarkup(mode_buttons)
        )



async def show_invite_settings(client: Client, message: Message):
    """Show invite settings menu."""
    user_id = message.from_user.id
    settings = user_states.get(user_id, {}).get('invite_settings', {})
    
    user_states[user_id]['state'] = FSM_INVITE_SETTINGS
    
    await message.reply(
        "⚙️ **Настройки инвайтинга**\n\n"
        "Выберите параметр для изменения:",
        reply_markup=get_settings_keyboard(settings)
    )


async def show_tasks_status(client: Client, message: Message, page: int = 0, edit_message: bool = False):
    """Show all tasks status with pagination."""
    user_id = message.from_user.id
    
    # Get invite tasks
    invite_result = await api_client.get_user_tasks(user_id)
    invite_tasks = invite_result.get('tasks', [])
    for t in invite_tasks: t['type'] = 'invite'
    
    # Get parse tasks
    parse_result = await api_client.get_user_parse_tasks(user_id)
    parse_tasks = parse_result.get('tasks', [])
    for t in parse_tasks: t['type'] = 'parse'
    
    # Merge tasks and sort by created_at (most recent first)
    tasks = invite_tasks + parse_tasks
    tasks.sort(key=lambda x: x.get('created_at', ''), reverse=True)
    
    if not tasks:
        empty_text = "📊 **Статус задач**\n\nУ вас нет активных задач."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="tasks_back")]])
        
        if edit_message:
            try:
                await message.edit_text(empty_text, reply_markup=kb)
            except Exception as e:
                logger.debug(f"Failed to edit message: {e}")
                await message.reply(empty_text, reply_markup=get_main_keyboard())
        else:
            await message.reply(empty_text, reply_markup=get_main_keyboard())
        return
    
    # Pagination settings
    tasks_per_page = 5
    total_pages = (len(tasks) + tasks_per_page - 1) // tasks_per_page
    page = max(0, min(page, total_pages - 1))  # Ensure page is in valid range
    
    start_idx = page * tasks_per_page
    end_idx = start_idx + tasks_per_page
    page_tasks = tasks[start_idx:end_idx]
    
    text = f"📊 **Статус задач** (Страница {page + 1}/{total_pages})\n\n"
    
    status_icons = {
        'pending': '⏳',
        'running': '🚀',
        'paused': '⏸️',
        'completed': '✅',
        'failed': '❌'
    }
    
    buttons = []
    
    for task in page_tasks:
        icon = status_icons.get(task['status'], '❓')
        is_parse = task.get('type') == 'parse'
        
        if is_parse:
            parsed = task.get('parsed_count', 0)
            limit = task.get('limit')
            limit_text = f"/{limit}" if limit else ""
            task_name = f"🔍 {task.get('source_group')} → 📁 {task.get('file_name')}"
            progress_text = f"   Спаршено: {parsed}{limit_text}"
        else:
            invited = task.get('invited_count', 0)
            limit = task.get('limit')
            limit_text = f"/{limit}" if limit else ""
            task_name = f"👥 {task.get('source_group')} → {task.get('target_group')}"
            progress_text = f"   Приглашено: {invited}{limit_text}"
        
        rotate_info = ""
        if task.get('rotate_sessions'):
            every = task.get('rotate_every', 0)
            rotate_info = f" | 🔄 Ротация: {'Да' if every == 0 else f'каждые {every}'}"
        
        task_text = f"{icon} **{task_name}**\n"
        task_text += f"{progress_text} | {task['status']}{rotate_info}"
        text += task_text + "\n\n"
        
        # Add action buttons for each task
        task_buttons = []
        prefix = "parse" if is_parse else "invite"
        
        if task['status'] == 'running':
            task_buttons.append(InlineKeyboardButton(
                f"⏹️ Остановить",
                callback_data=f"{prefix}_stop:{task['id']}"
            ))
        elif task['status'] == 'paused':
            task_buttons.append(InlineKeyboardButton(
                f"▶️ Продолжить",
                callback_data=f"{prefix}_resume:{task['id']}"
            ))
        
        # Add detail/refresh button
        task_buttons.append(InlineKeyboardButton(
            f"🔍 Детали",
            callback_data=f"{prefix}_status:{task['id']}"
        ))
        
        # Add delete button for completed/failed/pending tasks
        if task['status'] in ['completed', 'failed', 'pending']:
            task_buttons.append(InlineKeyboardButton(
                f"🗑️ Удалить",
                callback_data=f"{prefix}_delete:{task['id']}"
            ))
        
        if task_buttons:
            buttons.append(task_buttons)
    
    # Pagination buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"tasks_page:{page - 1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("Вперед ▶️", callback_data=f"tasks_page:{page + 1}"))
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    # Count completed/failed/pending tasks
    clearable_count = sum(1 for t in tasks if t['status'] in ['completed', 'failed', 'pending'])
    if clearable_count > 0:
        buttons.append([InlineKeyboardButton(
            f"🗑️ Очистить завершенные и ожидающие ({clearable_count})",
            callback_data="tasks_clear_completed"
        )])
    
    buttons.append([InlineKeyboardButton("🔄 Обновить", callback_data="tasks_refresh")])
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="tasks_back")])
    
    keyboard = InlineKeyboardMarkup(buttons)
    
    if edit_message:
        try:
            await message.edit_text(text, reply_markup=keyboard)
        except MessageNotModified:
            pass
        except Exception:
            # If edit fails, send new message
            await message.reply(text, reply_markup=keyboard)
    else:
        await message.reply(text, reply_markup=keyboard)


async def callback_handler(client: Client, callback_query):
    """Handle callback queries."""
    user_id = int(callback_query.from_user.id)
    data = callback_query.data
    
    logger.info(f"[CALLBACK] User {user_id}: {data}")
    
    # Initialize user state if needed
    if user_id not in user_states:
        user_states[user_id] = {}
    
    # ============== Invite Menu ==============
    
    if data.startswith("invite_start:"):
        await handle_invite_start(client, callback_query)
        return
    
    if data.startswith("invite_stop:"):
        await handle_invite_stop(client, callback_query)
        return
    
    if data.startswith("invite_pause:"):
        await handle_invite_stop(client, callback_query)  # Same as stop for now
        return
    
    if data.startswith("invite_resume:"):
        await handle_invite_resume(client, callback_query)
        return
    
    if data.startswith("invite_delete:"):
        await handle_invite_delete(client, callback_query)
        return
    
    if data.startswith("invite_refresh:"):
        await handle_invite_refresh(client, callback_query)
        return
    
    if data == "invite_settings":
        await handle_settings_menu(client, callback_query)
        return
    
    if data == "invite_status":
        # Create a mock message for show_tasks_status
        await show_tasks_status(client, callback_query.message)
        await callback_query.answer()
        return
    
    if data == "invite_back":
        user_states[user_id] = {"state": FSM_MAIN_MENU}
        await callback_query.message.reply("Выберите действие:", reply_markup=get_main_keyboard())
        await callback_query.answer()
        return
    
    # ============== Mode Selection ==============
    
    if data == "mode_member_list":
        # Set mode to member_list
        if 'invite_settings' not in user_states[user_id]:
            user_states[user_id]['invite_settings'] = {}
        user_states[user_id]['invite_settings']['invite_mode'] = 'member_list'
        
        # Show invite menu
        user_states[user_id]['state'] = FSM_INVITE_MENU
        
        source = user_states[user_id]['source_group']
        target = user_states[user_id]['target_group']
        settings = user_states[user_id].get('invite_settings', {})
        
        rotate_info = 'Да' if settings.get('rotate_sessions') else 'Нет'
        if settings.get('rotate_sessions') and settings.get('rotate_every', 0) > 0:
            rotate_info += f" (каждые {settings['rotate_every']} инв.)"

        text = f"""
✅ **Настройка инвайтинга**

📤 Источник: **{source['title']}**
📥 Цель: **{target['title']}**
🎯 Режим: **По списку участников**

⚙️ **Текущие настройки:**
⏱️ Задержка: ~{settings.get('delay_seconds', 30)} сек
🔢 Каждые {settings.get('delay_every', 1)} инвайта
🔢 Лимит: {settings.get('limit') or 'Без лимита'}
🔄 Ротация сессий: {rotate_info}

Выберите действие:
"""
        
        await callback_query.edit_message_text(
            text,
            reply_markup=get_invite_menu_keyboard()
        )
        await callback_query.answer("Режим: По списку участников")
        return
    
    if data == "mode_message_based":
        # Set mode to message_based
        if 'invite_settings' not in user_states[user_id]:
            user_states[user_id]['invite_settings'] = {}
        user_states[user_id]['invite_settings']['invite_mode'] = 'message_based'
        
        # Show invite menu
        user_states[user_id]['state'] = FSM_INVITE_MENU
        
        source = user_states[user_id]['source_group']
        target = user_states[user_id]['target_group']
        settings = user_states[user_id].get('invite_settings', {})
        
        rotate_info = 'Да' if settings.get('rotate_sessions') else 'Нет'
        if settings.get('rotate_sessions') and settings.get('rotate_every', 0) > 0:
            rotate_info += f" (каждые {settings['rotate_every']} инв.)"

        text = f"""
✅ **Настройка инвайтинга**

📤 Источник: **{source['title']}**
📥 Цель: **{target['title']}**
🎯 Режим: **По сообщениям в группе**

⚙️ **Текущие настройки:**
⏱️ Задержка: ~{settings.get('delay_seconds', 30)} сек
🔢 Каждые {settings.get('delay_every', 1)} инвайта
🔢 Лимит: {settings.get('limit') or 'Без лимита'}
🔄 Ротация сессий: {rotate_info}

Выберите действие:
"""
        
        await callback_query.edit_message_text(
            text,
            reply_markup=get_invite_menu_keyboard()
        )
        await callback_query.answer("Режим: По сообщениям")
        return
    
    if data == "mode_back":
        # Go back to target group selection
        user_states[user_id]['state'] = FSM_INVITE_TARGET_GROUP
        kb = await get_target_group_history_keyboard(user_id)
        
        source = user_states[user_id].get('source_group', {})
        
        await callback_query.message.reply(
            f"✅ Источник: **{source.get('title', 'N/A')}**\n\n"
            "📥 Теперь выберите целевую группу (куда добавлять):\n"
            "Введите ссылку на группу, username или выберите из истории:",
            reply_markup=kb or ReplyKeyboardRemove()
        )
        await callback_query.answer()
        return
    
    # ============== Settings ==============
    
    if data == "settings_delay":
        user_states[user_id]['state'] = FSM_SETTINGS_DELAY
        await callback_query.message.reply(
            "⏱️ Введите среднюю задержку между инвайтами (в секундах, от 1 до 3600):"
        )
        await callback_query.answer()
        return
    
    if data == "settings_delay_every":
        user_states[user_id]['state'] = FSM_SETTINGS_DELAY_EVERY
        await callback_query.message.reply(
            "🔢 Введите через сколько инвайтов делать задержку (например, 1 - после каждого, 4 - после каждых четырех):"
        )
        await callback_query.answer()
        return
    
    if data == "settings_limit":
        user_states[user_id]['state'] = FSM_SETTINGS_LIMIT
        await callback_query.message.reply(
            "🔢 Введите лимит приглашений (число) или 'нет' для снятия лимита:"
        )
        await callback_query.answer()
        return
    
    if data == "settings_rotate":
        settings = user_states[user_id].get('invite_settings', {})
        settings['rotate_sessions'] = not settings.get('rotate_sessions', False)
        user_states[user_id]['invite_settings'] = settings
        
        status = "включена" if settings['rotate_sessions'] else "выключена"
        await callback_query.answer(f"Ротация сессий {status}")
        
        await safe_edit_message_reply_markup(
            callback_query, 
            reply_markup=get_settings_keyboard(settings)
        )
        return
        
    if data == "settings_rotate_every":
        user_states[user_id]['state'] = FSM_SETTINGS_ROTATE_EVERY
        await callback_query.message.reply(
            "🔄 Введите число инвайтов, после которого нужно менять сессию (0 - менять только при ошибках):"
        )
        await callback_query.answer()
        return

    if data == "settings_proxy":
        settings = user_states[user_id].get('invite_settings', {})
        settings['use_proxy'] = not settings.get('use_proxy', True)
        user_states[user_id]['invite_settings'] = settings
        
        status = "включен" if settings['use_proxy'] else "выключен"
        await callback_query.answer(f"Режим прокси {status}")
        
        await safe_edit_message_reply_markup(
            callback_query, 
            reply_markup=get_settings_keyboard(settings)
        )
        return
    
    if data == "settings_filter_mode":
        await handle_filter_mode_selection(client, callback_query)
        return
    
    if data.startswith("filter_mode:"):
        await handle_filter_mode_change(client, callback_query)
        return
    
    if data == "settings_inactive_threshold_days":
        user_states[user_id]['state'] = FSM_SETTINGS_INACTIVE_THRESHOLD_DAYS
        await callback_query.message.reply(
            "🛌 Введите количество дней неактивности для фильтрации (число) или 'нет' для отключения фильтра:"
        )
        await callback_query.answer()
        return
    
    if data == "settings_sessions":
        await handle_session_selection(client, callback_query)
        return
    
    if data == "settings_back":
        user_states[user_id]['state'] = FSM_INVITE_MENU
        
        source = user_states[user_id].get('source_group') or {}
        target = user_states[user_id].get('target_group') or {}
        settings = user_states[user_id].get('invite_settings') or {}
        
        rotate_info = 'Да' if settings.get('rotate_sessions') else 'Нет'
        if settings.get('rotate_sessions') and settings.get('rotate_every', 0) > 0:
            rotate_info += f" (каждые {settings['rotate_every']} инв.)"

        if settings.get('invite_mode') == 'from_file':
            source_title = user_states[user_id].get('source_file') or "Файл"
            mode_text = "Из файла"
        else:
            source_title = source.get('title', 'N/A')
            mode_text = "По списку участников" if settings.get('invite_mode') != 'message_based' else "По сообщениям"

        text = f"""
✅ **Настройка инвайтинга**

📤 Источник: **{source_title}**
📥 Цель: **{target.get('title', 'N/A')}**
🎯 Режим: **{mode_text}**

⚙️ **Текущие настройки:**
⏱️ Задержка: ~{settings.get('delay_seconds', 30)} сек
🔢 Каждые {settings.get('delay_every', 1)} инвайта
🔢 Лимит: {settings.get('limit') or 'Без лимита'}
🔄 Ротация сессий: {rotate_info}
🌐 Прокси: {'✅' if settings.get('use_proxy', True) else '❌'}
"""
        
        await callback_query.edit_message_text(text, reply_markup=get_invite_menu_keyboard())
        return
    
    # ============== Session Selection ==============
    
    if data.startswith("toggle_session:"):
        await handle_toggle_session(client, callback_query)
        return
    
    if data == "sessions_done":
        await callback_query.answer("Сессии выбраны!")
        
        # Check if we're in parse or invite mode
        state = user_states.get(user_id, {}).get('state')
        
        if state == FSM_PARSE_SESSION_SELECT:
            settings = user_states[user_id].get('parse_settings', {})
            await callback_query.edit_message_text(
                "⚙️ **Настройки парсинга**\n\nВыберите параметр для изменения:",
                reply_markup=get_parse_settings_keyboard(settings)
            )
        else:
            settings = user_states[user_id].get('invite_settings', {})
            await callback_query.edit_message_text(
                "⚙️ **Настройки инвайтинга**\n\nВыберите параметр для изменения:",
                reply_markup=get_settings_keyboard(settings)
            )
        return
    
    if data == "sessions_back":
        # Check if we're in parse or invite mode
        state = user_states.get(user_id, {}).get('state')
        
        if state == FSM_PARSE_SESSION_SELECT:
            settings = user_states[user_id].get('parse_settings', {})
            await callback_query.edit_message_text(
                "⚙️ **Настройки парсинга**\n\nВыберите параметр для изменения:",
                reply_markup=get_parse_settings_keyboard(settings)
            )
        else:
            settings = user_states[user_id].get('invite_settings', {})
            await callback_query.edit_message_text(
                "⚙️ **Настройки инвайтинга**\n\nВыберите параметр для изменения:",
                reply_markup=get_settings_keyboard(settings)
            )
        await callback_query.answer()
        return
    
    # ============== Session Management ==============
    
    if data == "add_session":
        from bot.session_handlers import add_session_callback
        await add_session_callback(client, callback_query)
        return
    
    if data == "assign_session":
        from bot.session_handlers import assign_session_callback
        await assign_session_callback(client, callback_query)
        return
    
    if data == "delete_session":
        from bot.session_handlers import delete_session_callback
        await delete_session_callback(client, callback_query)
        return
    
    if data.startswith("select_session:"):
        from bot.session_handlers import select_session_callback
        await select_session_callback(client, callback_query)
        return
    
    if data.startswith("assign_task:"):
        from bot.session_handlers import assign_task_callback
        await assign_task_callback(client, callback_query)
        return
    
    if data.startswith("remove_task:"):
        from bot.session_handlers import remove_task_callback
        await remove_task_callback(client, callback_query)
        return
    
    if data.startswith("confirm_delete_session:"):
        from bot.session_handlers import confirm_delete_callback
        await confirm_delete_callback(client, callback_query)
        return
    
    if data.startswith("delete_confirmed:"):
        from bot.session_handlers import delete_confirmed_callback
    if data.startswith("delete_confirmed:"):
        from bot.session_handlers import delete_confirmed_callback
        await delete_confirmed_callback(client, callback_query)
        return
    
    if data.startswith("set_proxy:"):
        from bot.session_handlers import set_proxy_callback
        await set_proxy_callback(client, callback_query)
        return

    if data.startswith("test_proxy:"):
        from bot.session_handlers import test_proxy_callback
        await test_proxy_callback(client, callback_query)
        return

    if data.startswith("remove_proxy:"):
        from bot.session_handlers import remove_proxy_callback
        await remove_proxy_callback(client, callback_query)
        return

    if data.startswith("copy_proxy:"):
        from bot.session_handlers import copy_proxy_callback
        await copy_proxy_callback(client, callback_query)
        return

    if data.startswith("copy_proxy_confirm:"):
        from bot.session_handlers import copy_proxy_confirm_callback
        await copy_proxy_confirm_callback(client, callback_query)
        return

    if data == "cancel_session_action":
        from bot.session_handlers import cancel_session_action_callback
        await cancel_session_action_callback(client, callback_query)
        return
    
    if data == "sessions_menu_back":
        user_states[user_id] = {"state": FSM_MAIN_MENU}
        await callback_query.message.reply("Выберите действие:", reply_markup=get_main_keyboard())
        await callback_query.answer()
        return
    
    # ============== Tasks Status ==============
    
    if data.startswith("task_delete:"):
        await handle_task_delete(client, callback_query)
        return
    
    if data.startswith("tasks_page:"):
        page = int(data.split(":")[1])
        await show_tasks_status(client, callback_query.message, page=page, edit_message=True)
        await callback_query.answer()
        return
    
    if data == "tasks_clear_completed":
        await handle_clear_completed_tasks(client, callback_query)
        return
    
    if data == "tasks_refresh":
        await show_tasks_status(client, callback_query.message, page=0, edit_message=True)
        await callback_query.answer("Список обновлен")
        return
    
    if data == "tasks_back":
        user_states[user_id] = {"state": FSM_MAIN_MENU}
        await callback_query.message.reply("Выберите действие:", reply_markup=get_main_keyboard())
        await callback_query.answer()
        return

    # ============== Task Management Callbacks ==============
    
    if data.startswith("invite_status:"):
        await handle_invite_status(client, callback_query)
        return

    if data.startswith("invite_stop:"):
        await handle_invite_stop(client, callback_query)
        return
    
    if data.startswith("invite_resume:"):
        await handle_invite_resume(client, callback_query)
        return
    
    if data.startswith("invite_refresh:"):
        await handle_invite_refresh(client, callback_query)
        return

    if data.startswith("invite_delete:"):
        await handle_invite_delete(client, callback_query)
        return

    if data.startswith("parse_status:"):
        await handle_parse_status(client, callback_query)
        return

    if data.startswith("parse_stop:"):
        await handle_parse_stop(client, callback_query)
        return

    if data.startswith("parse_resume:"):
        await handle_parse_resume(client, callback_query)
        return

    if data.startswith("parse_refresh:"):
        await handle_parse_refresh(client, callback_query)
        return

    if data.startswith("parse_delete:"):
        await handle_parse_delete(client, callback_query)
        return
    
    # ============== Parsing to File Callbacks ==============
    
    if data == "parse_filter_admins":
        settings = user_states.get(user_id, {}).get('parse_settings', {})
        settings['filter_admins'] = not settings.get('filter_admins', False)
        user_states[user_id]['parse_settings'] = settings
        
        await safe_edit_message_reply_markup(
            callback_query, 
            reply_markup=get_parse_settings_keyboard(settings)
        )
        await safe_answer_callback(callback_query, f"Фильтр админов: {'Вкл' if settings['filter_admins'] else 'Выкл'}")
        return
    
    if data == "parse_filter_inactive":
        settings = user_states.get(user_id, {}).get('parse_settings', {})
        settings['filter_inactive'] = not settings.get('filter_inactive', False)
        user_states[user_id]['parse_settings'] = settings
        
        await safe_edit_message_reply_markup(
            callback_query, 
            reply_markup=get_parse_settings_keyboard(settings)
        )
        await safe_answer_callback(callback_query, f"Фильтр неактивных: {'Вкл' if settings['filter_inactive'] else 'Выкл'}")
        return
    
    if data == "parse_inactive_days":
        user_states[user_id]['state'] = FSM_PARSE_INACTIVE_DAYS
        await callback_query.message.reply(
            "📅 Введите количество дней неактивности (например: 30):"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_settings_limit":
        user_states[user_id]['state'] = FSM_PARSE_SETTINGS_LIMIT
        await callback_query.message.reply(
            "🔢 Введите лимит пользователей для парсинга\n"
            "(или '0' для снятия лимита):"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_settings_delay":
        user_states[user_id]['state'] = FSM_PARSE_SETTINGS_DELAY
        await callback_query.message.reply(
            "⏱️ Введите задержку между запросами в секундах (1-60):"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_settings_rotate":
        settings = user_states.get(user_id, {}).get('parse_settings', {})
        settings['rotate_sessions'] = not settings.get('rotate_sessions', False)
        user_states[user_id]['parse_settings'] = settings
        
        await safe_edit_message_reply_markup(
            callback_query, 
            reply_markup=get_parse_settings_keyboard(settings)
        )
        await safe_answer_callback(callback_query, f"Ротация сессий: {'Вкл' if settings['rotate_sessions'] else 'Выкл'}")
        return
    
    if data == "parse_settings_rotate_every":
        user_states[user_id]['state'] = FSM_PARSE_SETTINGS_ROTATE_EVERY
        await callback_query.message.reply(
            "🔄 Введите через сколько пользователей ротировать сессию\n"
            "(0 = только при ошибках):"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_settings_save_every":
        user_states[user_id]['state'] = FSM_PARSE_SETTINGS_SAVE_EVERY
        await callback_query.message.reply(
            "💾 Введите через сколько пользователей сохранять файл\n"
            "(0 = только в конце задачи):\n\n"
            "Например: 50 — файл будет обновляться каждые 50 спаршенных юзеров"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_settings_proxy":
        settings = user_states.get(user_id, {}).get('parse_settings', {})
        settings['use_proxy'] = not settings.get('use_proxy', True)
        user_states[user_id]['parse_settings'] = settings
        
        await safe_edit_message_reply_markup(
            callback_query, 
            reply_markup=get_parse_settings_keyboard(settings)
        )
        await safe_answer_callback(callback_query, f"Прокси: {'Вкл' if settings['use_proxy'] else 'Выкл'}")
        return
    
    # Message-based mode specific handlers
    if data == "parse_msg_limit":
        user_states[user_id]['state'] = FSM_PARSE_MSG_LIMIT
        await callback_query.message.reply(
            "🔢 Введите лимит сообщений для обработки\n"
            "(или '0' для снятия лимита):\n\n"
            "Например: 5000 — будет обработано не более 5000 последних сообщений"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_msg_delay":
        user_states[user_id]['state'] = FSM_PARSE_SETTINGS_DELAY
        await callback_query.message.reply(
            "⏱️ Введите задержку в секундах (1-60):"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_msg_delay_every":
        user_states[user_id]['state'] = FSM_PARSE_MSG_DELAY_EVERY
        await callback_query.message.reply(
            "⏱️ Введите через сколько API запросов делать задержку:\n\n"
            "Например: 2 — задержка будет после каждых 2 запросов к Telegram API"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_msg_rotate_every":
        user_states[user_id]['state'] = FSM_PARSE_MSG_ROTATE_EVERY
        await callback_query.message.reply(
            "🔄 Введите через сколько API запросов ротировать сессию\n"
            "(0 = только при ошибках):\n\n"
            "Например: 5 — сессия будет меняться каждые 5 запросов"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_msg_save_every":
        user_states[user_id]['state'] = FSM_PARSE_MSG_SAVE_EVERY
        await callback_query.message.reply(
            "💾 Введите через сколько найденных пользователей сохранять файл\n"
            "(0 = только в конце):\n\n"
            "Например: 50 — файл будет обновляться каждые 50 найденных пользователей"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_settings_sessions":
        settings = user_states.get(user_id, {}).get('parse_settings', {})
        selected = settings.get('selected_sessions', [])
        
        user_states[user_id]['state'] = FSM_PARSE_SESSION_SELECT
        
        kb = await get_session_select_keyboard(selected)
        await callback_query.message.reply(
            "🔐 **Выбор сессий для парсинга**\n\n"
            "Выберите сессии, которые будут использоваться для парсинга.\n"
            "Можно выбрать несколько сессий для ротации:",
            reply_markup=kb
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_start":
        await start_parsing_to_file(client, callback_query)
        return
    
    if data == "parse_mode_select":
        # Show mode selection menu
        settings = user_states.get(user_id, {}).get('parse_settings', {})
        current_mode = settings.get('parse_mode', 'member_list')
        
        buttons = [
            [InlineKeyboardButton(
                f"{'✅ ' if current_mode == 'member_list' else ''}👥 По участникам группы", 
                callback_data="parse_mode:member_list"
            )],
            [InlineKeyboardButton(
                f"{'✅ ' if current_mode == 'message_based' else ''}💬 По сообщениям в группе", 
                callback_data="parse_mode:message_based"
            )],
            [InlineKeyboardButton("🔙 Назад", callback_data="parse_mode_back")]
        ]
        
        await callback_query.edit_message_text(
            "📋 **Выбор режима парсинга**\n\n"
            "👥 **По участникам** - парсинг списка участников группы\n\n"
            "💬 **По сообщениям** - парсинг авторов сообщений.\n"
            "Позволяет фильтровать по ключевым словам в сообщениях.",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await safe_answer_callback(callback_query)
        return
    
    if data.startswith("parse_mode:"):
        mode = data.split(":")[1]
        settings = user_states.get(user_id, {}).get('parse_settings', {})
        settings['parse_mode'] = mode
        user_states[user_id]['parse_settings'] = settings
        
        # Return to settings menu
        await callback_query.edit_message_text(
            f"✅ Режим парсинга: **{'По участникам' if mode == 'member_list' else 'По сообщениям'}**\n\n"
            "Нажмите 'Назад' чтобы вернуться к настройкам.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 К настройкам", callback_data="parse_mode_back")]
            ])
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_mode_back":
        settings = user_states.get(user_id, {}).get('parse_settings', {})
        source = user_states.get(user_id, {}).get('parse_source_group', {})
        file_name = user_states.get(user_id, {}).get('parse_file_name', 'N/A')
        
        # Rebuild settings text
        parse_mode = settings.get('parse_mode', 'member_list')
        mode_text = "По участникам" if parse_mode == 'member_list' else "По сообщениям"
        
        lines = [
            "🔍 **Настройка парсинга**",
            "",
            f"📝 Файл: **{file_name}**",
            f"📤 Группа: **{source.get('title', 'N/A')}**",
            f"📋 Режим: **{mode_text}**",
            "",
            "Настройте параметры или начните парсинг:"
        ]
        text = "\n".join(lines)
        await callback_query.edit_message_text(text, reply_markup=get_parse_settings_keyboard(settings))
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_keyword_filter":
        user_states[user_id]['state'] = FSM_PARSE_KEYWORD_FILTER
        settings = user_states.get(user_id, {}).get('parse_settings', {})
        current_keywords = settings.get('keyword_filter', [])
        
        current_text = ""
        if current_keywords:
            current_text = f"\n\nТекущие ключевые слова:\n`{', '.join(current_keywords)}`"
        
        await callback_query.message.reply(
            f"🔑 **Настройка ключевых слов**\n\n"
            f"Введите ключевые слова через запятую.\n"
            f"Будут спаршены только пользователи, чьи сообщения содержат эти слова.\n\n"
            f"Пример: `работа, вакансия, заработок`\n\n"
            f"Отправьте `0` чтобы очистить фильтр.{current_text}"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_exclude_keywords":
        user_states[user_id]['state'] = FSM_PARSE_EXCLUDE_KEYWORDS
        settings = user_states.get(user_id, {}).get('parse_settings', {})
        current_excludes = settings.get('exclude_keywords', [])
        
        current_text = ""
        if current_excludes:
            current_text = f"\n\nТекущие слова для исключения:\n`{', '.join(current_excludes)}`"
        
        await callback_query.message.reply(
            f"🚫 **Настройка слов для исключения**\n\n"
            f"Введите слова через запятую.\n"
            f"Пользователи, написавшие сообщения с этими словами, будут исключены.\n\n"
            f"Пример: `бот, реклама, спам`\n\n"
            f"Отправьте `0` чтобы очистить фильтр.{current_text}"
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "parse_back":
        user_states[user_id] = {"state": FSM_MAIN_MENU}
        await callback_query.message.reply("Выберите действие:", reply_markup=get_main_keyboard())
        await safe_answer_callback(callback_query)
        return
    
    # ============== Parse Source Type Selection ==============
    
    if data.startswith("parse_source:"):
        source_type = data.split(":")[1]
        
        if source_type == "back":
            # Go back to file name input
            user_states[user_id]['state'] = FSM_PARSE_FILE_NAME
            await callback_query.message.reply(
                "🔍 **Парсинг пользователей в файл**\n\n"
                "📝 Введите имя файла для сохранения пользователей\n"
                "(например: 'юзы_из_чата1'):",
                reply_markup=ReplyKeyboardRemove()
            )
            await safe_answer_callback(callback_query)
            return
        
        # Save source type
        user_states[user_id]['parse_source_type'] = source_type
        user_states[user_id]['state'] = FSM_PARSE_SOURCE_GROUP
        
        # Get keyboard with group/channel history
        kb = await get_parse_source_group_history_keyboard(user_id)
        
        if source_type == "group":
            await callback_query.message.reply(
                "👥 **Парсинг из группы**\n\n"
                "📤 Выберите группу для парсинга:\n"
                "Введите ссылку на группу, username или выберите из истории:",
                reply_markup=kb or ReplyKeyboardRemove()
            )
        else:  # channel
            await callback_query.message.reply(
                "📢 **Парсинг из комментариев канала**\n\n"
                "📤 Выберите канал для парсинга:\n"
                "Введите ссылку на канал, username или выберите из истории:\n\n"
                "ℹ️ Пользователи будут собраны из комментариев под постами канала.",
                reply_markup=kb or ReplyKeyboardRemove()
            )
        
        await safe_answer_callback(callback_query)
        return
    
    # ============== File Selection Callbacks ==============
    
    if data.startswith("select_file:"):
        file_name = data.split(":", 1)[1]
        # Use standardized key for file selection
        user_states[user_id]['source_file'] = file_name
        user_states[user_id]['state'] = FSM_INVITE_FROM_FILE_TARGET
        
        kb = await get_target_group_history_keyboard(user_id)
        
        await callback_query.message.reply(
            f"✅ Файл: **{file_name}**\n\n"
            "📥 Теперь выберите целевую группу (куда добавлять):\n"
            "Введите ссылку на группу, username или выберите из истории:",
            reply_markup=kb or ReplyKeyboardRemove()
        )
        await safe_answer_callback(callback_query)
        return
    
    if data == "no_files":
        await callback_query.answer("Сначала создайте файлы через 'Парсинг в файл'", show_alert=True)
        return
    
    if data == "files_back":
        user_states[user_id] = {"state": FSM_MAIN_MENU}
        await callback_query.message.reply("Выберите действие:", reply_markup=get_main_keyboard())
        await callback_query.answer()
        return
    
    # ============== File Manager Callbacks ==============
    
    if data == "fm_back":
        user_states[user_id] = {"state": FSM_MAIN_MENU}
        await callback_query.message.reply("Выберите действие:", reply_markup=get_main_keyboard())
        await callback_query.answer()
        return
    
    if data == "fm_refresh" or data == "fm_list":
        # Reset to first page on refresh
        user_states[user_id]['fm_page'] = 0
        kb = await get_file_manager_list_keyboard(0)
        try:
            await callback_query.edit_message_text(
                "📁 **Менеджер файлов**\n\nВыберите файл для управления:",
                reply_markup=kb
            )
        except Exception as e:
            logger.debug(f"Failed to edit message for fm_refresh: {e}")
            await callback_query.message.reply(
                "📁 **Менеджер файлов**\n\nВыберите файл для управления:",
                reply_markup=kb
            )
        await callback_query.answer("Список обновлен")
        return
    
    if data.startswith("fm_page:"):
        page = int(data.split(":")[1])
        user_states[user_id]['fm_page'] = page
        kb = await get_file_manager_list_keyboard(page)
        try:
            await callback_query.edit_message_reply_markup(reply_markup=kb)
        except Exception as e:
            logger.debug(f"Failed to edit markup for fm_page: {e}")
            await callback_query.edit_message_text(
                "📁 **Менеджер файлов**\n\nВыберите файл для управления:",
                reply_markup=kb
            )
        await callback_query.answer()
        return
    
    if data == "fm_page_info":
        await callback_query.answer("Текущая страница")
        return
    
    if data == "fm_no_files":
        await callback_query.answer("Сначала создайте файлы через 'Парсинг в файл'", show_alert=True)
        return
    
    # fm_s:{index} - select file (short form)
    if data.startswith("fm_s:"):
        file_idx = data.split(":", 1)[1]
        filename = get_filename_by_index(file_idx)
        if not filename:
            await callback_query.answer("❌ Файл не найден, обновите список", show_alert=True)
            return
        user_states[user_id]['fm_selected_file'] = filename
        user_states[user_id]['state'] = FSM_FILE_MANAGER_ACTION
        
        # Get file stats for preview
        from shared.user_files_manager import UserFilesManager
        manager = UserFilesManager()
        stats = manager.get_file_stats(filename)
        
        if stats:
            count = stats.get('total_users', 0)
            with_username = stats.get('with_username', 0)
            preview = f"📄 **Файл: {filename}**\n\n👥 Пользователей: {count}\n🏷️ С username: {with_username}\n\nВыберите действие:"
        else:
            preview = f"📄 **Файл: {filename}**\n\nВыберите действие:"
        
        await callback_query.edit_message_text(
            preview,
            reply_markup=get_file_actions_keyboard(filename)
        )
        await callback_query.answer()
        return
    
    # fm_st:{index} - stats (short form)
    if data.startswith("fm_st:"):
        file_idx = data.split(":", 1)[1]
        filename = get_filename_by_index(file_idx)
        if not filename:
            await callback_query.answer("❌ Файл не найден", show_alert=True)
            return
        from shared.user_files_manager import UserFilesManager
        manager = UserFilesManager()
        stats = manager.get_file_stats(filename)
        
        text = format_file_stats(stats)
        
        await callback_query.edit_message_text(
            text,
            reply_markup=get_file_actions_keyboard(filename)
        )
        await callback_query.answer()
        return
    
    # fm_cp:{index} - copy (short form)
    if data.startswith("fm_cp:"):
        file_idx = data.split(":", 1)[1]
        filename = get_filename_by_index(file_idx)
        if not filename:
            await callback_query.answer("❌ Файл не найден", show_alert=True)
            return
        user_states[user_id]['fm_selected_file'] = filename
        user_states[user_id]['state'] = FSM_FILE_MANAGER_COPY_NAME
        
        await callback_query.message.reply(
            f"📋 **Копирование файла: {filename}**\n\n"
            "Введите имя для нового файла (копии):\n\n"
            "Отправьте `отмена` для отмены.",
            reply_markup=ReplyKeyboardRemove()
        )
        await callback_query.answer()
        return
    
    # fm_rn:{index} - rename (short form)
    if data.startswith("fm_rn:"):
        file_idx = data.split(":", 1)[1]
        filename = get_filename_by_index(file_idx)
        if not filename:
            await callback_query.answer("❌ Файл не найден", show_alert=True)
            return
        user_states[user_id]['fm_selected_file'] = filename
        user_states[user_id]['state'] = FSM_FILE_MANAGER_RENAME
        
        await callback_query.message.reply(
            f"✏️ **Переименование файла: {filename}**\n\n"
            "Введите новое имя файла:\n\n"
            "Отправьте `отмена` для отмены.",
            reply_markup=ReplyKeyboardRemove()
        )
        await callback_query.answer()
        return
    
    # fm_del:{index} - delete warning (short form)
    if data.startswith("fm_del:"):
        file_idx = data.split(":", 1)[1]
        filename = get_filename_by_index(file_idx)
        if not filename:
            await callback_query.answer("❌ Файл не найден", show_alert=True)
            return
        await callback_query.answer(
            f"⚠️ Для удаления файла '{filename}' нажмите 'Подтвердить'",
            show_alert=True
        )
        return
    
    # fm_dc:{index} - delete confirm (short form)
    if data.startswith("fm_dc:"):
        file_idx = data.split(":", 1)[1]
        filename = get_filename_by_index(file_idx)
        if not filename:
            await callback_query.answer("❌ Файл не найден", show_alert=True)
            return
        from shared.user_files_manager import UserFilesManager
        manager = UserFilesManager()
        
        if manager.delete_file(filename):
            await callback_query.answer("✅ Файл удален", show_alert=True)
            kb = await get_file_manager_list_keyboard()
            await callback_query.edit_message_text(
                "📁 **Менеджер файлов**\n\n✅ Файл удален.\n\nВыберите файл для управления:",
                reply_markup=kb
            )
        else:
            await callback_query.answer("❌ Ошибка при удалении файла", show_alert=True)
        return
    
    # fm_fl:{index} - filter menu (short form)
    if data.startswith("fm_fl:"):
        file_idx = data.split(":", 1)[1]
        filename = get_filename_by_index(file_idx)
        if not filename:
            await callback_query.answer("❌ Файл не найден", show_alert=True)
            return
        await callback_query.edit_message_text(
            f"🔧 **Фильтрация пользователей в файле: {filename}**\n\n"
            "Выберите действие для очистки списка:\n\n"
            "⚠️ Внимание: фильтрация изменит файл безвозвратно!",
            reply_markup=get_file_filter_keyboard(filename)
        )
        await callback_query.answer()
        return
    
    # fm_fa:{index}:{filter_code} - filter apply (short form)
    # Filter codes: ku=keep_with_username, nu=remove_no_username, nf=remove_no_first_name, rd=remove_duplicates
    if data.startswith("fm_fa:"):
        parts = data.split(":")
        file_idx = parts[1]
        filter_code = parts[2]
        
        filename = get_filename_by_index(file_idx)
        if not filename:
            await callback_query.answer("❌ Файл не найден", show_alert=True)
            return
        
        # Map short codes to full filter types
        filter_map = {
            'ku': 'keep_with_username',
            'nu': 'remove_no_username',
            'nf': 'remove_no_first_name',
            'rd': 'remove_duplicates'
        }
        filter_type = filter_map.get(filter_code, filter_code)
        
        from shared.user_files_manager import UserFilesManager
        manager = UserFilesManager()
        
        result = manager.filter_users_in_file(filename, filter_type)
        
        if result.get('success'):
            await callback_query.answer(
                f"✅ Удалено {result['removed_count']} пользователей\n"
                f"Было: {result['original_count']} → Стало: {result['new_count']}",
                show_alert=True
            )
            # Return to file actions
            stats = manager.get_file_stats(filename)
            if stats:
                count = stats.get('total_users', 0)
                preview = f"📄 **Файл: {filename}**\n\n👥 Пользователей: {count}\n\nВыберите действие:"
            else:
                preview = f"📄 **Файл: {filename}**\n\nВыберите действие:"
            
            await callback_query.edit_message_text(preview, reply_markup=get_file_actions_keyboard(filename))
        else:
            await callback_query.answer(f"❌ Ошибка: {result.get('error')}", show_alert=True)
        return
    
    # fm_fk:{index}:{mode} - filter by keyword (short form)
    # Modes: r=remove, k=keep
    if data.startswith("fm_fk:"):
        parts = data.split(":")
        file_idx = parts[1]
        mode_code = parts[2]
        
        filename = get_filename_by_index(file_idx)
        if not filename:
            await callback_query.answer("❌ Файл не найден", show_alert=True)
            return
        
        filter_mode = 'remove' if mode_code == 'r' else 'keep'
        
        user_states[user_id]['fm_selected_file'] = filename
        user_states[user_id]['fm_filter_mode'] = filter_mode
        user_states[user_id]['state'] = FSM_FILE_MANAGER_FILTER_KEYWORD
        
        action_text = "удалить пользователей" if filter_mode == 'remove' else "оставить только пользователей"
        
        await callback_query.message.reply(
            f"🔍 **Фильтрация по ключевому слову**\n\n"
            f"Файл: {filename}\n"
            f"Действие: {action_text}, чьи имена содержат указанное слово\n\n"
            "Введите ключевое слово:\n\n"
            "Отправьте `отмена` для отмены.",
            reply_markup=ReplyKeyboardRemove()
        )
        await callback_query.answer()
        return
    
    # Unknown callback
    await callback_query.answer("Неизвестная команда")


async def handle_invite_start(client: Client, callback_query):
    """Handle invite start."""
    user_id = int(callback_query.from_user.id)
    
    source = user_states.get(user_id, {}).get('source_group')
    target = user_states.get(user_id, {}).get('target_group')
    settings = user_states.get(user_id, {}).get('invite_settings', {})
    # Check both possible keys for file name (for compatibility)
    file_name = user_states.get(user_id, {}).get('source_file') or user_states.get(user_id, {}).get('selected_file_name')
    
    # Check if this is file-based inviting
    is_from_file = file_name is not None
    
    if is_from_file:
        # For file-based inviting, we only need target group
        if not target:
            await callback_query.answer("Сначала выберите целевую группу!", show_alert=True)
            return
        
        # Use dummy source for file-based mode
        source = {
            'id': -1,  # Special ID for file source
            'title': f'Файл: {file_name}',
            'username': None
        }
        invite_mode = 'from_file'
    else:
        # Normal group-based inviting
        if not source or not target:
            await callback_query.answer("Сначала выберите группы!", show_alert=True)
            return
        invite_mode = settings.get('invite_mode', 'member_list')

    
    # Get session to use
    sessions_result = await api_client.list_sessions()
    assignments = sessions_result.get('assignments', {})
    inviting_sessions = assignments.get('inviting', [])
    
    if not inviting_sessions:
        await callback_query.answer(
            "Нет сессий для инвайтинга! Назначьте сессию в меню 🔐 Сессии",
            show_alert=True
        )
        return
    
    session_alias = inviting_sessions[0]
    
    # Используем все назначенные сессии для инвайтинга, если не выбраны вручную
    # Если в настройках есть selected_sessions, используем их, иначе все назначенные
    available_sessions = settings.get('selected_sessions') or inviting_sessions
    
    # Create task
    result = await api_client.create_task(
        user_id=user_id,
        source_group_id=source['id'],
        source_group_title=source['title'],
        source_username=source.get('username'),
        target_group_id=target['id'],
        target_group_title=target['title'],
        target_username=target.get('username'),
        session_alias=session_alias,
        invite_mode=invite_mode,
        file_source=file_name if is_from_file else None,
        delay_seconds=settings.get('delay_seconds', 30),
        delay_every=settings.get('delay_every', 1),
        limit=settings.get('limit'),
        rotate_sessions=settings.get('rotate_sessions', False),
        rotate_every=settings.get('rotate_every', 0),
        use_proxy=settings.get('use_proxy', True),
        available_sessions=available_sessions,
        filter_mode=settings.get('filter_mode', 'all'),
        inactive_threshold_days=settings.get('inactive_threshold_days')
    )
    
    if not result.get('success'):
        await callback_query.answer(f"Ошибка: {result.get('error')}", show_alert=True)
        return
    
    task_id = result['task_id']
    user_states[user_id]['current_task_id'] = task_id
    
    # Start the task
    start_result = await api_client.start_task(task_id)
    
    if not start_result.get('success'):
        await callback_query.answer(f"Ошибка запуска: {start_result.get('error')}", show_alert=True)
        return
    
    # Show running status
    task_data = await api_client.get_task(task_id)
    text = format_invite_status(task_data)
    
    await callback_query.edit_message_text(
        text,
        reply_markup=get_invite_running_keyboard(task_id)
    )
    await callback_query.answer("Инвайтинг запущен!")



async def handle_invite_stop(client: Client, callback_query):
    """Handle invite stop."""
    task_id = int(callback_query.data.split(":")[1])
    
    result = await api_client.stop_task(task_id)
    
    if result.get('success'):
        task_data = await api_client.get_task(task_id)
        text = format_invite_status(task_data)
        
        await callback_query.edit_message_text(
            text,
            reply_markup=get_invite_paused_keyboard(task_id)
        )
        await callback_query.answer("Инвайтинг остановлен")
    else:
        await callback_query.answer(f"Ошибка: {result.get('error')}", show_alert=True)


async def handle_invite_resume(client: Client, callback_query):
    """Handle invite resume."""
    task_id = int(callback_query.data.split(":")[1])
    
    result = await api_client.start_task(task_id)
    
    if result.get('success'):
        task_data = await api_client.get_task(task_id)
        text = format_invite_status(task_data)
        
        await callback_query.edit_message_text(
            text,
            reply_markup=get_invite_running_keyboard(task_id)
        )
        await callback_query.answer("Инвайтинг продолжен")
    else:
        await callback_query.answer(f"Ошибка: {result.get('error')}", show_alert=True)


async def handle_invite_delete(client: Client, callback_query):
    """Handle invite delete."""
    task_id = int(callback_query.data.split(":")[1])
    
    result = await api_client.delete_task(task_id)
    
    if result.get('success'):
        await callback_query.answer("Задача удалена", show_alert=True)
        # Show tasks status again or go to main menu
        await show_tasks_status(client, callback_query.message, page=0)
        try:
            await callback_query.message.delete()
        except Exception as e:
            logger.debug(f"Failed to delete message after task delete: {e}")
    else:
        await callback_query.answer(f"Ошибка: {result.get('error')}", show_alert=True)


async def handle_parse_refresh(client: Client, callback_query):
    """Handle parse status refresh."""
    task_id = int(callback_query.data.split(":")[1])
    
    task_data = await api_client.get_parse_task(task_id)
    if task_data.get('success'):
        task = task_data.get('task', {})
        text = format_parse_status(task)
        
        kb = get_parse_running_keyboard(task_id) if task['status'] == 'running' else get_parse_paused_keyboard(task_id)
        
        try:
            await callback_query.edit_message_text(text, reply_markup=kb)
        except Exception as e:
            logger.debug(f"Failed to edit parse status message: {e}")
        await callback_query.answer("Статус обновлен")
    else:
        await callback_query.answer(f"Ошибка: {task_data.get('error')}", show_alert=True)


async def handle_parse_stop(client: Client, callback_query):
    """Handle parse stop."""
    task_id = int(callback_query.data.split(":")[1])
    
    result = await api_client.stop_parse_task(task_id)
    
    if result.get('success'):
        task_data = await api_client.get_parse_task(task_id)
        text = format_parse_status(task_data.get('task', {}))
        
        await callback_query.edit_message_text(
            text,
            reply_markup=get_parse_paused_keyboard(task_id)
        )
        await callback_query.answer("Парсинг остановлен")
    else:
        await callback_query.answer(f"Ошибка: {result.get('error')}", show_alert=True)


async def handle_parse_resume(client: Client, callback_query):
    """Handle parse resume."""
    task_id = int(callback_query.data.split(":")[1])
    
    result = await api_client.start_parse_task(task_id)
    
    if result.get('success'):
        task_data = await api_client.get_parse_task(task_id)
        text = format_parse_status(task_data.get('task', {}))
        
        await callback_query.edit_message_text(
            text,
            reply_markup=get_parse_running_keyboard(task_id)
        )
        await callback_query.answer("Парсинг продолжен")
    else:
        await callback_query.answer(f"Ошибка: {result.get('error')}", show_alert=True)


async def handle_parse_delete(client: Client, callback_query):
    """Handle parse delete."""
    task_id = int(callback_query.data.split(":")[1])
    
    result = await api_client.delete_parse_task(task_id)
    
    if result.get('success'):
        await callback_query.answer("Задача удалена")
        await show_tasks_status(client, callback_query.message, edit_message=True)
    else:
        await callback_query.answer(f"Ошибка: {result.get('error')}", show_alert=True)


async def handle_parse_status(client: Client, callback_query):
    """Show parse task details."""
    task_id = int(callback_query.data.split(":")[1])
    
    task_data = await api_client.get_parse_task(task_id)
    if task_data.get('success'):
        task = task_data.get('task', {})
        text = format_parse_status(task)
        
        kb = get_parse_running_keyboard(task_id) if task['status'] == 'running' else get_parse_paused_keyboard(task_id)
        
        await callback_query.edit_message_text(text, reply_markup=kb)
        await callback_query.answer()
    else:
        await callback_query.answer(f"Ошибка: {task_data.get('error')}", show_alert=True)


async def handle_invite_refresh(client: Client, callback_query):
    """Handle invite status refresh."""
    task_id = int(callback_query.data.split(":")[1])
    
    task_data = await api_client.get_task(task_id)
    text = format_invite_status(task_data)
    
    status = task_data.get('status', 'pending')
    if status == 'running':
        keyboard = get_invite_running_keyboard(task_id)
    else:
        keyboard = get_invite_paused_keyboard(task_id)
    
    try:
        await callback_query.edit_message_text(text, reply_markup=keyboard)
        await callback_query.answer("Статус обновлен")
    except MessageNotModified:
        # Сообщение не изменилось - это нормально, просто подтверждаем действие
        await callback_query.answer("Статус актуален")


async def handle_task_delete(client: Client, callback_query):
    """Handle task deletion from tasks list."""
    task_id = int(callback_query.data.split(":")[1])
    
    # Answer callback query immediately
    try:
        await callback_query.answer("⏳ Удаление...")
    except Exception:
        pass  # Ignore if query already expired
    
    result = await api_client.delete_task(task_id)
    
    if result.get('success'):
        # Refresh tasks list
        try:
            await show_tasks_status(client, callback_query.message, page=0, edit_message=True)
        except Exception as e:
            logger.error(f"Error refreshing tasks list after delete: {e}")
            # Try to answer again if query is still valid
            try:
                await callback_query.answer("✅ Задача удалена", show_alert=True)
            except Exception:
                pass  # Query expired, ignore
    else:
        # Try to answer with error if query is still valid
        try:
            await callback_query.answer(f"❌ Ошибка: {result.get('error')}", show_alert=True)
        except Exception:
            pass  # Query expired, ignore


async def handle_clear_completed_tasks(client: Client, callback_query):
    """Handle clearing all completed and failed tasks."""
    user_id = int(callback_query.from_user.id)
    
    # Answer callback query immediately to prevent timeout
    try:
        await callback_query.answer("⏳ Удаление задач...")
    except Exception:
        pass  # Ignore if query already expired
    
    # Get all tasks
    result = await api_client.get_user_tasks(user_id)
    tasks = result.get('tasks', [])
    
    # Filter completed, failed and pending tasks
    completed_tasks = [t for t in tasks if t['status'] in ['completed', 'failed', 'pending']]
    
    if not completed_tasks:
        # Already answered, just refresh the list
        try:
            await show_tasks_status(client, callback_query.message, page=0, edit_message=True)
        except Exception:
            pass
        return
    
    # Delete all completed/failed/pending tasks
    deleted_count = 0
    errors = []
    
    for task in completed_tasks:
        result = await api_client.delete_task(task['id'])
        if result.get('success'):
            deleted_count += 1
        else:
            errors.append(f"Задача {task['id']}: {result.get('error', 'Unknown error')}")
    
    # Refresh tasks list
    try:
        await show_tasks_status(client, callback_query.message, page=0, edit_message=True)
    except Exception as e:
        logger.error(f"Error refreshing tasks list after clear: {e}")
        # Try to send notification if query is still valid
        try:
            if deleted_count > 0:
                await callback_query.answer(f"✅ Удалено задач: {deleted_count}", show_alert=True)
            else:
                error_msg = "Не удалось удалить задачи"
                if errors:
                    error_msg += f": {', '.join(errors[:3])}"
                await callback_query.answer(f"❌ {error_msg}", show_alert=True)
        except Exception:
            pass  # Query expired, ignore


async def handle_filter_mode_selection(client: Client, callback_query):
    """Show filter mode selection menu."""
    user_id = int(callback_query.from_user.id)
    settings = user_states.get(user_id, {}).get('invite_settings', {})
    current_mode = settings.get('filter_mode', 'all')

    buttons = [
        [InlineKeyboardButton(f"{'✅ ' if current_mode == 'all' else ''}Всех (без фильтра)", callback_data="filter_mode:all")],
        [InlineKeyboardButton(f"{'✅ ' if current_mode == 'exclude_admins' else ''}Кроме админов", callback_data="filter_mode:exclude_admins")],
        [InlineKeyboardButton(f"{'✅ ' if current_mode == 'exclude_inactive' else ''}Кроме неактивных", callback_data="filter_mode:exclude_inactive")],
        [InlineKeyboardButton(f"{'✅ ' if current_mode == 'exclude_admins_and_inactive' else ''}Кроме админов и неактивных", callback_data="filter_mode:exclude_admins_and_inactive")],
        [InlineKeyboardButton("🔙 Назад", callback_data="settings_back")]
    ]

    await callback_query.edit_message_text(
        "👥 **Настройка фильтрации пользователей**\n\nВыберите режим фильтрации:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    await callback_query.answer()


async def handle_filter_mode_change(client: Client, callback_query):
    """Handle filter mode change."""
    user_id = int(callback_query.from_user.id)
    new_mode = callback_query.data.split(":")[1]

    settings = user_states.get(user_id, {}).get('invite_settings', {})
    settings['filter_mode'] = new_mode
    user_states[user_id]['invite_settings'] = settings

    await callback_query.answer(f"Режим фильтрации установлен: {new_mode}")
    await handle_filter_mode_selection(client, callback_query) # Refresh menu


async def handle_settings_menu(client: Client, callback_query):
    """Handle settings menu open."""
    user_id = int(callback_query.from_user.id)
    settings = user_states.get(user_id, {}).get('invite_settings', {})
    
    user_states[user_id]['state'] = FSM_INVITE_SETTINGS
    
    await callback_query.edit_message_text(
        "⚙️ **Настройки инвайтинга**\n\nВыберите параметр для изменения:",
        reply_markup=get_settings_keyboard(settings)
    )


async def handle_session_selection(client: Client, callback_query):
    """Handle session selection for inviting."""
    user_id = int(callback_query.from_user.id)
    selected = user_states.get(user_id, {}).get('invite_settings', {}).get('selected_sessions', [])
    
    keyboard = await get_session_select_keyboard(selected)
    
    await callback_query.edit_message_text(
        "🔐 **Выбор сессий для инвайтинга**\n\n"
        "Выберите сессии, которые будут использоваться при ротации:",
        reply_markup=keyboard
    )


async def handle_toggle_session(client: Client, callback_query):
    """Handle session toggle in selection."""
    user_id = int(callback_query.from_user.id)
    session_alias = callback_query.data.split(":")[1]
    
    # Check if we're in parse or invite mode
    state = user_states.get(user_id, {}).get('state')
    
    if state == FSM_PARSE_SESSION_SELECT:
        settings = user_states.get(user_id, {}).get('parse_settings', {})
    else:
        settings = user_states.get(user_id, {}).get('invite_settings', {})
    
    selected = settings.get('selected_sessions', [])
    
    if session_alias in selected:
        selected.remove(session_alias)
    else:
        selected.append(session_alias)
    
    settings['selected_sessions'] = selected
    
    if state == FSM_PARSE_SESSION_SELECT:
        user_states[user_id]['parse_settings'] = settings
    else:
        user_states[user_id]['invite_settings'] = settings
    
    keyboard = await get_session_select_keyboard(selected)
    
    # При выборе сессий для инвайта/парсинга Телеграм может вернуть
    # MESSAGE_NOT_MODIFIED, если разметка фактически не изменилась.
    # Это нормальная ситуация, поэтому просто игнорируем это исключение.
    await safe_edit_message_reply_markup(callback_query, reply_markup=keyboard)
    await callback_query.answer()


async def handle_session_proxy_input(client: Client, message: Message, text: str):
    """Handle session proxy input."""
    user_id = message.from_user.id

    if user_id not in user_states or 'session_name' not in user_states[user_id]:
        await message.reply("❌ Ошибка состояния. Попробуйте снова.")
        return

    session_name = user_states[user_id]['session_name']

    try:
        response = await api_client.set_session_proxy(session_name, text)
        if response.get("success"):
            proxy = response.get("proxy")
            if proxy:
                await message.reply(f"✅ Прокси для сессии **{session_name}** установлен!\n\nПрокси: `{proxy}`")
            else:
                await message.reply(f"🗑️ Прокси для сессии **{session_name}** удален!")
        else:
            await message.reply(f"❌ Ошибка: {response.get('error', 'Неизвестная ошибка')}")

    except Exception as e:
        await message.reply(f"❌ Ошибка при настройке прокси: {e}")
    finally:
        # Reset state
        user_states[user_id] = {"state": FSM_MAIN_MENU}
        await show_main_menu(client, message)


# ============== Parsing to File Handlers ==============

async def start_parse_to_file_flow(client: Client, message: Message):
    """Start the parse to file flow - ask for file name."""
    user_id = message.from_user.id
    
    # Check for sessions first
    result = await api_client.list_sessions()
    sessions = result.get('sessions', [])
    
    if not sessions:
        await message.reply(
            "⚠️ **Нет активных сессий!**\n\n"
            "Для парсинга необходимо добавить хотя бы одну сессию.\n"
            "Перейдите в меню 🔐 **Сессии** -> **Добавить сессию**.",
            reply_markup=get_main_keyboard()
        )
        return
    
    # Initialize parse settings
    user_states[user_id] = {
        'state': FSM_PARSE_FILE_NAME,
        'parse_settings': {
            'filter_admins': False,
            'filter_inactive': False,
            'inactive_days': 30,
            'keywords': []
        }
    }
    
    await message.reply(
        "🔍 **Парсинг пользователей в файл**\n\n"
        "📝 Введите имя файла для сохранения пользователей\n"
        "(например: 'юзы_из_чата1'):",
        reply_markup=ReplyKeyboardRemove()
    )


async def handle_parse_source_group_input(client: Client, message: Message, text: str):
    """Handle source group input for parsing."""
    user_id = message.from_user.id
    
    # Try to parse as button
    group_data = parse_group_button(text)
    
    if group_data:
        group_id = group_data['id']
        group_title = group_data['title']
        username = group_data.get('username')
    else:
        normalized = normalize_group_input(text)
        
        # Get sessions - prefer parsing-assigned sessions
        sessions_result = await api_client.list_sessions()
        sessions = sessions_result.get('sessions', [])
        assignments = sessions_result.get('assignments', {})
        
        # Prefer parsing sessions, then any available session
        parsing_sessions = assignments.get('parsing', [])
        session_alias = parsing_sessions[0] if parsing_sessions else (
            sessions[0]['alias'] if sessions else None
        )
        
        if not session_alias:
            await message.reply(
                "❌ Нет доступных сессий для проверки группы.\\n"
                "Добавьте сессию в меню 🔐 Сессии",
                reply_markup=get_main_keyboard()
            )
            user_states[user_id] = {"state": FSM_MAIN_MENU}
            return
        
        # Resolve group
        group_info = await api_client.get_group_info(session_alias, normalized)
        
        if not group_info.get('success') or not group_info.get('id'):
            error_detail = group_info.get('error', 'Неизвестная ошибка')
            if 'Session not available' in str(error_detail):
                await message.reply(
                    f"❌ **Сессия `{session_alias}` недоступна**\\n\\n"
                    "Возможные причины:\\n"
                    "• Сессия не авторизована\\n"
                    "• Сессия заблокирована\\n"
                    "• Неверные API credentials\\n\\n"
                    "Назначьте рабочую сессию на парсинг в меню 🔐 **Сессии**\\n\\n"
                    "Отправьте /start чтобы вернуться в главное меню",
                    reply_markup=get_main_keyboard()
                )
                user_states[user_id] = {"state": FSM_MAIN_MENU}
            else:
                await message.reply(
                    "❌ Не удалось найти группу. Проверьте ссылку или ID.\\n"
                    "Попробуйте еще раз или отправьте /start для возврата в меню:"
                )
            return
        
        group_id = str(group_info['id'])
        group_title = group_info.get('title', f'Группа {group_id}')
        username = group_info.get('username')
        
        # Save to history
        await api_client.add_user_group(user_id, group_id, group_title, username)
    
    # Save source group
    user_states[user_id]['parse_source_group'] = {
        'id': int(group_id),
        'title': group_title,
        'username': username
    }
    
    # Update last used
    await api_client.update_user_group_last_used(user_id, group_id)
    
    # Initialize default settings if not exists
    if 'parse_settings' not in user_states[user_id]:
        user_states[user_id]['parse_settings'] = {}
    
    # Set defaults
    settings = user_states[user_id]['parse_settings']
    if 'use_proxy' not in settings:
        settings['use_proxy'] = True
    if 'delay_seconds' not in settings:
        settings['delay_seconds'] = 2
    if 'rotate_sessions' not in settings:
        settings['rotate_sessions'] = False
    if 'rotate_every' not in settings:
        settings['rotate_every'] = 0
    if 'filter_admins' not in settings:
        settings['filter_admins'] = False
    if 'filter_inactive' not in settings:
        settings['filter_inactive'] = False
    if 'inactive_days' not in settings:
        settings['inactive_days'] = 30
    
    # Set parse mode based on source type
    source_type = user_states[user_id].get('parse_source_type', 'group')
    # Save source_type in settings for UI
    settings['source_type'] = source_type
    
    if source_type == 'channel':
        # For channels, always use message_based mode (comments)
        settings['parse_mode'] = 'message_based'
        # Set default message-based settings if not present
        if 'delay_every_requests' not in settings:
            settings['delay_every_requests'] = 1
        if 'rotate_every_requests' not in settings:
            settings['rotate_every_requests'] = 0
        if 'save_every_users' not in settings:
            settings['save_every_users'] = 0
    else:
        # For groups, default to member_list mode (can be changed later)
        if 'parse_mode' not in settings:
            settings['parse_mode'] = 'member_list'
    
    # Show settings
    await show_parse_settings(client, message)


async def show_parse_settings(client: Client, message: Message):
    """Show parsing settings menu."""
    user_id = message.from_user.id
    
    file_name = user_states.get(user_id, {}).get('parse_file_name', 'N/A')
    source = user_states.get(user_id, {}).get('parse_source_group', {})
    settings = user_states.get(user_id, {}).get('parse_settings', {})
    
    user_states[user_id]['state'] = FSM_PARSE_SETTINGS
    
    limit = settings.get('limit')
    limit_text = str(limit) if limit else "Без лимита"
    delay = settings.get('delay_seconds', 2)
    rotate = settings.get('rotate_sessions', False)
    rotate_every = settings.get('rotate_every', 0)
    use_proxy = settings.get('use_proxy', True)
    filter_admins = "Да" if settings.get('filter_admins') else "Нет"
    filter_inactive = "Да" if settings.get('filter_inactive') else "Нет"
    inactive_days = settings.get('inactive_days', 30)
    
    # New params
    parse_mode = settings.get('parse_mode', 'member_list')
    keyword_filter = settings.get('keyword_filter', [])
    exclude_keywords = settings.get('exclude_keywords', [])
    
    mode_text = "По участникам" if parse_mode == 'member_list' else "По сообщениям"
    
    rotate_text = "Да" if rotate else "Нет"
    rotate_every_text = f"каждые {rotate_every} польз." if rotate and rotate_every > 0 else "только при ошибках"
    proxy_text = "Да" if use_proxy else "Нет"
    
    # Session info
    selected_sessions = settings.get('selected_sessions', [])
    session_text = f"{len(selected_sessions)} шт." if selected_sessions else "Не выбраны"
    
    # Construct message text based on parse mode
    lines = [
        "🔍 **Настройка парсинга**",
        "",
        f"📝 Файл: **{file_name}**",
        f"📤 Группа: **{source.get('title', 'N/A')}**",
        f"📋 Режим: **{mode_text}**",
        ""
    ]
    
    if parse_mode == 'message_based':
        # Message-based mode specific settings
        messages_limit = settings.get('messages_limit')
        messages_limit_text = str(messages_limit) if messages_limit else "Без лимита"
        delay_every_requests = settings.get('delay_every_requests', 1)
        rotate_every_requests = settings.get('rotate_every_requests', 0)
        save_every_users = settings.get('save_every_users', 0)
        save_every_text = f"каждые {save_every_users} польз." if save_every_users > 0 else "в конце"
        rotate_every_text = f"каждые {rotate_every_requests} запросов" if rotate and rotate_every_requests > 0 else "только при ошибках"
        
        keywords_str = f"{len(keyword_filter)} слов" if keyword_filter else "Нет"
        exclude_str = f"{len(exclude_keywords)} слов" if exclude_keywords else "Нет"
        
        source_type = settings.get('source_type', 'group')
        limit_label = "Лимит постов" if source_type == 'channel' else "Лимит сообщений"
        
        lines.extend([
            "⚙️ **Основные настройки:**",
            f"🔢 {limit_label}: {messages_limit_text}",
            f"⏱️ Задержка: {delay} сек каждые {delay_every_requests} запросов",
            f"🔄 Ротация сессий: {rotate_text} ({rotate_every_text})",
            f"💾 Сохранение: {save_every_text}",
            f"🌐 Использовать прокси: {proxy_text}",
            f"🔐 Сессии: {session_text}",
            "",
        ])

        if settings.get('source_type', 'group') != 'channel':
            lines.extend([
                "⚙️ **Фильтры:**",
                f"🚫 Исключить админов: {filter_admins}",
                f"🛌 Исключить неактивных: {filter_inactive}",
                f"📅 Неактивен более: {inactive_days} дн.",
                "",
            ])

        lines.extend([
            "⚙️ **Фильтры сообщений:**",
            f"🔑 Ключевые слова: {keywords_str}",
            f"🚫 Исключить слова: {exclude_str}"
        ])
    else:
        # Member list mode settings (original)
        lines.extend([
            "⚙️ **Основные настройки:**",
            f"🔢 Лимит: {limit_text}",
            f"⏱️ Задержка: {delay} сек",
            f"🔄 Ротация сессий: {rotate_text} ({rotate_every_text})",
            f"🌐 Использовать прокси: {proxy_text}",
            f"🔐 Сессии: {session_text}",
            "",
            "⚙️ **Фильтры:**",
            f"🚫 Исключить админов: {filter_admins}",
            f"🛌 Исключить неактивных: {filter_inactive}",
            f"📅 Неактивен более: {inactive_days} дн."
        ])

    lines.append("\nНастройте параметры или начните парсинг:")
    
    text = "\n".join(lines)
    
    await message.reply(
        text,
        reply_markup=get_parse_settings_keyboard(settings)
    )




async def start_parsing_to_file(client: Client, callback_query):
    """Start parsing users to file via API task."""
    user_id = int(callback_query.from_user.id)
    
    file_name = user_states.get(user_id, {}).get('parse_file_name')
    source = user_states.get(user_id, {}).get('parse_source_group')
    settings = user_states.get(user_id, {}).get('parse_settings', {})
    
    if not file_name or not source:
        await callback_query.answer("Ошибка: не все данные заполнены", show_alert=True)
        return
    
    # Get selected sessions
    selected_sessions = settings.get('selected_sessions', [])
    logger.info(f"[PARSE_START] User {user_id} selected sessions: {selected_sessions}")
    
    # If no sessions selected, use sessions assigned to 'parsing' task
    if not selected_sessions:
        sessions_result = await api_client.list_sessions()
        sessions = sessions_result.get('sessions', [])
        assignments = sessions_result.get('assignments', {})
        
        # Prefer parsing-assigned sessions
        parsing_sessions = assignments.get('parsing', [])
        if parsing_sessions:
            selected_sessions = parsing_sessions
            logger.info(f"[PARSE_START] Using parsing-assigned sessions: {selected_sessions}")
        else:
            # Fallback to all sessions if no parsing assignments
            selected_sessions = [s['alias'] for s in sessions]
            logger.info(f"[PARSE_START] No parsing assignments, using all sessions: {selected_sessions}")
        
        if not selected_sessions:
            await callback_query.answer(
                "❌ Нет доступных сессий для парсинга!\n"
                "Назначьте сессию на парсинг в меню 🔐 Сессии",
                show_alert=True
            )
            return
    
    # Use first session as primary
    session_alias = selected_sessions[0]
    
    try:
        # Create parse task via API
        # Determine which parameters to use based on parse mode
        parse_mode = settings.get('parse_mode', 'member_list')
        source_type = user_states.get(user_id, {}).get('parse_source_type', 'group')
        
        if parse_mode == 'message_based':
            # Message-based mode: use messages-specific params
            result = await api_client.create_parse_task(
                user_id=user_id,
                file_name=file_name,
                source_group_id=source['id'],
                source_group_title=source['title'],
                source_username=source.get('username'),
                session_alias=session_alias,
                source_type=source_type,  # Add source type
                delay_seconds=settings.get('delay_seconds', 2),
                limit=None,  # Not used in message_based mode
                save_every=0,  # Not used in message_based mode
                rotate_sessions=settings.get('rotate_sessions', False),
                rotate_every=0,  # Not used in message_based mode
                use_proxy=settings.get('use_proxy', True),
                available_sessions=selected_sessions,
                filter_admins=settings.get('filter_admins', False),
                filter_inactive=settings.get('filter_inactive', False),
                inactive_threshold_days=settings.get('inactive_days', 30),
                parse_mode=parse_mode,
                keyword_filter=settings.get('keyword_filter', []),
                exclude_keywords=settings.get('exclude_keywords', []),
                # Message-based specific params
                messages_limit=settings.get('messages_limit'),
                delay_every_requests=settings.get('delay_every_requests', 1),
                rotate_every_requests=settings.get('rotate_every_requests', 0),
                save_every_users=settings.get('save_every_users', 0)
            )
        else:
            # Member list mode: use standard params
            result = await api_client.create_parse_task(
                user_id=user_id,
                file_name=file_name,
                source_group_id=source['id'],
                source_group_title=source['title'],
                source_username=source.get('username'),
                session_alias=session_alias,
                source_type=source_type,  # Add source type
                delay_seconds=settings.get('delay_seconds', 2),
                limit=settings.get('limit'),
                save_every=settings.get('save_every', 0),
                rotate_sessions=settings.get('rotate_sessions', False),
                rotate_every=settings.get('rotate_every', 0),
                use_proxy=settings.get('use_proxy', True),
                available_sessions=selected_sessions,
                filter_admins=settings.get('filter_admins', False),
                filter_inactive=settings.get('filter_inactive', False),
                inactive_threshold_days=settings.get('inactive_days', 30),
                parse_mode=parse_mode,
                keyword_filter=[],
                exclude_keywords=[]
            )
        
        if not result.get('success'):
            await callback_query.edit_message_text(
                f"❌ Ошибка создания задачи: {result.get('error', 'Неизвестная ошибка')}"
            )
            return
        
        task_id = result.get('task_id')
        
        # Start the task
        start_result = await api_client.start_parse_task(task_id)
        
        if not start_result.get('success'):
            await callback_query.edit_message_text(
                f"❌ Ошибка запуска задачи: {start_result.get('error', 'Неизвестная ошибка')}"
            )
            return
        
        # Get actual task data to show status
        task_data = await api_client.get_parse_task(task_id)
        if task_data.get('success'):
            task = task_data.get('task', {})
            text = format_parse_status(task)
            kb = get_parse_running_keyboard(task_id)
            
            await callback_query.edit_message_text(text, reply_markup=kb)
        else:
            await callback_query.edit_message_text(
                f"✅ **Задача парсинга создана и запущена!**\n\n"
                f"Проверить статус можно в меню **📊 Статус задач**"
            )
        
        # Reset state
        user_states[user_id] = {"state": FSM_MAIN_MENU}
        
        # Send a separate message with main menu
        await callback_query.message.reply(
            "Парсинг запущен в фоновом режиме. Выберите действие:",
            reply_markup=get_main_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Error creating parse task: {e}")
        await callback_query.edit_message_text(
            f"❌ Ошибка при создании задачи: {e}"
        )


# ============== Inviting from File Handlers ==============

async def start_invite_from_file_flow(client: Client, message: Message):
    """Start the invite from file flow - show file selection."""
    user_id = message.from_user.id
    
    # Check for sessions first
    result = await api_client.list_sessions()
    sessions = result.get('sessions', [])
    assignments = result.get('assignments', {})
    inviting_sessions = assignments.get('inviting', [])
    
    if not inviting_sessions:
        await message.reply(
            "⚠️ **Нет сессий для инвайтинга!**\n\n"
            "Назначьте сессию для инвайтинга в меню 🔐 **Сессии**.",
            reply_markup=get_main_keyboard()
        )
        return
    
    # Initialize state
    user_states[user_id] = {
        'state': FSM_INVITE_FILE_SELECT,
        'invite_settings': {
            'delay_seconds': 30,
            'delay_every': 1,
            'limit': None,
            'rotate_sessions': False,
            'rotate_every': 0,
            'use_proxy': True,
            'selected_sessions': [],
            'filter_mode': 'all',
            'inactive_threshold_days': None
        }
    }
    
    kb = await get_user_files_keyboard()
    
    await message.reply(
        "📁 **Инвайтинг из файла**\n\n"
        "Выберите файл с пользователями:",
        reply_markup=kb
    )


async def handle_invite_from_file_target_input(client: Client, message: Message, text: str):
    """Handle target group input for inviting from file."""
    user_id = message.from_user.id
    
    # Try to parse as button
    group_data = parse_group_button(text)
    
    if group_data:
        group_id = group_data['id']
        group_title = group_data['title']
        username = group_data.get('username')
    else:
        normalized = normalize_group_input(text)
        
        # Get session for resolving
        sessions_result = await api_client.list_sessions()
        sessions = sessions_result.get('sessions', [])
        assignments = sessions_result.get('assignments', {})
        
        inviting_sessions = assignments.get('inviting', [])
        session_alias = inviting_sessions[0] if inviting_sessions else (
            sessions[0]['alias'] if sessions else None
        )
        
        if not session_alias:
            await message.reply(
                "❌ Нет доступных сессий для проверки группы.\n"
                "Добавьте сессию в меню 🔐 Сессии"
            )
            return
        
        group_info = await api_client.get_group_info(session_alias, normalized)
        
        if not group_info.get('success') or not group_info.get('id'):
            await message.reply(
                "❌ Не удалось найти группу. Проверьте ссылку или ID.\n"
                "Попробуйте еще раз:"
            )
            return
        
        group_id = str(group_info['id'])
        group_title = group_info.get('title', f'Группа {group_id}')
        username = group_info.get('username')
        
        await api_client.add_user_target_group(user_id, group_id, group_title, username)
    
    # Save target group
    user_states[user_id]['target_group'] = {
        'id': int(group_id),
        'title': group_title,
        'username': username
    }
    
    await api_client.update_user_target_group_last_used(user_id, group_id)
    
    # Show invite menu
    user_states[user_id]['state'] = FSM_INVITE_MENU
    
    file_name = user_states[user_id].get('source_file', 'N/A')
    settings = user_states[user_id].get('invite_settings', {})
    
    rotate_info = 'Да' if settings.get('rotate_sessions') else 'Нет'
    if settings.get('rotate_sessions') and settings.get('rotate_every', 0) > 0:
        rotate_info += f" (каждые {settings['rotate_every']} инв.)"

    # Construct message text safely
    lines = [
        "✅ **Настройка инвайтинга из файла**",
        "",
        f"📁 Файл: **{file_name}**",
        f"📥 Цель: **{group_title}**",
        "🎯 Режим: **Из файла**",
        "",
        "⚙️ **Текущие настройки:**",
        f"⏱️ Задержка: ~{settings.get('delay_seconds', 30)} сек",
        f"🔢 Каждые {settings.get('delay_every', 1)} инвайта",
        f"🔢 Лимит: {settings.get('limit') or 'Без лимита'}",
        f"🔄 Ротация сессий: {rotate_info}",
        "",
        "Выберите действие:"
    ]
    
    text = "\n".join(lines)
    
    await message.reply(
        text,
        reply_markup=get_invite_menu_keyboard()
    )


# ============== File Manager Handlers ==============

async def start_file_manager(client: Client, message: Message):
    """Start the file manager - show list of files."""
    user_id = message.from_user.id
    
    user_states[user_id] = {
        'state': FSM_FILE_MANAGER,
        'fm_page': 0  # Initialize pagination
    }
    
    kb = await get_file_manager_list_keyboard(0)
    
    await message.reply(
        "📁 **Менеджер файлов**\n\n"
        "Здесь вы можете управлять файлами, созданными через парсинг:\n"
        "• 📊 Просмотреть статистику\n"
        "• 📋 Копировать файлы\n"
        "• ✏️ Переименовать\n"
        "• 🗑️ Удалить\n"
        "• 🔧 Фильтровать пользователей\n\n"
        "Выберите файл для управления:",
        reply_markup=kb
    )


async def handle_file_manager_copy_name(client: Client, message: Message, text: str):
    """Handle copy file name input."""
    user_id = message.from_user.id
    source_filename = user_states[user_id].get('fm_selected_file')
    
    if not source_filename:
        await message.reply("❌ Ошибка: файл не выбран")
        await start_file_manager(client, message)
        return
    
    from shared.user_files_manager import UserFilesManager
    from shared.validation import sanitize_filename
    
    is_valid, clean_name, error_msg = sanitize_filename(text)
    
    if not is_valid:
        await message.reply(
            f"❌ Ошибка валидации: {error_msg}\n\n"
            "Попробуйте другое имя:"
        )
        return
    
    manager = UserFilesManager()
    
    new_path = manager.copy_file(source_filename, clean_name)
    
    if new_path:
        # Get new filename from path
        import os
        new_name = os.path.basename(new_path).replace('.json', '')
        
        await message.reply(
            f"✅ **Файл успешно скопирован!**\n\n"
            f"📄 Исходный файл: `{source_filename}`\n"
            f"📄 Новый файл: `{new_name}`\n\n"
            "Возвращаемся к менеджеру файлов..."
        )
    else:
        await message.reply(
            "❌ **Ошибка при копировании файла**\n\n"
            "Возможные причины:\n"
            "• Исходный файл не найден\n"
            "• Ошибка записи\n\n"
            "Попробуйте еще раз."
        )
    
    await start_file_manager(client, message)


async def handle_file_manager_rename(client: Client, message: Message, text: str):
    """Handle rename file input."""
    user_id = message.from_user.id
    old_filename = user_states[user_id].get('fm_selected_file')
    
    if not old_filename:
        await message.reply("❌ Ошибка: файл не выбран")
        await start_file_manager(client, message)
        return
    
    from shared.user_files_manager import UserFilesManager
    from shared.validation import sanitize_filename
    
    is_valid, clean_name, error_msg = sanitize_filename(text)
    
    if not is_valid:
        await message.reply(
            f"❌ Ошибка валидации: {error_msg}\n\n"
            "Попробуйте другое имя:"
        )
        return
    
    manager = UserFilesManager()
    
    new_path = manager.rename_file(old_filename, clean_name)
    
    if new_path:
        import os
        new_name = os.path.basename(new_path).replace('.json', '')
        
        await message.reply(
            f"✅ **Файл успешно переименован!**\n\n"
            f"📄 Было: `{old_filename}`\n"
            f"📄 Стало: `{new_name}`\n\n"
            "Возвращаемся к менеджеру файлов..."
        )
    else:
        await message.reply(
            "❌ **Ошибка при переименовании файла**\n\n"
            "Возможные причины:\n"
            "• Файл не найден\n"
            "• Файл с таким именем уже существует\n"
            "• Недопустимые символы в имени\n\n"
            "Попробуйте еще раз."
        )
    
    await start_file_manager(client, message)


async def handle_file_manager_filter_keyword(client: Client, message: Message, text: str):
    """Handle filter keyword input."""
    user_id = message.from_user.id
    filename = user_states[user_id].get('fm_selected_file')
    filter_mode = user_states[user_id].get('fm_filter_mode', 'remove')
    
    if not filename:
        await message.reply("❌ Ошибка: файл не выбран")
        await start_file_manager(client, message)
        return
    
    keyword = text.strip()
    if not keyword:
        await message.reply("❌ Ключевое слово не может быть пустым")
        return
    
    from shared.user_files_manager import UserFilesManager
    manager = UserFilesManager()
    
    if filter_mode == 'remove':
        filter_type = 'remove_by_keyword'
    else:
        filter_type = 'keep_by_keyword'
    
    result = manager.filter_users_in_file(filename, filter_type, keyword=keyword)
    
    if result.get('success'):
        action_text = "удалены" if filter_mode == 'remove' else "оставлены"
        await message.reply(
            f"✅ **Фильтрация завершена!**\n\n"
            f"📄 Файл: `{filename}`\n"
            f"🔍 Ключевое слово: `{keyword}`\n\n"
            f"📊 **Результат:**\n"
            f"• Было пользователей: {result['original_count']}\n"
            f"• Стало пользователей: {result['new_count']}\n"
            f"• Удалено: {result['removed_count']}\n\n"
            "Возвращаемся к менеджеру файлов..."
        )
    else:
        await message.reply(
            f"❌ **Ошибка при фильтрации**\n\n"
            f"Причина: {result.get('error', 'Неизвестная ошибка')}"
        )
    
    await start_file_manager(client, message)
