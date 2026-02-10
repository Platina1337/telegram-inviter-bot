# -*- coding: utf-8 -*-
"""
Main main handlers for the inviter bot.
"""
import logging
from typing import Dict
from pyrogram import Client, filters
from pyrogram.types import (
    Message, ReplyKeyboardRemove, CallbackQuery, InlineKeyboardMarkup, 
    InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
)
from pyrogram.errors import FloodWait, RPCError, MessageNotModified

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
    # Post Forwarding states
    FSM_POST_FORWARD_SOURCE_TYPE, FSM_POST_FORWARD_SOURCE, FSM_POST_FORWARD_TARGET_TYPE,
    FSM_POST_FORWARD_TARGET, FSM_POST_FORWARD_SESSION_SELECT, FSM_POST_FORWARD_MODE_SELECT,
    FSM_POST_FORWARD_SETTINGS, FSM_POST_FORWARD_SETTINGS_LIMIT, FSM_POST_FORWARD_SETTINGS_DELAY,
    FSM_POST_FORWARD_SETTINGS_DELAY_EVERY, FSM_POST_FORWARD_SETTINGS_ROTATE_EVERY,
    FSM_POST_FORWARD_SETTINGS_NATIVE,
    FSM_POST_FORWARD_SETTINGS_KEYWORDS_WHITELIST, FSM_POST_FORWARD_SETTINGS_KEYWORDS_BLACKLIST,
    FSM_POST_FORWARD_SIGNATURE_LABEL_POST, FSM_POST_FORWARD_SIGNATURE_LABEL_SOURCE, FSM_POST_FORWARD_SIGNATURE_LABEL_AUTHOR,
    FSM_PP_EDIT_SESSION_SELECT,
    get_main_keyboard, get_group_history_keyboard, get_target_group_history_keyboard,
    get_parse_source_group_history_keyboard,
    get_invite_menu_keyboard, get_settings_keyboard, get_session_select_keyboard,
    get_invite_running_keyboard, get_invite_paused_keyboard,
    get_parse_running_keyboard, get_parse_paused_keyboard,
    get_parse_settings_keyboard, get_user_files_keyboard,
    get_file_manager_list_keyboard, get_file_actions_keyboard, get_file_filter_keyboard, format_file_stats,
    get_filename_by_index,
    parse_group_button, normalize_group_input, format_group_button,
    get_full_group_title_from_history,
    format_invite_status, format_parse_status,
    # Post Forwarding keyboards and formatters
    get_post_forward_main_keyboard, get_post_forward_source_type_keyboard,
    get_post_forward_target_type_keyboard, get_post_forward_mode_keyboard,
    get_post_forward_settings_keyboard,
    get_post_forward_settings_message_text,
    get_signature_options_keyboard,
    get_signature_options_message_text,
    get_default_signature_options,
    get_post_forward_session_keyboard,
    get_post_parse_running_keyboard, get_post_parse_paused_keyboard,
    get_post_monitor_running_keyboard, get_post_monitor_paused_keyboard,
    format_post_parse_status, format_post_monitor_status, format_session_error_message
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
        elif text == "📨 Пересылка постов":
            await start_post_forward_flow(client, message)
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
    
    # ============== Post Forwarding States ==============
    if state in [FSM_POST_FORWARD_SOURCE, FSM_POST_FORWARD_TARGET,
                 FSM_POST_FORWARD_SETTINGS_LIMIT, FSM_POST_FORWARD_SETTINGS_DELAY,
                 FSM_POST_FORWARD_SETTINGS_DELAY_EVERY, FSM_POST_FORWARD_SETTINGS_ROTATE_EVERY,
                 FSM_POST_FORWARD_SETTINGS_KEYWORDS_WHITELIST, FSM_POST_FORWARD_SETTINGS_KEYWORDS_BLACKLIST,
                 FSM_POST_FORWARD_SIGNATURE_LABEL_POST, FSM_POST_FORWARD_SIGNATURE_LABEL_SOURCE, FSM_POST_FORWARD_SIGNATURE_LABEL_AUTHOR]:
        if text == "🔙 Назад":
            await show_main_menu(client, message)
            return
        await handle_post_forward_text_input(client, message, text)
        return
    
    # Default - show main menu
    await show_main_menu(client, message)


async def resolve_group_with_rotation(normalized: str, sessions: list, assignments: dict, task_type: str = 'inviting') -> tuple:
    """
    Попытаться разрешить группу через разные сессии (ротация).
    
    Args:
        normalized: Нормализованный ввод группы (ID, username, link)
        sessions: Список всех сессий
        assignments: Назначения сессий на задачи
        task_type: Тип задачи для выбора приоритетных сессий ('inviting' или 'parsing')
        
    Returns:
        tuple: (group_info, last_error, failed_sessions_info)
    """
    # Сначала пробуем назначенные сессии
    priority_sessions = assignments.get(task_type, [])
    # Затем все остальные активные сессии
    other_sessions = [s['alias'] for s in sessions if s['is_active'] and s['alias'] not in priority_sessions]
    
    candidates = priority_sessions + other_sessions
    
    if not candidates:
        return None, "Нет активных сессий", ["• Нет доступных сессий для выполнения этой операции"]
    
    group_info = None
    last_error = None
    failed_sessions = []
    
    for session_alias in candidates:
        group_info = await api_client.get_group_info(session_alias, normalized)
        
        if group_info.get('success'):
            logger.info(f"[{task_type.upper()}] Resolved {normalized} using session {session_alias}")
            return group_info, None, []
        else:
            last_error = group_info.get('error', 'Неизвестная ошибка')
            failed_sessions.append(f"• {session_alias}: {last_error}")
            logger.warning(f"[{task_type.upper()}] Session {session_alias} failed to resolve {normalized}: {last_error}")
            
    return None, last_error, failed_sessions


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
        # From button — restore full title from history (button text is truncated)
        group_id = group_data['id']
        full_title, full_username = await get_full_group_title_from_history(user_id, group_id, is_target=False)
        group_title = full_title or group_data['title']
        username = full_username if full_username is not None else group_data.get('username')
    else:
        # User input - need to resolve
        normalized = normalize_group_input(text)
        
        # Get available sessions for rotation
        sessions_result = await api_client.list_sessions()
        sessions = sessions_result.get('sessions', [])
        assignments = sessions_result.get('assignments', {})
        
        # Resolve group with rotation
        group_info, last_error, failed_sessions = await resolve_group_with_rotation(normalized, sessions, assignments, 'inviting')
        
        if not group_info:
            # All sessions failed - show detailed error
            error_details = "\n".join(failed_sessions) if failed_sessions else "Неизвестная ошибка"
            error_msg = format_session_error_message(last_error)
            
            await message.reply(
                f"{error_msg}\n\n"
                f"**Проблемы с сессиями ({len(failed_sessions)}):**\n"
                f"{error_details}\n\n"
                "Проверьте настройки сессий или попробуйте другую группу."
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
        # From button — restore full title from history (button text is truncated)
        group_id = group_data['id']
        full_title, full_username = await get_full_group_title_from_history(user_id, group_id, is_target=True)
        group_title = full_title or group_data['title']
        username = full_username if full_username is not None else group_data.get('username')
    else:
        normalized = normalize_group_input(text)
        
        # Get session for resolving
        sessions_result = await api_client.list_sessions()
        sessions = sessions_result.get('sessions', [])
        assignments = sessions_result.get('assignments', {})
        
        # Resolve group with rotation
        group_info, last_error, failed_sessions = await resolve_group_with_rotation(normalized, sessions, assignments, 'inviting')
        
        if not group_info:
            # All sessions failed - show detailed error
            error_details = "\n".join(failed_sessions) if failed_sessions else "Неизвестная ошибка"
            error_msg = format_session_error_message(last_error)
            
            await message.reply(
                f"{error_msg}\n\n"
                f"**Проблемы с сессиями ({len(failed_sessions)}):**\n"
                f"{error_details}\n\n"
                "Проверьте настройки сессий или попробуйте другую группу."
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


async def show_tasks_status(client: Client, message: Message, page: int = 0, edit_message: bool = False, user_id: int = None):
    """Show all tasks status with pagination."""
    # Use provided user_id or get from message
    if user_id is None:
        user_id = message.from_user.id if message.from_user else message.chat.id
    
    # Get invite tasks
    invite_result = await api_client.get_user_tasks(user_id)
    invite_tasks = invite_result.get('tasks', [])
    for t in invite_tasks: t['type'] = 'invite'
    
    # Get parse tasks
    parse_result = await api_client.get_user_parse_tasks(user_id)
    parse_tasks = parse_result.get('tasks', [])
    for t in parse_tasks: t['type'] = 'parse'
    
    # Get post parse tasks
    post_parse_result = await api_client.get_user_post_parse_tasks(user_id)
    post_parse_tasks = post_parse_result.get('tasks', []) if post_parse_result.get('success') else []
    for t in post_parse_tasks: t['type'] = 'post_parse'
    
    # Get post monitoring tasks
    post_monitor_result = await api_client.get_user_post_monitoring_tasks(user_id)
    post_monitor_tasks = post_monitor_result.get('tasks', []) if post_monitor_result.get('success') else []
    for t in post_monitor_tasks: t['type'] = 'post_monitor'
    
    # Merge all tasks and sort by created_at (most recent first)
    tasks = invite_tasks + parse_tasks + post_parse_tasks + post_monitor_tasks
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
    tasks_per_page = 20
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
    
    status_names = {
        'pending': 'ожидание',
        'running': 'выполняется',
        'paused': 'пауза',
        'completed': 'завершено',
        'failed': 'ошибка'
    }
    
    buttons = []
    
    for idx, task in enumerate(page_tasks, start=start_idx + 1):
        icon = status_icons.get(task['status'], '❓')
        status_text = status_names.get(task['status'], task['status'])
        task_type = task.get('type')
        
        if task_type == 'parse':
            # Parse to file task
            parsed = task.get('parsed_count', 0)
            limit = task.get('limit')
            limit_text = f"/{limit}" if limit else ""
            task_name = f"🔍 {task.get('source_group', 'N/A')[:30]} → 📁 {task.get('file_name', 'N/A')}"
            progress_text = f"   Спаршено: {parsed}{limit_text}"
        elif task_type == 'post_parse':
            # Post parsing task
            forwarded = task.get('forwarded_count', 0)
            limit = task.get('limit')
            limit_text = f"/{limit}" if limit else ""
            source = task.get('source_title', 'N/A')[:25]
            target = task.get('target_title', 'N/A')[:25]
            task_name = f"📥 {source} → {target}"
            progress_text = f"   Переслано: {forwarded}{limit_text}"
        elif task_type == 'post_monitor':
            # Post monitoring task
            forwarded = task.get('forwarded_count', 0)
            limit = task.get('limit')
            limit_text = f"/{limit}" if limit else ""
            source = task.get('source_title', 'N/A')[:25]
            target = task.get('target_title', 'N/A')[:25]
            task_name = f"🔄 {source} → {target}"
            progress_text = f"   Переслано: {forwarded}{limit_text}"
        else:
            # Invite task
            invited = task.get('invited_count', 0)
            limit = task.get('limit')
            limit_text = f"/{limit}" if limit else ""
            task_name = f"👥 {task.get('source_group', 'N/A')[:20]} → {task.get('target_group', 'N/A')[:20]}"
            progress_text = f"   Приглашено: {invited}{limit_text}"
        
        rotate_info = ""
        if task.get('rotate_sessions'):
            every = task.get('rotate_every', 0)
            rotate_info = f" | 🔄 каждые {every}" if every > 0 else " | 🔄 Да"
        
        # Numbered task text
        task_text = f"**{idx}.** {icon} {task_name}\n"
        task_text += f"{progress_text} | {status_text}{rotate_info}"
        text += task_text + "\n\n"
        
        # Add action buttons for each task with numbers
        task_buttons = []
        
        # Determine prefix based on task type
        if task_type == 'post_parse':
            prefix = "pp"
        elif task_type == 'post_monitor':
            prefix = "pm"
        elif task_type == 'parse':
            prefix = "parse"
        else:
            prefix = "invite"
        
        if task['status'] == 'running':
            if task_type in ['post_parse', 'post_monitor']:
                task_buttons.append(InlineKeyboardButton(
                    f"{idx}. ⏸️ Пауза",
                    callback_data=f"{prefix}_pause:{task['id']}"
                ))
            else:
                task_buttons.append(InlineKeyboardButton(
                    f"{idx}. ⏹️ Стоп",
                    callback_data=f"{prefix}_stop:{task['id']}"
                ))
        elif task['status'] == 'paused':
            task_buttons.append(InlineKeyboardButton(
                f"{idx}. ▶️ Продолжить",
                callback_data=f"{prefix}_resume:{task['id']}"
            ))
        
        # Add detail button for non-post tasks
        if task_type not in ['post_parse', 'post_monitor']:
            task_buttons.append(InlineKeyboardButton(
                f"{idx}. 🔍 Детали",
                callback_data=f"{prefix}_status:{task['id']}"
            ))
        else:
            # For post tasks, refresh shows details
            task_buttons.append(InlineKeyboardButton(
                f"{idx}. 🔍 Детали",
                callback_data=f"{prefix}_refresh:{task['id']}"
            ))
        
        # Add delete button for all tasks
        task_buttons.append(InlineKeyboardButton(
            f"{idx}. 🗑️",
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
    
    # Clear buttons row
    clear_buttons = []
    if clearable_count > 0:
        clear_buttons.append(InlineKeyboardButton(
            f"🗑️ Завершённые ({clearable_count})",
            callback_data="tasks_clear_completed"
        ))
    
    # Always add "Clear all" button if there are tasks
    if len(tasks) > 0:
        clear_buttons.append(InlineKeyboardButton(
            f"🗑️ Все ({len(tasks)})",
            callback_data="tasks_clear_all"
        ))
    
    if clear_buttons:
        buttons.append(clear_buttons)
    
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
    
    # ============== Post Forwarding Callbacks ==============
    post_forward_prefixes = [
        "post_parse_start", "post_monitor_start", "post_forward_tasks", "post_forward_back",
        "pf_source_type:", "pf_target_type:", "pf_back", "pf_mode:",
        "pf_settings_", "pf_start_task",
        "pf_open_native_settings", "pf_native_toggle", "pf_native_check", "pf_native_source", "pf_native_back",
        "pf_signature_menu", "pf_sig_include_post", "pf_sig_include_source", "pf_sig_include_author",
        "pf_sig_label_post", "pf_sig_label_source", "pf_sig_label_author", "pf_sig_done",
        # Session selection
        "pf_toggle_session:", "pf_sessions_done", "pf_sessions_back", "pf_sessions_info", "pf_no_sessions",
        # Task actions
        "pp_pause:", "pp_resume:", "pp_delete:", "pp_refresh:", "pp_settings:",
        "pp_settings_save:", "pp_settings_cancel:", "pp_settings_restart:",
        "pp_settings_sessions:", "pp_edit_sessions_done:", "pp_edit_sessions_back:",
        "pm_pause:", "pm_resume:", "pm_delete:", "pm_refresh:", "pm_settings:",
        "pm_settings_save:", "pm_settings_cancel:", "pm_settings_restart:",
        "pm_settings_sessions:", "pm_edit_sessions_done:", "pm_edit_sessions_back:"
    ]
    if any(data.startswith(prefix) for prefix in post_forward_prefixes):
        handled = await handle_post_forward_callback(client, callback_query)
        if handled:
            return
    
    # ============== Invite Menu ==============
    
    if data.startswith("invite_settings_from_status:"):
        await handle_invite_settings_from_status(client, callback_query)
        return
        
    if data.startswith("parse_settings_from_status:"):
        await handle_parse_settings_from_status(client, callback_query)
        return

    if data == "invite_settings_save":
        await handle_invite_settings_save(client, callback_query)
        return
        
    if data == "invite_settings_cancel":
        await handle_invite_settings_cancel(client, callback_query)
        return

    if data == "parse_settings_save":
        await handle_parse_settings_save(client, callback_query)
        return
        
    if data == "parse_settings_cancel":
        await handle_parse_settings_cancel(client, callback_query)
        return

    if data.startswith("invite_start:"):
        await handle_invite_start(client, callback_query)
        return
    
    if data.startswith("invite_stop:"):
        await handle_invite_stop(client, callback_query)
        return
    
    if data.startswith("invite_pause:"):
        await handle_invite_pause(client, callback_query)
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
        editing_task_id = user_states.get(user_id, {}).get('editing_task_id')
        
        if state == FSM_PARSE_SESSION_SELECT:
            settings = user_states[user_id].get('parse_settings', {})
            await callback_query.edit_message_text(
                "⚙️ **Редактирование настроек задачи**\n\nИзмените нужные параметры, затем нажмите 'Сохранить':" if editing_task_id else "⚙️ **Настройки парсинга**\n\nВыберите параметр для изменения:",
                reply_markup=get_parse_settings_keyboard(settings, edit_mode=bool(editing_task_id))
            )
        else:
            settings = user_states[user_id].get('invite_settings', {})
            is_edit = bool(editing_task_id)
            await callback_query.edit_message_text(
                "⚙️ **Редактирование настроек задачи**\n\nИзмените нужные параметры, затем нажмите 'Сохранить':" if is_edit else "⚙️ **Настройки инвайтинга**\n\nВыберите параметр для изменения:",
                reply_markup=get_settings_keyboard(settings, edit_mode=is_edit)
            )
        return
    
    if data == "sessions_back":
        # Check if we're in parse or invite mode
        state = user_states.get(user_id, {}).get('state')
        editing_task_id = user_states.get(user_id, {}).get('editing_task_id')
        
        if state == FSM_PARSE_SESSION_SELECT:
            settings = user_states[user_id].get('parse_settings', {})
            await callback_query.edit_message_text(
                "⚙️ **Редактирование настроек задачи**\n\nИзмените нужные параметры, затем нажмите 'Сохранить':" if editing_task_id else "⚙️ **Настройки парсинга**\n\nВыберите параметр для изменения:",
                reply_markup=get_parse_settings_keyboard(settings, edit_mode=bool(editing_task_id))
            )
        else:
            settings = user_states[user_id].get('invite_settings', {})
            is_edit = bool(editing_task_id)
            await callback_query.edit_message_text(
                "⚙️ **Редактирование настроек задачи**\n\nИзмените нужные параметры, затем нажмите 'Сохранить':" if is_edit else "⚙️ **Настройки инвайтинга**\n\nВыберите параметр для изменения:",
                reply_markup=get_settings_keyboard(settings, edit_mode=is_edit)
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
    
    if data == "tasks_clear_all":
        await handle_clear_all_tasks(client, callback_query)
        return
    
    if data == "tasks_refresh":
        await show_tasks_status(client, callback_query.message, page=0, edit_message=True, user_id=user_id)
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

    if data.startswith("parse_pause:"):
        await handle_parse_pause(client, callback_query)
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
    
    # ============== Settings Editing from Status ==============
    
    if data.startswith("invite_settings_from_status:"):
        task_id = int(data.split(":")[1])
        
        # Load current task settings
        task_result = await api_client.get_task(task_id)
        if not task_result.get('success'):
            await callback_query.answer("❌ Не удалось загрузить задачу", show_alert=True)
            return
        
        # API инвайтинга возвращает задачу в корне ответа (без ключа "task"), в отличие от parse/pp/pm
        task_data = task_result.get('task') or task_result
        
        # Store task_id and current settings in user state
        user_states[user_id]['editing_task_id'] = task_id
        user_states[user_id]['editing_task_type'] = 'invite'
        user_states[user_id]['invite_settings'] = {
            'delay_seconds': task_data.get('delay_seconds', 30),
            'delay_every': task_data.get('delay_every', 1),
            'limit': task_data.get('limit'),
            'rotate_sessions': task_data.get('rotate_sessions', False),
            'rotate_every': task_data.get('rotate_every', 0),
            'use_proxy': task_data.get('use_proxy', True),
            'available_sessions': task_data.get('available_sessions', []),
            'selected_sessions': task_data.get('available_sessions', []),
            'filter_mode': task_data.get('filter_mode', 'all'),
            'inactive_threshold_days': task_data.get('inactive_threshold_days')
        }
        user_states[user_id]['state'] = FSM_INVITE_SETTINGS
        
        # Show settings keyboard
        await callback_query.edit_message_text(
            "⚙️ **Редактирование настроек задачи**\n\n"
            "Измените нужные параметры, затем нажмите 'Сохранить':",
            reply_markup=get_settings_keyboard(user_states[user_id]['invite_settings'], edit_mode=True)
        )
        await callback_query.answer()
        return
    
    if data.startswith("parse_settings_from_status:"):
        task_id = int(data.split(":")[1])
        
        # Load current task settings
        task_result = await api_client.get_parse_task(task_id)
        if not task_result.get('success'):
            await callback_query.answer("❌ Не удалось загрузить задачу", show_alert=True)
            return
        
        task_data = task_result.get('task', {})
        
        # Store task_id and current settings in user state
        user_states[user_id]['editing_task_id'] = task_id
        user_states[user_id]['editing_task_type'] = 'parse'
        user_states[user_id]['parse_settings'] = {
            'delay_seconds': task_data.get('delay_seconds', 2),
            'limit': task_data.get('limit'),
            'save_every': task_data.get('save_every', 0),
            'rotate_sessions': task_data.get('rotate_sessions', False),
            'rotate_every': task_data.get('rotate_every', 0),
            'use_proxy': task_data.get('use_proxy', True),
            'available_sessions': task_data.get('available_sessions', []),
            'selected_sessions': task_data.get('available_sessions', []),
            'filter_admins': task_data.get('filter_admins', False),
            'filter_inactive': task_data.get('filter_inactive', False),
            'inactive_days': task_data.get('inactive_threshold_days', 30),
            'parse_mode': task_data.get('parse_mode', 'member_list'),
            'keyword_filter': task_data.get('keyword_filter', []),
            'exclude_keywords': task_data.get('exclude_keywords', []),
            'source_type': task_data.get('source_type', 'group')
        }
        user_states[user_id]['state'] = FSM_PARSE_SETTINGS
        
        # Show settings keyboard
        await callback_query.edit_message_text(
            "⚙️ **Редактирование настроек задачи**\n\n"
            "Измените нужные параметры, затем нажмите 'Сохранить':",
            reply_markup=get_parse_settings_keyboard(user_states[user_id]['parse_settings'], edit_mode=True)
        )
        await callback_query.answer()
        return

    # ============== Save/Cancel Settings Editing ==============
    
    if data == "invite_settings_save":
        task_id = user_states.get(user_id, {}).get('editing_task_id')
        if not task_id:
            await callback_query.answer("❌ Ошибка: задача не найдена", show_alert=True)
            return
        
        settings = user_states[user_id].get('invite_settings', {})
        
        # Save settings via API
        result = await api_client.update_task(
            task_id,
            delay_seconds=settings.get('delay_seconds'),
            delay_every=settings.get('delay_every'),
            limit=settings.get('limit'),
            rotate_sessions=settings.get('rotate_sessions'),
            rotate_every=settings.get('rotate_every'),
            use_proxy=settings.get('use_proxy'),
            available_sessions=settings.get('selected_sessions') or settings.get('available_sessions')
        )
        
        if not result.get('success'):
            await callback_query.answer("❌ Не удалось сохранить настройки", show_alert=True)
            return
        
        # Clear editing state
        user_states[user_id].pop('editing_task_id', None)
        user_states[user_id].pop('editing_task_type', None)
        
        # Redirect to task details (pass task_id — callback.data is "invite_settings_save")
        await callback_query.answer("✅ Настройки сохранены")
        await handle_invite_status(client, callback_query, task_id=task_id)
        return
    
    if data == "invite_settings_cancel":
        task_id = user_states.get(user_id, {}).get('editing_task_id')
        if not task_id:
            await callback_query.answer("❌ Ошибка: задача не найдена", show_alert=True)
            return
        
        # Clear editing state
        user_states[user_id].pop('editing_task_id', None)
        user_states[user_id].pop('editing_task_type', None)
        
        # Redirect to task details (pass task_id — callback.data is "invite_settings_cancel")
        await callback_query.answer("Отменено")
        await handle_invite_status(client, callback_query, task_id=task_id)
        return
    
    if data == "parse_settings_save":
        task_id = user_states.get(user_id, {}).get('editing_task_id')
        if not task_id:
            await callback_query.answer("❌ Ошибка: задача не найдена", show_alert=True)
            return
        
        settings = user_states[user_id].get('parse_settings', {})
        
        # Save settings via API
        result = await api_client.update_parse_task(
            task_id,
            delay_seconds=settings.get('delay_seconds'),
            limit=settings.get('limit'),
            save_every=settings.get('save_every'),
            rotate_sessions=settings.get('rotate_sessions'),
            rotate_every=settings.get('rotate_every'),
            use_proxy=settings.get('use_proxy'),
            available_sessions=settings.get('selected_sessions') or settings.get('available_sessions'),
            filter_admins=settings.get('filter_admins'),
            filter_inactive=settings.get('filter_inactive'),
            inactive_threshold_days=settings.get('inactive_threshold_days'),
            keyword_filter=settings.get('keyword_filter'),
            exclude_keywords=settings.get('exclude_keywords')
        )
        
        if not result.get('success'):
            await callback_query.answer("❌ Не удалось сохранить настройки", show_alert=True)
            return
        
        # Clear editing state
        user_states[user_id].pop('editing_task_id', None)
        user_states[user_id].pop('editing_task_type', None)
        
        # Redirect to task details (pass task_id — callback.data is "parse_settings_save")
        await callback_query.answer("✅ Настройки сохранены")
        await handle_parse_status(client, callback_query, task_id=task_id)
        return
    
    if data == "parse_settings_cancel":
        task_id = user_states.get(user_id, {}).get('editing_task_id')
        if not task_id:
            await callback_query.answer("❌ Ошибка: задача не найдена", show_alert=True)
            return
        
        # Clear editing state
        user_states[user_id].pop('editing_task_id', None)
        user_states[user_id].pop('editing_task_type', None)
        
        # Redirect to task details (pass task_id — callback.data is "parse_settings_cancel")
        await callback_query.answer("Отменено")
        await handle_parse_status(client, callback_query, task_id=task_id)
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
        selected = settings.get('selected_sessions') or settings.get('available_sessions') or []
        
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


async def handle_invite_pause(client: Client, callback_query):
    """Handle invite pause - same as stop but with clearer messaging."""
    task_id = int(callback_query.data.split(":")[1])
    
    result = await api_client.stop_task(task_id)
    
    if result.get('success'):
        task_data = await api_client.get_task(task_id)
        text = format_invite_status(task_data)
        
        await callback_query.edit_message_text(
            text,
            reply_markup=get_invite_paused_keyboard(task_id)
        )
        await callback_query.answer("⏸️ Задача приостановлена. Нажмите 'Продолжить' чтобы возобновить.")
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
    user_id = int(callback_query.from_user.id)
    
    result = await api_client.delete_task(task_id)
    
    if result.get('success'):
        await callback_query.answer("Задача удалена")
        # Show tasks status again with edit
        await show_tasks_status(client, callback_query.message, page=0, edit_message=True, user_id=user_id)
    else:
        await callback_query.answer(f"Ошибка: {result.get('error')}", show_alert=True)


async def handle_invite_status(client: Client, callback_query, task_id: int = None):
    """Show invite task details. If task is running, auto-pause it first. task_id can be passed when returning from edit (save/cancel)."""
    if task_id is None:
        parts = callback_query.data.split(":")
        if len(parts) < 2:
            await callback_query.answer("Ошибка: неверные данные", show_alert=True)
            return
        task_id = int(parts[1])
    
    task_data = await api_client.get_task(task_id)
    if not task_data.get('success'):
        await callback_query.answer(f"Ошибка: {task_data.get('error')}", show_alert=True)
        return
    
    text = format_invite_status(task_data)
    status = task_data.get('status', 'pending')
    kb = get_invite_running_keyboard(task_id) if status == 'running' else get_invite_paused_keyboard(task_id)
    await callback_query.edit_message_text(text, reply_markup=kb)
    await callback_query.answer()


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


async def handle_parse_pause(client: Client, callback_query):
    """Handle parse pause - same as stop but with clearer messaging."""
    task_id = int(callback_query.data.split(":")[1])
    
    result = await api_client.stop_parse_task(task_id)
    
    if result.get('success'):
        task_data = await api_client.get_parse_task(task_id)
        text = format_parse_status(task_data.get('task', {}))
        
        await callback_query.edit_message_text(
            text,
            reply_markup=get_parse_paused_keyboard(task_id)
        )
        await callback_query.answer("⏸️ Парсинг приостановлен. Нажмите 'Продолжить' чтобы возобновить.")
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
    user_id = int(callback_query.from_user.id)
    
    result = await api_client.delete_parse_task(task_id)
    
    if result.get('success'):
        await callback_query.answer("Задача удалена")
        await show_tasks_status(client, callback_query.message, edit_message=True, user_id=user_id)
    else:
        await callback_query.answer(f"Ошибка: {result.get('error')}", show_alert=True)


async def handle_parse_status(client: Client, callback_query, task_id: int = None):
    """Show parse task details. If task is running, auto-pause it first. task_id can be passed when returning from edit (save/cancel)."""
    if task_id is None:
        parts = callback_query.data.split(":")
        if len(parts) < 2:
            await callback_query.answer("Ошибка: неверные данные", show_alert=True)
            return
        task_id = int(parts[1])
    
    task_data = await api_client.get_parse_task(task_id)
    if not task_data.get('success'):
        await callback_query.answer(f"Ошибка: {task_data.get('error')}", show_alert=True)
        return
    
    task = task_data.get('task', {})
    
    text = format_parse_status(task)
    kb = get_parse_running_keyboard(task_id) if task.get('status') == 'running' else get_parse_paused_keyboard(task_id)
    await callback_query.edit_message_text(text, reply_markup=kb)
    await callback_query.answer()


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
    """Handle clearing all completed and failed tasks (both invite and parse)."""
    user_id = int(callback_query.from_user.id)
    
    # Answer callback query immediately to prevent timeout
    try:
        await callback_query.answer("⏳ Удаление задач...")
    except Exception:
        pass  # Ignore if query already expired
    
    deleted_count = 0
    errors = []
    
    # Get and clear invite tasks
    result = await api_client.get_user_tasks(user_id)
    invite_tasks = result.get('tasks', [])
    
    # Filter completed, failed and pending invite tasks
    completed_invite_tasks = [t for t in invite_tasks if t['status'] in ['completed', 'failed', 'pending']]
    
    for task in completed_invite_tasks:
        result = await api_client.delete_task(task['id'])
        if result.get('success'):
            deleted_count += 1
        else:
            errors.append(f"Инвайт {task['id']}: {result.get('error', 'Unknown error')}")
    
    # Get and clear parse tasks
    parse_result = await api_client.get_user_parse_tasks(user_id)
    if parse_result.get('success'):
        parse_tasks = parse_result.get('tasks', [])
        
        # Filter completed, failed and pending parse tasks
        completed_parse_tasks = [t for t in parse_tasks if t.get('status') in ['completed', 'failed', 'pending']]
        
        for task in completed_parse_tasks:
            result = await api_client.delete_parse_task(task['id'])
            if result.get('success'):
                deleted_count += 1
            else:
                errors.append(f"Парс {task['id']}: {result.get('error', 'Unknown error')}")
    
    # Get and clear post parse tasks
    post_parse_result = await api_client.get_user_post_parse_tasks(user_id)
    if post_parse_result.get('success'):
        post_parse_tasks = post_parse_result.get('tasks', [])
        
        # Filter completed, failed and pending post parse tasks
        completed_post_parse_tasks = [t for t in post_parse_tasks if t.get('status') in ['completed', 'failed', 'pending']]
        
        for task in completed_post_parse_tasks:
            result = await api_client.delete_post_parse_task(task['id'])
            if result.get('success'):
                deleted_count += 1
            else:
                errors.append(f"Парс постов {task['id']}: {result.get('error', 'Unknown error')}")
    
    # Get and clear post monitoring tasks
    post_monitor_result = await api_client.get_user_post_monitoring_tasks(user_id)
    if post_monitor_result.get('success'):
        post_monitor_tasks = post_monitor_result.get('tasks', [])
        
        # Filter completed, failed and pending post monitoring tasks
        completed_post_monitor_tasks = [t for t in post_monitor_tasks if t.get('status') in ['completed', 'failed', 'pending']]
        
        for task in completed_post_monitor_tasks:
            result = await api_client.delete_post_monitoring_task(task['id'])
            if result.get('success'):
                deleted_count += 1
            else:
                errors.append(f"Мониторинг {task['id']}: {result.get('error', 'Unknown error')}")
    
    # Refresh tasks list
    try:
        await show_tasks_status(client, callback_query.message, page=0, edit_message=True, user_id=user_id)
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


async def handle_clear_all_tasks(client: Client, callback_query):
    """Handle clearing ALL tasks (both invite and parse, any status)."""
    user_id = int(callback_query.from_user.id)
    
    # Answer callback query immediately to prevent timeout
    try:
        await callback_query.answer("⏳ Удаление всех задач...")
    except Exception:
        pass  # Ignore if query already expired
    
    deleted_count = 0
    errors = []
    
    # Get and clear ALL invite tasks
    result = await api_client.get_user_tasks(user_id)
    invite_tasks = result.get('tasks', [])
    
    for task in invite_tasks:
        # First stop the task if running
        if task['status'] == 'running':
            await api_client.stop_task(task['id'])
        
        result = await api_client.delete_task(task['id'])
        if result.get('success'):
            deleted_count += 1
        else:
            errors.append(f"Инвайт {task['id']}: {result.get('error', 'Unknown error')}")
    
    # Get and clear ALL parse tasks
    parse_result = await api_client.get_user_parse_tasks(user_id)
    if parse_result.get('success'):
        parse_tasks = parse_result.get('tasks', [])
        
        for task in parse_tasks:
            # First stop the task if running
            if task.get('status') == 'running':
                await api_client.stop_parse_task(task['id'])
            
            result = await api_client.delete_parse_task(task['id'])
            if result.get('success'):
                deleted_count += 1
            else:
                errors.append(f"Парс {task['id']}: {result.get('error', 'Unknown error')}")
    
    # Get and clear ALL post parse tasks
    post_parse_result = await api_client.get_user_post_parse_tasks(user_id)
    if post_parse_result.get('success'):
        post_parse_tasks = post_parse_result.get('tasks', [])
        
        for task in post_parse_tasks:
            # First stop the task if running
            if task.get('status') == 'running':
                await api_client.stop_post_parse_task(task['id'])
            
            result = await api_client.delete_post_parse_task(task['id'])
            if result.get('success'):
                deleted_count += 1
            else:
                errors.append(f"Парс постов {task['id']}: {result.get('error', 'Unknown error')}")
    
    # Get and clear ALL post monitoring tasks
    post_monitor_result = await api_client.get_user_post_monitoring_tasks(user_id)
    if post_monitor_result.get('success'):
        post_monitor_tasks = post_monitor_result.get('tasks', [])
        
        for task in post_monitor_tasks:
            # First stop the task if running
            if task.get('status') == 'running':
                await api_client.stop_post_monitoring_task(task['id'])
            
            result = await api_client.delete_post_monitoring_task(task['id'])
            if result.get('success'):
                deleted_count += 1
            else:
                errors.append(f"Мониторинг {task['id']}: {result.get('error', 'Unknown error')}")
    
    # Refresh tasks list
    try:
        await show_tasks_status(client, callback_query.message, page=0, edit_message=True, user_id=user_id)
    except Exception as e:
        logger.error(f"Error refreshing tasks list after clear all: {e}")
        try:
            if deleted_count > 0:
                await callback_query.answer(f"✅ Удалено всех задач: {deleted_count}", show_alert=True)
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
    """Handle session selection for inviting (from create or from task edit)."""
    user_id = int(callback_query.from_user.id)
    settings = user_states.get(user_id, {}).get('invite_settings', {})
    selected = settings.get('selected_sessions') or settings.get('available_sessions') or []
    
    keyboard = await get_session_select_keyboard(selected)
    
    await callback_query.edit_message_text(
        "🔐 **Выбор сессий для инвайтинга**\n\n"
        "Выберите сессии, которые будут использоваться при ротации:",
        reply_markup=keyboard
    )


async def handle_toggle_session(client: Client, callback_query):
    """Handle session toggle in selection (invite, parse or PP/PM edit)."""
    user_id = int(callback_query.from_user.id)
    session_alias = callback_query.data.split(":")[1]
    
    state = user_states.get(user_id, {}).get('state')
    
    if state == FSM_PARSE_SESSION_SELECT:
        settings = user_states.get(user_id, {}).get('parse_settings', {})
        done_cb, back_cb = "sessions_done", "sessions_back"
    elif state == FSM_PP_EDIT_SESSION_SELECT:
        settings = user_states.get(user_id, {}).get('post_forward_settings', {})
        task_id = user_states.get(user_id, {}).get('editing_task_id')
        edit_type = user_states.get(user_id, {}).get('editing_task_type', '')
        prefix = "pp_edit_sessions" if edit_type == 'post_parse' else "pm_edit_sessions"
        done_cb = f"{prefix}_done:{task_id}" if task_id else "sessions_done"
        back_cb = f"{prefix}_back:{task_id}" if task_id else "sessions_back"
    else:
        settings = user_states.get(user_id, {}).get('invite_settings', {})
        done_cb, back_cb = "sessions_done", "sessions_back"
    
    selected = settings.get('selected_sessions', [])
    
    if session_alias in selected:
        selected.remove(session_alias)
    else:
        selected.append(session_alias)
    
    settings['selected_sessions'] = selected
    
    if state == FSM_PARSE_SESSION_SELECT:
        user_states[user_id]['parse_settings'] = settings
    elif state == FSM_PP_EDIT_SESSION_SELECT:
        user_states[user_id]['post_forward_settings'] = settings
    else:
        user_states[user_id]['invite_settings'] = settings
    
    keyboard = await get_session_select_keyboard(selected, done_callback=done_cb, back_callback=back_cb)
    
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
        # From button — restore full title from history (button text is truncated)
        group_id = group_data['id']
        full_title, full_username = await get_full_group_title_from_history(user_id, group_id, is_target=False)
        group_title = full_title or group_data['title']
        username = full_username if full_username is not None else group_data.get('username')
    else:
        normalized = normalize_group_input(text)
        
        # Get sessions
        sessions_result = await api_client.list_sessions()
        sessions = sessions_result.get('sessions', [])
        assignments = sessions_result.get('assignments', {})
        
        # Resolve group with rotation
        group_info, last_error, failed_sessions = await resolve_group_with_rotation(normalized, sessions, assignments, 'parsing')
        
        if not group_info:
            # All sessions failed - show detailed error
            error_details = "\n".join(failed_sessions) if failed_sessions else "Неизвестная ошибка"
            error_msg = format_session_error_message(last_error)
            
            await message.reply(
                f"{error_msg}\n\n"
                f"**Проблемы с сессиями ({len(failed_sessions)}):**\n"
                f"{error_details}\n\n"
                "Проверьте настройки сессий или попробуйте другую группу."
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
        # From button — restore full title from history (button text is truncated)
        group_id = group_data['id']
        full_title, full_username = await get_full_group_title_from_history(user_id, group_id, is_target=True)
        group_title = full_title or group_data['title']
        username = full_username if full_username is not None else group_data.get('username')
    else:
        normalized = normalize_group_input(text)
        
        # Get available sessions for rotation
        sessions_result = await api_client.list_sessions()
        sessions = sessions_result.get('sessions', [])
        assignments = sessions_result.get('assignments', {})
        
        # Resolve group with rotation
        group_info, last_error, failed_sessions = await resolve_group_with_rotation(normalized, sessions, assignments, 'inviting')
        
        if not group_info:
            # All sessions failed - show detailed error
            error_details = "\n".join(failed_sessions) if failed_sessions else "Неизвестная ошибка"
            error_msg = format_session_error_message(last_error)
            
            await message.reply(
                f"{error_msg}\n\n"
                f"**Проблемы с сессиями ({len(failed_sessions)}):**\n"
                f"{error_details}\n\n"
                "Проверьте настройки сессий или попробуйте другую группу."
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


# ============== Post Forwarding Handlers ==============

async def start_post_forward_flow(client: Client, message: Message):
    """Start the post forwarding flow - show main menu for post parsing/monitoring."""
    user_id = message.from_user.id
    
    # Check for sessions first
    result = await api_client.list_sessions()
    sessions = result.get('sessions', [])
    
    if not sessions:
        await message.reply(
            "⚠️ **Нет активных сессий!**\n\n"
            "Для пересылки постов необходимо добавить хотя бы одну сессию.\n"
            "Перейдите в меню 🔐 **Сессии** -> **Добавить сессию**.",
            reply_markup=get_main_keyboard()
        )
        return
    
    user_states[user_id] = {
        'state': FSM_POST_FORWARD_MODE_SELECT,
        'post_forward_settings': {
            'delay_seconds': 2,
            'delay_every': 1,
            'limit': None,
            'rotate_sessions': False,
            'rotate_every': 0,
            'use_proxy': True,
            'filter_contacts': False,
            'remove_contacts': False,
            'skip_on_contacts': False,
            'parse_direction': 'backward',
            'media_filter': 'all',
            'add_signature': False,
            'signature_options': None,
            'selected_sessions': []
        }
    }
    
    await message.reply(
        "📨 **Пересылка постов**\n\n"
        "Выберите режим работы:\n\n"
        "📥 **Парсинг постов** - парсинг исторических постов из канала/группы\n"
        "🔄 **Мониторинг постов** - пересылка в реал-тайме новых постов",
        reply_markup=get_post_forward_main_keyboard()
    )


async def handle_post_forward_callback(client: Client, callback_query):
    """Handle callbacks for post forwarding flow."""
    user_id = callback_query.from_user.id
    data = callback_query.data
    
    if user_id not in user_states:
        user_states[user_id] = {}
    
    # Выбор сессий при редактировании PP/PM из статуса задач
    if data.startswith("pp_settings_sessions:"):
        task_id = int(data.split(":")[1])
        user_states[user_id]['state'] = FSM_PP_EDIT_SESSION_SELECT
        settings = user_states.get(user_id, {}).get('post_forward_settings', {})
        selected = settings.get('selected_sessions', [])
        kb = await get_session_select_keyboard(
            selected,
            done_callback=f"pp_edit_sessions_done:{task_id}",
            back_callback=f"pp_edit_sessions_back:{task_id}",
        )
        await callback_query.message.edit_text(
            "🔐 **Выбор сессий для парсинга постов**\n\n"
            "Выберите сессии для ротации (текущие уже отмечены):",
            reply_markup=kb
        )
        await safe_answer_callback(callback_query)
        return True
    
    if data.startswith("pm_settings_sessions:"):
        task_id = int(data.split(":")[1])
        user_states[user_id]['state'] = FSM_PP_EDIT_SESSION_SELECT
        settings = user_states.get(user_id, {}).get('post_forward_settings', {})
        selected = settings.get('selected_sessions', [])
        kb = await get_session_select_keyboard(
            selected,
            done_callback=f"pm_edit_sessions_done:{task_id}",
            back_callback=f"pm_edit_sessions_back:{task_id}",
        )
        await callback_query.message.edit_text(
            "🔐 **Выбор сессий для мониторинга постов**\n\n"
            "Выберите сессии для ротации (текущие уже отмечены):",
            reply_markup=kb
        )
        await safe_answer_callback(callback_query)
        return True
    
    async def _redraw_pp_edit_screen(cq, tid, mode_parse=True):
        """Вернуть экран редактирования задачи PP/PM."""
        st = user_states.get(user_id, {})
        settings = st.get('post_forward_settings', {})
        source = st.get('pf_source', {})
        target = st.get('pf_target', {})
        sc = len(settings.get('selected_sessions', [])) or None
        mode = "parse" if mode_parse else "monitor"
        msg = get_post_forward_settings_message_text(mode, source, target, settings, sc)
        msg = msg.rstrip() + "\n\n**Редактирование:** Сохранить — вернуться к деталям задачи. Запустить заново — сбросить прогресс и запустить с новыми настройками."
        kb = get_post_forward_settings_keyboard(settings, mode=mode, edit_mode=True, task_id=tid)
        await cq.message.edit_text(msg, reply_markup=kb)
    
    if data.startswith("pp_edit_sessions_done:"):
        task_id = int(data.split(":")[1])
        settings = user_states.get(user_id, {}).get('post_forward_settings', {})
        selected = settings.get('selected_sessions', [])
        if not selected:
            await safe_answer_callback(callback_query, "⚠️ Выберите хотя бы одну сессию!", show_alert=True)
            return True
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
        await _redraw_pp_edit_screen(callback_query, task_id, mode_parse=True)
        await safe_answer_callback(callback_query, "Сессии сохранены")
        return True

    if data.startswith("pp_edit_sessions_back:"):
        task_id = int(data.split(":")[1])
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
        await _redraw_pp_edit_screen(callback_query, task_id, mode_parse=True)
        await safe_answer_callback(callback_query)
        return True

    if data.startswith("pm_edit_sessions_done:"):
        task_id = int(data.split(":")[1])
        settings = user_states.get(user_id, {}).get('post_forward_settings', {})
        selected = settings.get('selected_sessions', [])
        if not selected:
            await safe_answer_callback(callback_query, "⚠️ Выберите хотя бы одну сессию!", show_alert=True)
            return True
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
        await _redraw_pp_edit_screen(callback_query, task_id, mode_parse=False)
        await safe_answer_callback(callback_query, "Сессии сохранены")
        return True

    if data.startswith("pm_edit_sessions_back:"):
        task_id = int(data.split(":")[1])
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
        await _redraw_pp_edit_screen(callback_query, task_id, mode_parse=False)
        await safe_answer_callback(callback_query)
        return True
    
    # Handle main menu buttons
    if data == "post_parse_start":
        user_states[user_id]['post_forward_mode'] = 'parse'
        user_states[user_id]['state'] = FSM_POST_FORWARD_SESSION_SELECT
        user_states[user_id]['post_forward_settings'] = user_states[user_id].get('post_forward_settings', {})
        
        # Get assigned sessions for post_parsing task
        sessions_result = await api_client.list_sessions()
        assignments = sessions_result.get('assignments', {})
        assigned_sessions = assignments.get('post_parsing', [])
        
        # Pre-select assigned sessions
        user_states[user_id]['post_forward_settings']['selected_sessions'] = assigned_sessions.copy() if assigned_sessions else []
        
        preselect_info = ""
        if assigned_sessions:
            preselect_info = f"\n✅ Предвыбрано из назначений: {len(assigned_sessions)} сессий\n"
        
        keyboard = await get_post_forward_session_keyboard(assigned_sessions, sessions_result.get('sessions', []))
        await callback_query.message.edit_text(
            "📥 **Парсинг постов**\n\n"
            "🔐 **Шаг 1: Выберите сессии**\n"
            f"{preselect_info}\n"
            "Выберите одну или несколько сессий для выполнения задачи.\n"
            "Сессии будут использоваться для доступа к каналам/группам.\n\n"
            "🟢 - активная сессия\n"
            "🔴 - неактивная сессия\n"
            "🟡 - статус неизвестен",
            reply_markup=keyboard
        )
        await safe_answer_callback(callback_query)
        return True
    
    if data == "post_monitor_start":
        user_states[user_id]['post_forward_mode'] = 'monitor'
        user_states[user_id]['state'] = FSM_POST_FORWARD_SESSION_SELECT
        user_states[user_id]['post_forward_settings'] = user_states[user_id].get('post_forward_settings', {})
        
        # Get assigned sessions for post_monitoring task
        sessions_result = await api_client.list_sessions()
        assignments = sessions_result.get('assignments', {})
        assigned_sessions = assignments.get('post_monitoring', [])
        
        # Pre-select assigned sessions
        user_states[user_id]['post_forward_settings']['selected_sessions'] = assigned_sessions.copy() if assigned_sessions else []
        
        preselect_info = ""
        if assigned_sessions:
            preselect_info = f"\n✅ Предвыбрано из назначений: {len(assigned_sessions)} сессий\n"
        
        keyboard = await get_post_forward_session_keyboard(assigned_sessions, sessions_result.get('sessions', []))
        await callback_query.message.edit_text(
            "🔄 **Мониторинг постов**\n\n"
            "🔐 **Шаг 1: Выберите сессии**\n"
            f"{preselect_info}\n"
            "Выберите одну или несколько сессий для выполнения задачи.\n"
            "Сессии будут использоваться для доступа к каналам/группам.\n\n"
            "🟢 - активная сессия\n"
            "🔴 - неактивная сессия\n"
            "🟡 - статус неизвестен",
            reply_markup=keyboard
        )
        await safe_answer_callback(callback_query)
        return True
    
    if data == "post_forward_tasks":
        await show_post_forward_tasks(client, callback_query)
        return True
    
    if data == "post_forward_back":
        await callback_query.message.delete()
        await show_main_menu(client, callback_query.message, "🏠 **Главное меню**\n\nВыберите действие:")
        await safe_answer_callback(callback_query)
        return True
    
    # ============== Session Selection for Post Forwarding ==============
    
    if data.startswith("pf_toggle_session:"):
        session_alias = data.split(":")[1]
        settings = user_states[user_id].get('post_forward_settings', {})
        selected = settings.get('selected_sessions', [])
        
        if session_alias in selected:
            selected.remove(session_alias)
        else:
            selected.append(session_alias)
        
        settings['selected_sessions'] = selected
        user_states[user_id]['post_forward_settings'] = settings
        
        keyboard = await get_post_forward_session_keyboard(selected)
        await callback_query.message.edit_reply_markup(reply_markup=keyboard)
        await safe_answer_callback(callback_query, f"Сессия {'выбрана' if session_alias in selected else 'отменена'}")
        return True
    
    if data == "pf_sessions_info":
        settings = user_states[user_id].get('post_forward_settings', {})
        selected = settings.get('selected_sessions', [])
        if not selected:
            await safe_answer_callback(callback_query, "Выберите хотя бы одну сессию для продолжения", show_alert=True)
        else:
            await safe_answer_callback(callback_query, f"Выбрано сессий: {len(selected)}")
        return True
    
    if data == "pf_no_sessions":
        await safe_answer_callback(callback_query, "Добавьте сессии в меню 🔐 Сессии", show_alert=True)
        return True
    
    if data == "pf_sessions_done":
        settings = user_states[user_id].get('post_forward_settings', {})
        selected = settings.get('selected_sessions', [])
        
        if not selected:
            await safe_answer_callback(callback_query, "⚠️ Выберите хотя бы одну сессию!", show_alert=True)
            return True
        
        # Move to source type selection
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        mode_name = "Парсинг постов" if mode == 'parse' else "Мониторинг постов"
        mode_icon = "📥" if mode == 'parse' else "🔄"
        
        user_states[user_id]['state'] = FSM_POST_FORWARD_SOURCE_TYPE
        await callback_query.message.edit_text(
            f"{mode_icon} **{mode_name}**\n\n"
            f"✅ Выбрано сессий: {len(selected)}\n\n"
            "📤 **Шаг 2: Выберите тип источника**",
            reply_markup=get_post_forward_source_type_keyboard()
        )
        await safe_answer_callback(callback_query)
        return True
    
    if data == "pf_sessions_back":
        user_states[user_id]['state'] = FSM_POST_FORWARD_MODE_SELECT
        await callback_query.message.edit_text(
            "📨 **Пересылка постов**\n\n"
            "Выберите режим работы:\n\n"
            "📥 **Парсинг постов** - парсинг исторических постов из канала/группы\n"
            "🔄 **Мониторинг постов** - пересылка в реал-тайме новых постов",
            reply_markup=get_post_forward_main_keyboard()
        )
        await safe_answer_callback(callback_query)
        return True
    
    # Handle source type selection
    if data.startswith("pf_source_type:"):
        source_type = data.split(":")[1]
        user_states[user_id]['pf_source_type'] = source_type
        user_states[user_id]['state'] = FSM_POST_FORWARD_SOURCE
        
        type_name = "канала" if source_type == "channel" else "группы"
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        mode_name = "Парсинг постов" if mode == 'parse' else "Мониторинг постов"
        mode_icon = "📥" if mode == 'parse' else "🔄"
        
        kb = await get_parse_source_group_history_keyboard(user_id)
        await callback_query.message.reply(
            f"{mode_icon} **{mode_name}**\n\n"
            f"📤 **Шаг 3: Введите ссылку или ID {type_name}-источника:**\n\n"
            "Выберите из списка или введите вручную:\n"
            "• @channel_username\n"
            "• https://t.me/channel_username\n"
            "• -1001234567890",
            reply_markup=kb or ReplyKeyboardRemove()
        )
        await safe_answer_callback(callback_query)
        return True
    
    # Handle target type selection
    if data.startswith("pf_target_type:"):
        target_type = data.split(":")[1]
        user_states[user_id]['pf_target_type'] = target_type
        user_states[user_id]['state'] = FSM_POST_FORWARD_TARGET
        
        type_name = "канала" if target_type == "channel" else "группы"
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        mode_name = "Парсинг постов" if mode == 'parse' else "Мониторинг постов"
        mode_icon = "📥" if mode == 'parse' else "🔄"
        
        kb = await get_target_group_history_keyboard(user_id)
        await callback_query.message.reply(
            f"{mode_icon} **{mode_name}**\n\n"
            f"📥 **Шаг 5: Введите ссылку или ID {type_name}-цели:**\n\n"
            "Выберите из списка или введите вручную:\n"
            "• @channel_username\n"
            "• https://t.me/channel_username\n"
            "• -1001234567890",
            reply_markup=kb or ReplyKeyboardRemove()
        )
        await safe_answer_callback(callback_query)
        return True
    
    # Handle back
    if data == "pf_back":
        user_states[user_id]['state'] = FSM_POST_FORWARD_MODE_SELECT
        await callback_query.message.edit_text(
            "📨 **Пересылка постов**\n\n"
            "Выберите режим работы:",
            reply_markup=get_post_forward_main_keyboard()
        )
        await safe_answer_callback(callback_query)
        return True
    
    # Handle settings toggles
    if data == "pf_settings_rotate":
        settings = user_states[user_id].get('post_forward_settings', {})
        settings['rotate_sessions'] = not settings.get('rotate_sessions', False)
        user_states[user_id]['post_forward_settings'] = settings
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        source = user_states[user_id].get('pf_source', {})
        target = user_states[user_id].get('pf_target', {})
        sc = len(settings.get('selected_sessions', [])) or None
        await callback_query.message.edit_text(
            get_post_forward_settings_message_text(mode, source, target, settings, sc),
            reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=bool(user_states[user_id].get('editing_task_id')), task_id=user_states[user_id].get('editing_task_id'))
        )
        await safe_answer_callback(callback_query, "Ротация сессий изменена")
        return True
    
    if data == "pf_settings_proxy":
        settings = user_states[user_id].get('post_forward_settings', {})
        settings['use_proxy'] = not settings.get('use_proxy', True)
        user_states[user_id]['post_forward_settings'] = settings
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        source = user_states[user_id].get('pf_source', {})
        target = user_states[user_id].get('pf_target', {})
        sc = len(settings.get('selected_sessions', [])) or None
        await callback_query.message.edit_text(
            get_post_forward_settings_message_text(mode, source, target, settings, sc),
            reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=bool(user_states[user_id].get('editing_task_id')), task_id=user_states[user_id].get('editing_task_id'))
        )
        await safe_answer_callback(callback_query, "Настройка прокси изменена")
        return True
    
    if data == "pf_settings_contact_action":
        settings = user_states[user_id].get('post_forward_settings', {})
        
        # Determine current mode
        skip_on_contacts = settings.get('skip_on_contacts', False)
        remove_contacts = settings.get('remove_contacts', False)
        
        if settings.get('use_native_forward', False):
            # Native Mode: Toggle between Ignore and Skip
            if skip_on_contacts:
                # Skip -> Ignore
                settings['skip_on_contacts'] = False
                mode_text = "Игнорировать"
            else:
                # Ignore -> Skip
                settings['skip_on_contacts'] = True
                mode_text = "Пропускать"
            
            # Ensure edit related flags are OFF
            settings['remove_contacts'] = False
            settings['filter_contacts'] = False
            
        else:
            # Normal Mode: Cycle ignore -> edit -> skip -> ignore
            if skip_on_contacts:
                # skip -> ignore
                settings['skip_on_contacts'] = False
                settings['remove_contacts'] = False
                settings['filter_contacts'] = False
                mode_text = "Игнорировать"
            elif remove_contacts:
                # edit -> skip
                settings['skip_on_contacts'] = True
                settings['remove_contacts'] = False
                settings['filter_contacts'] = False
                mode_text = "Пропускать"
            else:
                # ignore -> edit
                settings['skip_on_contacts'] = False
                settings['remove_contacts'] = True
                settings['filter_contacts'] = False
                mode_text = "Редактировать"
        
        user_states[user_id]['post_forward_settings'] = settings
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        source = user_states[user_id].get('pf_source', {})
        target = user_states[user_id].get('pf_target', {})
        sc = len(settings.get('selected_sessions', [])) or None
        await callback_query.message.edit_text(
            get_post_forward_settings_message_text(mode, source, target, settings, sc),
            reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=bool(user_states[user_id].get('editing_task_id')), task_id=user_states[user_id].get('editing_task_id'))
        )
        await safe_answer_callback(callback_query, f"При контактах: {mode_text}")
        return True
    
    if data == "pf_settings_direction":
        settings = user_states[user_id].get('post_forward_settings', {})
        current = settings.get('parse_direction', 'backward')
        settings['parse_direction'] = 'forward' if current == 'backward' else 'backward'
        user_states[user_id]['post_forward_settings'] = settings
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        source = user_states[user_id].get('pf_source', {})
        target = user_states[user_id].get('pf_target', {})
        sc = len(settings.get('selected_sessions', [])) or None
        await callback_query.message.edit_text(
            get_post_forward_settings_message_text(mode, source, target, settings, sc),
            reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=bool(user_states[user_id].get('editing_task_id')), task_id=user_states[user_id].get('editing_task_id'))
        )
        await safe_answer_callback(callback_query, "Направление парсинга изменено")
        return True
    
    if data == "pf_settings_media_filter":
        settings = user_states[user_id].get('post_forward_settings', {})
        
        if settings.get('use_native_forward', False):
            await safe_answer_callback(callback_query, "⚠️ Недоступно при нативной пересылке", show_alert=True)
            return True
            
        current = settings.get('media_filter', 'all')
        filters = ['all', 'media_only', 'text_only']
        idx = (filters.index(current) + 1) % len(filters)
        settings['media_filter'] = filters[idx]
        settings['use_native_forward'] = False  # mutually exclusive with media filter
        user_states[user_id]['post_forward_settings'] = settings
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        source = user_states[user_id].get('pf_source', {})
        target = user_states[user_id].get('pf_target', {})
        sc = len(settings.get('selected_sessions', [])) or None
        await callback_query.message.edit_text(
            get_post_forward_settings_message_text(mode, source, target, settings, sc),
            reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=bool(user_states[user_id].get('editing_task_id')), task_id=user_states[user_id].get('editing_task_id'))
        )
        await safe_answer_callback(callback_query, "Фильтр медиа изменен")
        return True
    
    if data == "pf_settings_whitelist":
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS_KEYWORDS_WHITELIST
        await callback_query.message.edit_text(
            "✅ **Включая слова (Whitelist)**\n\n"
            "Введите слова через запятую, которые **должны** присутствовать в посте.\n"
            "Если пост не содержит ни одного из этих слов, он будет пропущен.\n\n"
            "Пример: `скидка, акция, распродажа`\n"
            "Отправьте `нет` или `-` чтобы очистить список."
        )
        await safe_answer_callback(callback_query)
        return True

    if data == "pf_settings_blacklist":
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS_KEYWORDS_BLACKLIST
        await callback_query.message.edit_text(
            "🚫 **Исключая слова (Blacklist)**\n\n"
            "Введите слова через запятую, которых **не должно** быть в посте.\n"
            "Если пост содержит хотя бы одно из этих слов, он будет пропущен.\n\n"
            "Пример: `реклама, казино, ставки`\n"
            "Отправьте `нет` или `-` чтобы очистить список."
        )
        await safe_answer_callback(callback_query)
        return True
    
    if data == "pf_settings_limit":
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS_LIMIT
        await callback_query.message.edit_text(
            "🔢 **Лимит постов**\n\n"
            "Введите максимальное количество постов для пересылки\n"
            "(или 0 для снятия лимита):"
        )
        await safe_answer_callback(callback_query)
        return True
    
    if data == "pf_settings_delay":
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS_DELAY
        await callback_query.message.edit_text(
            "⏱️ **Задержка**\n\n"
            "Введите задержку между пересылками (в секундах):"
        )
        await safe_answer_callback(callback_query)
        return True
    
    if data == "pf_settings_delay_every":
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS_DELAY_EVERY
        await callback_query.message.edit_text(
            "🔢 **Задержка каждые N постов**\n\n"
            "Введите через сколько постов применять задержку:"
        )
        await safe_answer_callback(callback_query)
        return True
    
    if data == "pf_settings_rotate_every":
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS_ROTATE_EVERY
        await callback_query.message.edit_text(
            "🔄 **Ротация каждые N постов**\n\n"
            "Введите через сколько постов менять сессию\n"
            "(0 = только при ошибках):"
        )
        await safe_answer_callback(callback_query)
        return True
    
    if data == "pf_native_toggle":
        settings = user_states[user_id].get('post_forward_settings', {})
        current = settings.get('use_native_forward', False)
        settings['use_native_forward'] = not current
        
        if not current: # Enabling Native
            # Preserve Skip if enabled, otherwise default to Ignore
            # But Force Edit (remove_contacts) to False -> Ignore
            if settings.get('remove_contacts', False):
                 settings['skip_on_contacts'] = False # Edit -> Ignore
            
            settings['filter_contacts'] = False
            settings['remove_contacts'] = False
            # settings['skip_on_contacts'] remains as is (True or False)
            
            settings['media_filter'] = 'all'
            settings['forward_show_source'] = True # Force Source ON
            
        user_states[user_id]['post_forward_settings'] = settings
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        source = user_states[user_id].get('pf_source', {})
        target = user_states[user_id].get('pf_target', {})
        sc = len(settings.get('selected_sessions', [])) or None
        await callback_query.message.edit_text(
            get_post_forward_settings_message_text(mode, source, target, settings, sc),
            reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=bool(user_states[user_id].get('editing_task_id')), task_id=user_states[user_id].get('editing_task_id'))
        )
        msg_text = "Включена" if not current else "Выключена"
        await safe_answer_callback(callback_query, f"Нативная пересылка: {msg_text}")
        return True

    if data == "pf_native_check":
        settings = user_states[user_id].get('post_forward_settings', {})
        settings['check_content_if_native'] = not settings.get('check_content_if_native', True)
        user_states[user_id]['post_forward_settings'] = settings
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        source = user_states[user_id].get('pf_source', {})
        target = user_states[user_id].get('pf_target', {})
        sc = len(settings.get('selected_sessions', [])) or None
        await callback_query.message.edit_text(
            get_post_forward_settings_message_text(mode, source, target, settings, sc),
            reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=bool(user_states[user_id].get('editing_task_id')), task_id=user_states[user_id].get('editing_task_id'))
        )
        await safe_answer_callback(callback_query, "Настройка изменена")
        return True

    if data == "pf_settings_signature":
        settings = user_states[user_id].get('post_forward_settings', {})
        if settings.get('use_native_forward', False):
            await safe_answer_callback(callback_query, "⚠️ Недоступно при нативной пересылке", show_alert=True)
            return True
            
        settings['add_signature'] = not settings.get('add_signature', False)
        if settings['add_signature'] and not settings.get('signature_options'):
            settings['signature_options'] = get_default_signature_options()
        user_states[user_id]['post_forward_settings'] = settings
        
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        source = user_states[user_id].get('pf_source', {})
        target = user_states[user_id].get('pf_target', {})
        sc = len(settings.get('selected_sessions', [])) or None
        
        await callback_query.message.edit_text(
            get_post_forward_settings_message_text(mode, source, target, settings, sc),
            reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=bool(user_states[user_id].get('editing_task_id')), task_id=user_states[user_id].get('editing_task_id'))
        )
        msg_text = "Включена" if settings['add_signature'] else "Выключена"
        await safe_answer_callback(callback_query, f"Подпись: {msg_text}")
        return True

    if data == "pf_signature_menu":
        settings = user_states[user_id].get('post_forward_settings', {})
        if not settings.get('signature_options'):
            settings['signature_options'] = get_default_signature_options()
            user_states[user_id]['post_forward_settings'] = settings
        await callback_query.message.edit_text(
            get_signature_options_message_text(settings),
            reply_markup=get_signature_options_keyboard(settings)
        )
        await safe_answer_callback(callback_query)
        return True

    if data == "pf_sig_include_post":
        settings = user_states[user_id].get('post_forward_settings', {})
        opts = settings.get('signature_options') or get_default_signature_options()
        opts['include_post'] = not opts.get('include_post', True)
        settings['signature_options'] = opts
        user_states[user_id]['post_forward_settings'] = settings
        await callback_query.message.edit_text(
            get_signature_options_message_text(settings),
            reply_markup=get_signature_options_keyboard(settings)
        )
        await safe_answer_callback(callback_query, "Ссылка на пост" + (" вкл." if opts['include_post'] else " выкл."))
        return True

    if data == "pf_sig_include_source":
        settings = user_states[user_id].get('post_forward_settings', {})
        opts = settings.get('signature_options') or get_default_signature_options()
        opts['include_source'] = not opts.get('include_source', False)
        settings['signature_options'] = opts
        user_states[user_id]['post_forward_settings'] = settings
        await callback_query.message.edit_text(
            get_signature_options_message_text(settings),
            reply_markup=get_signature_options_keyboard(settings)
        )
        await safe_answer_callback(callback_query, "Ссылка на источник" + (" вкл." if opts['include_source'] else " выкл."))
        return True

    if data == "pf_sig_include_author":
        settings = user_states[user_id].get('post_forward_settings', {})
        opts = settings.get('signature_options') or get_default_signature_options()
        opts['include_author'] = not opts.get('include_author', True)
        settings['signature_options'] = opts
        user_states[user_id]['post_forward_settings'] = settings
        await callback_query.message.edit_text(
            get_signature_options_message_text(settings),
            reply_markup=get_signature_options_keyboard(settings)
        )
        await safe_answer_callback(callback_query, "Ссылка на автора" + (" вкл." if opts['include_author'] else " выкл."))
        return True

    if data == "pf_sig_label_post":
        user_states[user_id]['state'] = FSM_POST_FORWARD_SIGNATURE_LABEL_POST
        await callback_query.message.edit_text(
            "🏷 **Текст для ссылки на пост**\n\n"
            "Введите текст, который будет отображаться перед прямой ссылкой на сообщение.\n"
            "Например: `Ссылка на пост` или `Оригинал`.\n\n"
            "Отправьте новое значение текстом."
        )
        await safe_answer_callback(callback_query)
        return True

    if data == "pf_sig_label_source":
        user_states[user_id]['state'] = FSM_POST_FORWARD_SIGNATURE_LABEL_SOURCE
        await callback_query.message.edit_text(
            "🏷 **Текст для источника (канал)**\n\n"
            "Введите текст перед ссылкой на канал/группу.\n"
            "Например: `Источник` или `Канал`.\n\n"
            "Отправьте новое значение текстом."
        )
        await safe_answer_callback(callback_query)
        return True

    if data == "pf_sig_label_author":
        user_states[user_id]['state'] = FSM_POST_FORWARD_SIGNATURE_LABEL_AUTHOR
        await callback_query.message.edit_text(
            "🏷 **Текст для автора**\n\n"
            "Введите текст перед ссылкой на автора поста.\n"
            "Например: `Обращаться по объявлению сюда:`.\n\n"
            "Отправьте новое значение текстом."
        )
        await safe_answer_callback(callback_query)
        return True

    if data == "pf_sig_done":
        settings = user_states[user_id].get('post_forward_settings', {})
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        source = user_states[user_id].get('pf_source', {})
        target = user_states[user_id].get('pf_target', {})
        sc = len(settings.get('selected_sessions', [])) or None
        edit_task_id = user_states[user_id].get('editing_task_id')
        edit_mode = bool(edit_task_id)
        msg_text = get_post_forward_settings_message_text(mode, source, target, settings, sc)
        if edit_mode:
            msg_text = msg_text.rstrip()
            msg_text += "\n\n**Редактирование:** Сохранить — вернуться к деталям задачи. Запустить заново — сбросить прогресс и запустить с новыми настройками."
        await callback_query.message.edit_text(
            msg_text,
            reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=edit_mode, task_id=edit_task_id)
        )
        await safe_answer_callback(callback_query, "Готово")
        return True

    if data == "pf_native_source":
        # Кнопка отображения убрана; отображение только в тексте. Оставляем no-op на случай старого callback.
        await safe_answer_callback(callback_query)
        return True

    if data == "pf_settings_back":
        # Go back to mode selection
        user_states[user_id]['state'] = FSM_POST_FORWARD_MODE_SELECT
        await callback_query.message.edit_text(
            "📨 **Пересылка постов**\n\n"
            "Выберите режим работы:",
            reply_markup=get_post_forward_main_keyboard()
        )
        await safe_answer_callback(callback_query)
        return True
    
    if data == "pf_start_task":
        await create_and_start_post_forward_task(client, callback_query)
        return True
    
    # Handle post parse task actions
    if data.startswith("pp_pause:"):
        task_id = int(data.split(":")[1])
        result = await api_client.stop_post_parse_task(task_id)
        if result.get('success'):
            await safe_answer_callback(callback_query, "⏸️ Задача приостановлена")
            # Refresh status
            task_result = await api_client.get_post_parse_task(task_id)
            if task_result.get('success'):
                task = task_result['task']
                await callback_query.message.edit_text(
                    format_post_parse_status(task),
                    reply_markup=get_post_parse_paused_keyboard(task_id)
                )
        else:
            await safe_answer_callback(callback_query, f"❌ Ошибка: {result.get('error')}", show_alert=True)
        return True
    
    if data.startswith("pp_resume:"):
        task_id = int(data.split(":")[1])
        result = await api_client.start_post_parse_task(task_id)
        if result.get('success'):
            await safe_answer_callback(callback_query, "▶️ Задача возобновлена")
            task_result = await api_client.get_post_parse_task(task_id)
            if task_result.get('success'):
                task = task_result['task']
                await callback_query.message.edit_text(
                    format_post_parse_status(task),
                    reply_markup=get_post_parse_running_keyboard(task_id)
                )
        else:
            await safe_answer_callback(callback_query, f"❌ Ошибка: {result.get('error')}", show_alert=True)
        return True
    
    if data.startswith("pp_delete:"):
        task_id = int(data.split(":")[1])
        result = await api_client.delete_post_parse_task(task_id)
        if result.get('success'):
            await callback_query.message.edit_text("🗑️ Задача удалена")
            await safe_answer_callback(callback_query, "Задача удалена")
        else:
            await safe_answer_callback(callback_query, f"❌ Ошибка: {result.get('error')}", show_alert=True)
        return True
    
    if data.startswith("pp_refresh:"):
        task_id = int(data.split(":")[1])
        task_result = await api_client.get_post_parse_task(task_id)
        if not task_result.get('success'):
            await safe_answer_callback(callback_query, f"❌ Ошибка: {task_result.get('error')}", show_alert=True)
            return True
        task = task_result['task']
        keyboard = get_post_parse_running_keyboard(task_id) if task['status'] == 'running' else get_post_parse_paused_keyboard(task_id)
        try:
            await callback_query.message.edit_text(
                format_post_parse_status(task),
                reply_markup=keyboard
            )
        except MessageNotModified:
            pass
        except Exception as e:
            logger.error(f"Error refreshing post parse task: {e}")
        await safe_answer_callback(callback_query, "🔄 Статус обновлен")
        return True
    
    if data.startswith("pp_settings:"):
        task_id = int(data.split(":")[1])
        
        task_result = await api_client.get_post_parse_task(task_id)
        if not task_result.get('success'):
            await safe_answer_callback(callback_query, "❌ Не удалось загрузить задачу", show_alert=True)
            return True
        
        task_data = task_result.get('task', {})
        
        # Stop task if running
        if task_data.get('status') == 'running':
            await safe_answer_callback(callback_query, "⏸️ Приостановка задачи...")
            await api_client.stop_post_parse_task(task_id)
            task_data['status'] = 'paused'
            
        user_states[user_id]['editing_task_id'] = task_id
        user_states[user_id]['editing_task_type'] = 'post_parse'
        user_states[user_id]['post_forward_mode'] = 'parse'
        user_states[user_id]['pf_source'] = {
            'id': task_data.get('source_id'),
            'title': task_data.get('source_title') or 'N/A',
            'username': task_data.get('source_username'),
            'type': task_data.get('source_type', 'channel')
        }
        user_states[user_id]['pf_target'] = {
            'id': task_data.get('target_id'),
            'title': task_data.get('target_title') or 'N/A',
            'username': task_data.get('target_username'),
            'type': task_data.get('target_type', 'channel')
        }
        user_states[user_id]['post_forward_settings'] = {
            'limit': task_data.get('limit'),
            'delay_seconds': task_data.get('delay_seconds', 2),
            'delay_every': task_data.get('delay_every', 1),
            'rotate_sessions': task_data.get('rotate_sessions', False),
            'rotate_every': task_data.get('rotate_every', 0),
            'use_proxy': task_data.get('use_proxy', True),
            'selected_sessions': task_data.get('available_sessions', []),
            'filter_contacts': task_data.get('filter_contacts', False),
            'remove_contacts': task_data.get('remove_contacts', False),
            'skip_on_contacts': task_data.get('skip_on_contacts', False),
            'parse_direction': task_data.get('parse_direction', 'backward'),
            'media_filter': task_data.get('media_filter', 'all'),
            'use_native_forward': task_data.get('use_native_forward', False),
            'check_content_if_native': task_data.get('check_content_if_native', True),
            'forward_show_source': task_data.get('forward_show_source', True),
            'keywords_whitelist': task_data.get('keywords_whitelist', []),
            'keywords_blacklist': task_data.get('keywords_blacklist', []),
            'add_signature': task_data.get('add_signature', False),
            'signature_options': task_data.get('signature_options')
        }
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
        
        settings = user_states[user_id]['post_forward_settings']
        source = user_states[user_id]['pf_source']
        target = user_states[user_id]['pf_target']
        sc = len(settings.get('selected_sessions', [])) or None
        msg_text = get_post_forward_settings_message_text("parse", source, target, settings, sc)
        msg_text = msg_text.rstrip()
        msg_text += "\n\n**Редактирование:** Сохранить — вернуться к деталям задачи. Запустить заново — сбросить прогресс и запустить с новыми настройками."
        await callback_query.message.edit_text(
            msg_text,
            reply_markup=get_post_forward_settings_keyboard(settings, mode="parse", edit_mode=True, task_id=task_id)
        )
        await safe_answer_callback(callback_query)
        return True
    
    if data.startswith("pm_settings:"):
        task_id = int(data.split(":")[1])
        
        task_result = await api_client.get_post_monitoring_task(task_id)
        if not task_result.get('success'):
            await safe_answer_callback(callback_query, "❌ Не удалось загрузить задачу", show_alert=True)
            return True
        
        task_data = task_result.get('task', {})
        
        # Stop task if running
        if task_data.get('status') == 'running':
            await safe_answer_callback(callback_query, "⏸️ Приостановка задачи...")
            await api_client.stop_post_monitoring_task(task_id)
            task_data['status'] = 'paused'
            
        user_states[user_id]['editing_task_id'] = task_id
        user_states[user_id]['editing_task_type'] = 'post_monitor'
        user_states[user_id]['post_forward_mode'] = 'monitor'
        user_states[user_id]['pf_source'] = {
            'id': task_data.get('source_id'),
            'title': task_data.get('source_title') or 'N/A',
            'username': task_data.get('source_username'),
            'type': task_data.get('source_type', 'channel')
        }
        user_states[user_id]['pf_target'] = {
            'id': task_data.get('target_id'),
            'title': task_data.get('target_title') or 'N/A',
            'username': task_data.get('target_username'),
            'type': task_data.get('target_type', 'channel')
        }
        user_states[user_id]['post_forward_settings'] = {
            'limit': task_data.get('limit'),
            'delay_seconds': task_data.get('delay_seconds', 0),
            'rotate_sessions': task_data.get('rotate_sessions', False),
            'rotate_every': task_data.get('rotate_every', 0),
            'use_proxy': task_data.get('use_proxy', True),
            'selected_sessions': task_data.get('available_sessions', []),
            'filter_contacts': task_data.get('filter_contacts', False),
            'remove_contacts': task_data.get('remove_contacts', False),
            'skip_on_contacts': task_data.get('skip_on_contacts', False),
            'media_filter': task_data.get('media_filter', 'all'),
            'use_native_forward': task_data.get('use_native_forward', False),
            'check_content_if_native': task_data.get('check_content_if_native', True),
            'forward_show_source': task_data.get('forward_show_source', True),
            'keywords_whitelist': task_data.get('keywords_whitelist', []),
            'keywords_blacklist': task_data.get('keywords_blacklist', []),
            'add_signature': task_data.get('add_signature', False),
            'signature_options': task_data.get('signature_options')
        }
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
        
        settings = user_states[user_id]['post_forward_settings']
        source = user_states[user_id]['pf_source']
        target = user_states[user_id]['pf_target']
        sc = len(settings.get('selected_sessions', [])) or None
        msg_text = get_post_forward_settings_message_text("monitor", source, target, settings, sc)
        msg_text = msg_text.rstrip()
        msg_text += "\n\n**Редактирование:** Сохранить — вернуться к деталям задачи. Запустить заново — сбросить прогресс и запустить с новыми настройками."
        await callback_query.message.edit_text(
            msg_text,
            reply_markup=get_post_forward_settings_keyboard(settings, mode="monitor", edit_mode=True, task_id=task_id)
        )
        await safe_answer_callback(callback_query)
        return True
    
    # Save/Cancel handlers for post tasks
    if data.startswith("pp_settings_save:"):
        task_id = int(data.split(":")[1])
        settings = user_states.get(user_id, {}).get('post_forward_settings', {})
        
        # Save settings via API
        result = await api_client.update_post_parse_task(
            task_id,
            limit=settings.get('limit'),
            delay_seconds=settings.get('delay_seconds'),
            delay_every=settings.get('delay_every'),
            rotate_sessions=settings.get('rotate_sessions'),
            rotate_every=settings.get('rotate_every'),
            use_proxy=settings.get('use_proxy'),
            available_sessions=settings.get('selected_sessions'),
            filter_contacts=settings.get('filter_contacts'),
            remove_contacts=settings.get('remove_contacts'),
            skip_on_contacts=settings.get('skip_on_contacts'),
            parse_direction=settings.get('parse_direction'),
            media_filter=settings.get('media_filter'),
            use_native_forward=settings.get('use_native_forward'),
            check_content_if_native=settings.get('check_content_if_native'),
            forward_show_source=settings.get('forward_show_source'),
            keywords_whitelist=settings.get('keywords_whitelist'),
            keywords_blacklist=settings.get('keywords_blacklist'),
            add_signature=settings.get('add_signature'),
            signature_options=settings.get('signature_options')
        )
        
        if not result.get('success'):
            await safe_answer_callback(callback_query, "❌ Не удалось сохранить настройки", show_alert=True)
            return True
        
        # Clear editing state
        user_states[user_id].pop('editing_task_id', None)
        user_states[user_id].pop('editing_task_type', None)
        
        # Redirect to task details
        await safe_answer_callback(callback_query, "✅ Настройки сохранены")
        
        # Refresh task details
        task_result = await api_client.get_post_parse_task(task_id)
        if task_result.get('success'):
            task = task_result['task']
            keyboard = get_post_parse_running_keyboard(task_id) if task['status'] == 'running' else get_post_parse_paused_keyboard(task_id)
            await callback_query.message.edit_text(
                format_post_parse_status(task),
                reply_markup=keyboard
            )
        return True
    
    if data.startswith("pp_settings_cancel:"):
        task_id = int(data.split(":")[1])
        
        # Clear editing state
        user_states[user_id].pop('editing_task_id', None)
        user_states[user_id].pop('editing_task_type', None)
        
        # Redirect to task details
        await safe_answer_callback(callback_query, "Отменено")
        
        # Refresh task details
        task_result = await api_client.get_post_parse_task(task_id)
        if task_result.get('success'):
            task = task_result['task']
            keyboard = get_post_parse_running_keyboard(task_id) if task['status'] == 'running' else get_post_parse_paused_keyboard(task_id)
            await callback_query.message.edit_text(
                format_post_parse_status(task),
                reply_markup=keyboard
            )
        return True
    
    if data.startswith("pp_settings_restart:"):
        task_id = int(data.split(":")[1])
        settings = user_states.get(user_id, {}).get('post_forward_settings', {})
        result = await api_client.restart_post_parse_task(
            task_id,
            limit=settings.get('limit'),
            delay_seconds=settings.get('delay_seconds'),
            delay_every=settings.get('delay_every'),
            rotate_sessions=settings.get('rotate_sessions'),
            rotate_every=settings.get('rotate_every'),
            use_proxy=settings.get('use_proxy'),
            available_sessions=settings.get('selected_sessions'),
            filter_contacts=settings.get('filter_contacts'),
            remove_contacts=settings.get('remove_contacts'),
            skip_on_contacts=settings.get('skip_on_contacts'),
            parse_direction=settings.get('parse_direction'),
            media_filter=settings.get('media_filter'),
            use_native_forward=settings.get('use_native_forward'),
            check_content_if_native=settings.get('check_content_if_native'),
            forward_show_source=settings.get('forward_show_source'),
            keywords_whitelist=settings.get('keywords_whitelist'),
            keywords_blacklist=settings.get('keywords_blacklist'),
            add_signature=settings.get('add_signature'),
            signature_options=settings.get('signature_options')
        )
        if not result.get('success'):
            await safe_answer_callback(callback_query, "❌ Не удалось запустить заново: " + result.get('error', 'Ошибка'), show_alert=True)
            return True
        user_states[user_id].pop('editing_task_id', None)
        user_states[user_id].pop('editing_task_type', None)
        await safe_answer_callback(callback_query, "🔄 Задача запущена заново с новыми настройками")
        task_result = await api_client.get_post_parse_task(task_id)
        if task_result.get('success'):
            task = task_result['task']
            keyboard = get_post_parse_running_keyboard(task_id) if task['status'] == 'running' else get_post_parse_paused_keyboard(task_id)
            await callback_query.message.edit_text(
                format_post_parse_status(task),
                reply_markup=keyboard
            )
        return True
    
    if data.startswith("pm_settings_save:"):
        task_id = int(data.split(":")[1])
        settings = user_states.get(user_id, {}).get('post_forward_settings', {})
        
        # Save settings via API
        result = await api_client.update_post_monitoring_task(
            task_id,
            limit=settings.get('limit'),
            delay_seconds=settings.get('delay_seconds'),
            rotate_sessions=settings.get('rotate_sessions'),
            rotate_every=settings.get('rotate_every'),
            use_proxy=settings.get('use_proxy'),
            available_sessions=settings.get('selected_sessions'),
            filter_contacts=settings.get('filter_contacts'),
            remove_contacts=settings.get('remove_contacts'),
            skip_on_contacts=settings.get('skip_on_contacts'),
            media_filter=settings.get('media_filter'),
            use_native_forward=settings.get('use_native_forward'),
            check_content_if_native=settings.get('check_content_if_native'),
            forward_show_source=settings.get('forward_show_source'),
            keywords_whitelist=settings.get('keywords_whitelist'),
            keywords_blacklist=settings.get('keywords_blacklist'),
            add_signature=settings.get('add_signature'),
            signature_options=settings.get('signature_options')
        )
        
        if not result.get('success'):
            await safe_answer_callback(callback_query, "❌ Не удалось сохранить настройки", show_alert=True)
            return True
        
        # Clear editing state
        user_states[user_id].pop('editing_task_id', None)
        user_states[user_id].pop('editing_task_type', None)
        
        # Redirect to task details
        await safe_answer_callback(callback_query, "✅ Настройки сохранены")
        
        # Refresh task details
        task_result = await api_client.get_post_monitoring_task(task_id)
        if task_result.get('success'):
            task = task_result['task']
            keyboard = get_post_monitor_running_keyboard(task_id) if task['status'] == 'running' else get_post_monitor_paused_keyboard(task_id)
            await callback_query.message.edit_text(
                format_post_monitor_status(task),
                reply_markup=keyboard
            )
        return True
    
    if data.startswith("pm_settings_cancel:"):
        task_id = int(data.split(":")[1])
        
        # Clear editing state
        user_states[user_id].pop('editing_task_id', None)
        user_states[user_id].pop('editing_task_type', None)
        
        # Redirect to task details
        await safe_answer_callback(callback_query, "Отменено")
        
        # Refresh task details
        task_result = await api_client.get_post_monitoring_task(task_id)
        if task_result.get('success'):
            task = task_result['task']
            keyboard = get_post_monitor_running_keyboard(task_id) if task['status'] == 'running' else get_post_monitor_paused_keyboard(task_id)
            await callback_query.message.edit_text(
                format_post_monitor_status(task),
                reply_markup=keyboard
            )
        return True
    
    if data.startswith("pm_settings_restart:"):
        task_id = int(data.split(":")[1])
        settings = user_states.get(user_id, {}).get('post_forward_settings', {})
        result = await api_client.restart_post_monitoring_task(
            task_id,
            limit=settings.get('limit'),
            delay_seconds=settings.get('delay_seconds'),
            rotate_sessions=settings.get('rotate_sessions'),
            rotate_every=settings.get('rotate_every'),
            use_proxy=settings.get('use_proxy'),
            available_sessions=settings.get('selected_sessions'),
            filter_contacts=settings.get('filter_contacts'),
            remove_contacts=settings.get('remove_contacts'),
            skip_on_contacts=settings.get('skip_on_contacts'),
            media_filter=settings.get('media_filter'),
            use_native_forward=settings.get('use_native_forward'),
            check_content_if_native=settings.get('check_content_if_native'),
            forward_show_source=settings.get('forward_show_source'),
            keywords_whitelist=settings.get('keywords_whitelist'),
            keywords_blacklist=settings.get('keywords_blacklist'),
            add_signature=settings.get('add_signature'),
            signature_options=settings.get('signature_options')
        )
        if not result.get('success'):
            await safe_answer_callback(callback_query, "❌ Не удалось запустить заново: " + result.get('error', 'Ошибка'), show_alert=True)
            return True
        user_states[user_id].pop('editing_task_id', None)
        user_states[user_id].pop('editing_task_type', None)
        await safe_answer_callback(callback_query, "🔄 Задача запущена заново с новыми настройками")
        task_result = await api_client.get_post_monitoring_task(task_id)
        if task_result.get('success'):
            task = task_result['task']
            keyboard = get_post_monitor_running_keyboard(task_id) if task['status'] == 'running' else get_post_monitor_paused_keyboard(task_id)
            await callback_query.message.edit_text(
                format_post_monitor_status(task),
                reply_markup=keyboard
            )
        return True
    
    # Handle post monitor task actions
    if data.startswith("pm_pause:"):
        task_id = int(data.split(":")[1])
        result = await api_client.stop_post_monitoring_task(task_id)
        if result.get('success'):
            await safe_answer_callback(callback_query, "⏸️ Мониторинг приостановлен")
            task_result = await api_client.get_post_monitoring_task(task_id)
            if task_result.get('success'):
                task = task_result['task']
                await callback_query.message.edit_text(
                    format_post_monitor_status(task),
                    reply_markup=get_post_monitor_paused_keyboard(task_id)
                )
        else:
            await safe_answer_callback(callback_query, f"❌ Ошибка: {result.get('error')}", show_alert=True)
        return True
    
    if data.startswith("pm_resume:"):
        task_id = int(data.split(":")[1])
        result = await api_client.start_post_monitoring_task(task_id)
        if result.get('success'):
            await safe_answer_callback(callback_query, "▶️ Мониторинг возобновлен")
            task_result = await api_client.get_post_monitoring_task(task_id)
            if task_result.get('success'):
                task = task_result['task']
                await callback_query.message.edit_text(
                    format_post_monitor_status(task),
                    reply_markup=get_post_monitor_running_keyboard(task_id)
                )
        else:
            await safe_answer_callback(callback_query, f"❌ Ошибка: {result.get('error')}", show_alert=True)
        return True
    
    if data.startswith("pm_delete:"):
        task_id = int(data.split(":")[1])
        result = await api_client.delete_post_monitoring_task(task_id)
        if result.get('success'):
            await callback_query.message.edit_text("🗑️ Мониторинг удален")
            await safe_answer_callback(callback_query, "Мониторинг удален")
        else:
            await safe_answer_callback(callback_query, f"❌ Ошибка: {result.get('error')}", show_alert=True)
        return True
    
    if data.startswith("pm_refresh:"):
        task_id = int(data.split(":")[1])
        task_result = await api_client.get_post_monitoring_task(task_id)
        if not task_result.get('success'):
            await safe_answer_callback(callback_query, f"❌ Ошибка: {task_result.get('error')}", show_alert=True)
            return True
        task = task_result['task']
        keyboard = get_post_monitor_running_keyboard(task_id) if task['status'] == 'running' else get_post_monitor_paused_keyboard(task_id)
        try:
            await callback_query.message.edit_text(
                format_post_monitor_status(task),
                reply_markup=keyboard
            )
        except MessageNotModified:
            pass
        except Exception as e:
            logger.error(f"Error refreshing post monitoring task: {e}")
        await safe_answer_callback(callback_query, "🔄 Статус обновлен")
        return True
    
    return False


async def show_post_forward_tasks(client: Client, callback_query):
    """Show list of post forwarding tasks."""
    user_id = callback_query.from_user.id
    
    # Get parse tasks
    parse_result = await api_client.get_user_post_parse_tasks(user_id)
    parse_tasks = parse_result.get('tasks', []) if parse_result.get('success') else []
    
    # Get monitoring tasks
    monitor_result = await api_client.get_user_post_monitoring_tasks(user_id)
    monitor_tasks = monitor_result.get('tasks', []) if monitor_result.get('success') else []
    
    if not parse_tasks and not monitor_tasks:
        await callback_query.message.edit_text(
            "📋 **Мои задачи пересылки**\n\n"
            "У вас нет активных задач.\n\n"
            "Создайте новую задачу с помощью кнопок ниже.",
            reply_markup=get_post_forward_main_keyboard()
        )
        await safe_answer_callback(callback_query)
        return
    
    text = "📋 **Мои задачи пересылки**\n\n"
    buttons = []
    
    if parse_tasks:
        text += "**📥 Парсинг постов:**\n"
        for task in parse_tasks[:5]:
            status_icon = {'running': '🚀', 'paused': '⏸️', 'completed': '✅', 'failed': '❌'}.get(task['status'], '⏳')
            text += f"• {status_icon} {task['source_title']} → {task['target_title']} ({task['forwarded_count']} пост.)\n"
            buttons.append([InlineKeyboardButton(
                f"{status_icon} {task['source_title'][:20]}",
                callback_data=f"pp_refresh:{task['id']}"
            )])
    
    if monitor_tasks:
        text += "\n**🔄 Мониторинг постов:**\n"
        for task in monitor_tasks[:5]:
            status_icon = {'running': '🚀', 'paused': '⏸️', 'completed': '✅', 'failed': '❌'}.get(task['status'], '⏳')
            text += f"• {status_icon} {task['source_title']} → {task['target_title']} ({task['forwarded_count']} пост.)\n"
            buttons.append([InlineKeyboardButton(
                f"{status_icon} {task['source_title'][:20]}",
                callback_data=f"pm_refresh:{task['id']}"
            )])
    
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="pf_back")])
    
    await callback_query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    await safe_answer_callback(callback_query)


async def create_and_start_post_forward_task(client: Client, callback_query):
    """Create and start a post forwarding task."""
    user_id = callback_query.from_user.id
    state = user_states.get(user_id, {})
    
    mode = state.get('post_forward_mode', 'parse')
    settings = state.get('post_forward_settings', {})
    source = state.get('pf_source', {})
    target = state.get('pf_target', {})
    
    # Get selected sessions from user's choice
    available_sessions = settings.get('selected_sessions', [])
    
    if not available_sessions:
        # Fallback: get first session from all sessions
        sessions_result = await api_client.list_sessions()
        sessions = sessions_result.get('sessions', [])
        if not sessions:
            await safe_answer_callback(callback_query, "❌ Нет доступных сессий", show_alert=True)
            return
        available_sessions = [sessions[0]['alias']]
        logger.warning(f"[POST_FORWARD] No selected sessions, falling back to first: {available_sessions}")
    
    # Use first selected session as primary
    session_alias = available_sessions[0]
    logger.info(f"[POST_FORWARD] Creating task with session_alias={session_alias}, available_sessions={available_sessions}")
    
    try:
        if mode == 'parse':
            result = await api_client.create_post_parse_task(
                user_id=user_id,
                source_id=source.get('id'),
                source_title=source.get('title', ''),
                source_username=source.get('username'),
                source_type=state.get('pf_source_type', 'channel'),
                target_id=target.get('id'),
                target_title=target.get('title', ''),
                target_username=target.get('username'),
                target_type=state.get('pf_target_type', 'channel'),
                session_alias=session_alias,
                available_sessions=available_sessions,
                limit=settings.get('limit'),
                delay_seconds=settings.get('delay_seconds', 2),
                delay_every=settings.get('delay_every', 1),
                rotate_sessions=settings.get('rotate_sessions', False),
                rotate_every=settings.get('rotate_every', 0),
                use_proxy=settings.get('use_proxy', True),
                filter_contacts=settings.get('filter_contacts', False),
                remove_contacts=settings.get('remove_contacts', False),
                skip_on_contacts=settings.get('skip_on_contacts', False),
                parse_direction=settings.get('parse_direction', 'backward'),
                media_filter=settings.get('media_filter', 'all'),
                use_native_forward=settings.get('use_native_forward', False),
                check_content_if_native=settings.get('check_content_if_native', True),
                forward_show_source=settings.get('forward_show_source', True),
                keywords_whitelist=settings.get('keywords_whitelist', []),
                keywords_blacklist=settings.get('keywords_blacklist', []),
                add_signature=settings.get('add_signature', False),
                signature_options=settings.get('signature_options')
            )
            
            if result.get('success'):
                task_id = result['task_id']
                # Start task
                await api_client.start_post_parse_task(task_id)
                
                await callback_query.message.edit_text(
                    f"✅ **Задача парсинга постов создана!**\n\n"
                    f"📤 Источник: {source.get('title', 'N/A')}\n"
                    f"📥 Цель: {target.get('title', 'N/A')}\n\n"
                    f"Задача запущена...",
                    reply_markup=get_post_parse_running_keyboard(task_id)
                )
            else:
                await safe_answer_callback(callback_query, f"❌ Ошибка: {result.get('error')}", show_alert=True)
        else:
            # Monitoring
            result = await api_client.create_post_monitoring_task(
                user_id=user_id,
                source_id=source.get('id'),
                source_title=source.get('title', ''),
                source_username=source.get('username'),
                source_type=state.get('pf_source_type', 'channel'),
                target_id=target.get('id'),
                target_title=target.get('title', ''),
                target_username=target.get('username'),
                target_type=state.get('pf_target_type', 'channel'),
                session_alias=session_alias,
                available_sessions=available_sessions,
                limit=settings.get('limit'),
                delay_seconds=settings.get('delay_seconds', 0),
                rotate_sessions=settings.get('rotate_sessions', False),
                rotate_every=settings.get('rotate_every', 0),
                use_proxy=settings.get('use_proxy', True),
                filter_contacts=settings.get('filter_contacts', False),
                remove_contacts=settings.get('remove_contacts', False),
                skip_on_contacts=settings.get('skip_on_contacts', False),
                use_native_forward=settings.get('use_native_forward', False),
                check_content_if_native=settings.get('check_content_if_native', True),
                forward_show_source=settings.get('forward_show_source', True),
                media_filter=settings.get('media_filter', 'all'),
                keywords_whitelist=settings.get('keywords_whitelist', []),
                keywords_blacklist=settings.get('keywords_blacklist', []),
                add_signature=settings.get('add_signature', False),
                signature_options=settings.get('signature_options')
            )
            
            if result.get('success'):
                task_id = result['task_id']
                # Start task
                await api_client.start_post_monitoring_task(task_id)
                
                await callback_query.message.edit_text(
                    f"✅ **Мониторинг постов запущен!**\n\n"
                    f"📤 Источник: {source.get('title', 'N/A')}\n"
                    f"📥 Цель: {target.get('title', 'N/A')}\n\n"
                    f"Новые посты будут пересылаться автоматически.",
                    reply_markup=get_post_monitor_running_keyboard(task_id)
                )
            else:
                await safe_answer_callback(callback_query, f"❌ Ошибка: {result.get('error')}", show_alert=True)
    
    except Exception as e:
        logger.error(f"Error creating post forward task: {e}")
        await safe_answer_callback(callback_query, f"❌ Ошибка: {e}", show_alert=True)
    
    await safe_answer_callback(callback_query)


async def handle_post_forward_text_input(client: Client, message: Message, text: str):
    """Handle text input for post forwarding flow."""
    user_id = message.from_user.id
    state = user_states.get(user_id, {}).get('state')
    
    if state == FSM_POST_FORWARD_SOURCE:
        # Back button
        if text.strip() == "🔙 Назад":
            user_states[user_id]['state'] = FSM_POST_FORWARD_SOURCE_TYPE
            mode = user_states[user_id].get('post_forward_mode', 'parse')
            mode_name = "Парсинг постов" if mode == 'parse' else "Мониторинг постов"
            mode_icon = "📥" if mode == 'parse' else "🔄"
            await message.reply("🔙", reply_markup=ReplyKeyboardRemove())
            await message.reply(
                f"{mode_icon} **{mode_name}**\n\n"
                "📤 **Шаг 2: Выберите тип источника**",
                reply_markup=get_post_forward_source_type_keyboard()
            )
            return
        
        # Selection from keyboard (list of channels/groups)
        group_data = parse_group_button(text)
        if group_data:
            group_id = group_data['id']
            full_title, full_username = await get_full_group_title_from_history(user_id, group_id, is_target=False)
            group_title = full_title or group_data['title']
            username = full_username if full_username is not None else group_data.get('username')
            user_states[user_id]['pf_source'] = {
                'id': int(group_id),
                'title': group_title,
                'username': username
            }
            user_states[user_id]['state'] = FSM_POST_FORWARD_TARGET_TYPE
            mode = user_states[user_id].get('post_forward_mode', 'parse')
            mode_name = "Парсинг постов" if mode == 'parse' else "Мониторинг постов"
            mode_icon = "📥" if mode == 'parse' else "🔄"
            await message.reply("✅", reply_markup=ReplyKeyboardRemove())
            await message.reply(
                f"{mode_icon} **{mode_name}**\n\n"
                f"✅ Источник: **{group_title}**\n\n"
                "📥 **Шаг 4: Выберите тип цели:**",
                reply_markup=get_post_forward_target_type_keyboard()
            )
            return
        
        # User entered source channel/group manually
        normalized = normalize_group_input(text)
        
        # Get selected sessions - use those assigned by user
        settings = user_states[user_id].get('post_forward_settings', {})
        selected_sessions = settings.get('selected_sessions', [])
        
        if not selected_sessions:
            await message.reply(
                "⚠️ **Сессии не назначены!**\n\n"
                "Вы не выбрали сессии для этой задачи.\n"
                "Вернитесь назад и выберите хотя бы одну сессию.",
                reply_markup=get_main_keyboard()
            )
            user_states[user_id]['state'] = FSM_POST_FORWARD_MODE_SELECT
            return
        
        # Try each selected session until one works
        group_info = None
        last_error = None
        failed_sessions = []
        
        for session_alias in selected_sessions:
            group_info = await api_client.get_group_info(session_alias, normalized)
            
            if group_info.get('success'):
                logger.info(f"[POST_FORWARD] Resolved {normalized} using session {session_alias}")
                break
            else:
                last_error = group_info.get('error', 'Неизвестная ошибка')
                failed_sessions.append(f"• {session_alias}: {last_error}")
                logger.warning(f"[POST_FORWARD] Session {session_alias} failed to resolve {normalized}: {last_error}")
        
        if not group_info or not group_info.get('success'):
            # All sessions failed - show detailed error
            error_details = "\n".join(failed_sessions) if failed_sessions else "Неизвестная ошибка"
            
            # Format user-friendly error
            error_msg = format_session_error_message(last_error, selected_sessions[0] if len(selected_sessions) == 1 else None)
            
            await message.reply(
                f"{error_msg}\n\n"
                f"**Проблемы с сессиями ({len(failed_sessions)}):**\n"
                f"{error_details}\n\n"
                "Попробуйте:\n"
                "• Проверить ссылку на канал/группу\n"
                "• Убедиться что сессия подключена к каналу/группе\n"
                "• Выбрать другие сессии"
            )
            return
        
        # Save source and add to history
        group_id = str(group_info['id'])
        group_title = group_info.get('title', f"ID: {group_info['id']}")
        username = group_info.get('username')
        user_states[user_id]['pf_source'] = {
            'id': int(group_id),
            'title': group_title,
            'username': username
        }
        await api_client.add_user_group(user_id, group_id, group_title, username)
        
        # Move to target type selection
        user_states[user_id]['state'] = FSM_POST_FORWARD_TARGET_TYPE
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        mode_name = "Парсинг постов" if mode == 'parse' else "Мониторинг постов"
        mode_icon = "📥" if mode == 'parse' else "🔄"
        await message.reply("✅", reply_markup=ReplyKeyboardRemove())
        await message.reply(
            f"{mode_icon} **{mode_name}**\n\n"
            f"✅ Источник: **{group_title}**\n\n"
            "📥 **Шаг 4: Выберите тип цели:**",
            reply_markup=get_post_forward_target_type_keyboard()
        )
        return
    
    if state == FSM_POST_FORWARD_TARGET:
        # Back button
        if text.strip() == "🔙 Назад":
            user_states[user_id]['state'] = FSM_POST_FORWARD_TARGET_TYPE
            mode = user_states[user_id].get('post_forward_mode', 'parse')
            mode_name = "Парсинг постов" if mode == 'parse' else "Мониторинг постов"
            mode_icon = "📥" if mode == 'parse' else "🔄"
            source = user_states[user_id].get('pf_source', {})
            await message.reply("🔙", reply_markup=ReplyKeyboardRemove())
            await message.reply(
                f"{mode_icon} **{mode_name}**\n\n"
                f"✅ Источник: **{source.get('title', 'N/A')}**\n\n"
                "📥 **Шаг 4: Выберите тип цели:**",
                reply_markup=get_post_forward_target_type_keyboard()
            )
            return
        
        # Selection from keyboard (list of channels/groups)
        group_data = parse_group_button(text)
        if group_data:
            group_id = group_data['id']
            full_title, full_username = await get_full_group_title_from_history(user_id, group_id, is_target=True)
            group_title = full_title or group_data['title']
            username = full_username if full_username is not None else group_data.get('username')
            user_states[user_id]['pf_target'] = {
                'id': int(group_id),
                'title': group_title,
                'username': username
            }
            user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
            mode = user_states[user_id].get('post_forward_mode', 'parse')
            source = user_states[user_id].get('pf_source', {})
            mode_name = "Парсинг постов" if mode == 'parse' else "Мониторинг постов"
            mode_icon = "📥" if mode == 'parse' else "🔄"
            await message.reply("✅", reply_markup=ReplyKeyboardRemove())
            settings = user_states[user_id].get('post_forward_settings', {})
            target = user_states[user_id].get('pf_target', {})
            msg_text = get_post_forward_settings_message_text(
                mode, source, target, settings, sessions_count=None
            )
            await message.reply(
                msg_text,
                reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=bool(user_states[user_id].get('editing_task_id')), task_id=user_states[user_id].get('editing_task_id'))
            )
            return
        
        # User entered target channel/group manually
        normalized = normalize_group_input(text)
        
        # Get selected sessions - use those assigned by user
        settings = user_states[user_id].get('post_forward_settings', {})
        selected_sessions = settings.get('selected_sessions', [])
        
        if not selected_sessions:
            await message.reply(
                "⚠️ **Сессии не назначены!**\n\n"
                "Вы не выбрали сессии для этой задачи.\n"
                "Вернитесь назад и выберите хотя бы одну сессию.",
                reply_markup=get_main_keyboard()
            )
            user_states[user_id]['state'] = FSM_POST_FORWARD_MODE_SELECT
            return
        
        # Try each selected session until one works
        group_info = None
        last_error = None
        failed_sessions = []
        
        for session_alias in selected_sessions:
            group_info = await api_client.get_group_info(session_alias, normalized)
            
            if group_info.get('success'):
                logger.info(f"[POST_FORWARD] Resolved {normalized} using session {session_alias}")
                break
            else:
                last_error = group_info.get('error', 'Неизвестная ошибка')
                failed_sessions.append(f"• {session_alias}: {last_error}")
                logger.warning(f"[POST_FORWARD] Session {session_alias} failed to resolve {normalized}: {last_error}")
        
        if not group_info or not group_info.get('success'):
            # All sessions failed - show detailed error
            error_details = "\n".join(failed_sessions) if failed_sessions else "Неизвестная ошибка"
            
            # Format user-friendly error
            error_msg = format_session_error_message(last_error, selected_sessions[0] if len(selected_sessions) == 1 else None)
            
            await message.reply(
                f"{error_msg}\n\n"
                f"**Проблемы с сессиями ({len(failed_sessions)}):**\n"
                f"{error_details}\n\n"
                "Попробуйте:\n"
                "• Проверить ссылку на канал/группу\n"
                "• Убедиться что сессия подключена к каналу/группе\n"
                "• Выбрать другие сессии"
            )
            return
        
        # Save target and add to history
        group_id = str(group_info['id'])
        group_title = group_info.get('title', f"ID: {group_info['id']}")
        username = group_info.get('username')
        user_states[user_id]['pf_target'] = {
            'id': int(group_id),
            'title': group_title,
            'username': username
        }
        await api_client.add_user_target_group(user_id, group_id, group_title, username)
        
        # Move to settings
        # user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
        mode = user_states[user_id].get('post_forward_mode', 'parse')
        # source = user_states[user_id].get('pf_source', {})
        # mode_name = "Парсинг постов" if mode == 'parse' else "Мониторинг постов"
        # mode_icon = "📥" if mode == 'parse' else "🔄"
        await message.reply("✅", reply_markup=ReplyKeyboardRemove())
        # target = user_states[user_id].get('pf_target', {})
        # msg_text = get_post_forward_settings_message_text(
        #     mode, source, target, settings, sessions_count=len(selected_sessions)
        # )
        # await message.reply(
        #     msg_text,
        #     reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=bool(user_states[user_id].get('editing_task_id')), task_id=user_states[user_id].get('editing_task_id'))
        # )
        await show_post_forward_settings(client, message)
        return
    
    # Handle settings inputs
    if state == FSM_POST_FORWARD_SETTINGS_LIMIT:
        try:
            if text.lower() in ['0', 'нет', 'без лимита']:
                user_states[user_id]['post_forward_settings']['limit'] = None
                await message.reply("✅ Лимит снят")
            else:
                limit = int(text)
                if limit < 1:
                    limit = None
                user_states[user_id]['post_forward_settings']['limit'] = limit
                await message.reply(f"✅ Лимит установлен: {limit}")
        except ValueError:
            await message.reply("❌ Введите число или '0' для снятия лимита")
            return
        
        await show_post_forward_settings(client, message)
        return
    
    if state == FSM_POST_FORWARD_SETTINGS_DELAY:
        try:
            delay = int(text)
            if delay < 0:
                delay = 0
            user_states[user_id]['post_forward_settings']['delay_seconds'] = delay
            await message.reply(f"✅ Задержка установлена: {delay} сек")
        except ValueError:
            await message.reply("❌ Введите число")
            return
        
        await show_post_forward_settings(client, message)
        return
    
    if state == FSM_POST_FORWARD_SETTINGS_DELAY_EVERY:
        try:
            every = int(text)
            if every < 1:
                every = 1
            user_states[user_id]['post_forward_settings']['delay_every'] = every
            await message.reply(f"✅ Задержка каждые {every} постов")
        except ValueError:
            await message.reply("❌ Введите число (минимум 1)")
            return
        
        await show_post_forward_settings(client, message)
        return
    
    if state == FSM_POST_FORWARD_SETTINGS_ROTATE_EVERY:
        try:
            rotate = int(text)
            if rotate < 0:
                rotate = 0
            user_states[user_id]['post_forward_settings']['rotate_every'] = rotate
            if rotate > 0:
                await message.reply(f"✅ Ротация каждые {rotate} постов")
            else:
                await message.reply("✅ Ротация только при ошибках")
        except ValueError:
            await message.reply("❌ Введите число")
            return
        
        await show_post_forward_settings(client, message)
        return

    if state == FSM_POST_FORWARD_SETTINGS_KEYWORDS_WHITELIST:
        if text.lower() in ['нет', '-', 'отмена']:
            user_states[user_id]['post_forward_settings']['keywords_whitelist'] = []
            await message.reply("✅ Список обязательных слов очищен")
        else:
            words = [w.strip() for w in text.split(',') if w.strip()]
            user_states[user_id]['post_forward_settings']['keywords_whitelist'] = words
            await message.reply(f"✅ Установлено {len(words)} обязательных слов")
        
        await show_post_forward_settings(client, message)
        return

    if state == FSM_POST_FORWARD_SETTINGS_KEYWORDS_BLACKLIST:
        if text.lower() in ['нет', '-', 'отмена']:
            user_states[user_id]['post_forward_settings']['keywords_blacklist'] = []
            await message.reply("✅ Список запрещенных слов очищен")
        else:
            words = [w.strip() for w in text.split(',') if w.strip()]
            user_states[user_id]['post_forward_settings']['keywords_blacklist'] = words
            await message.reply(f"✅ Установлено {len(words)} запрещенных слов")
        
        await show_post_forward_settings(client, message)
        return

    if state == FSM_POST_FORWARD_SIGNATURE_LABEL_POST:
        settings = user_states[user_id].get('post_forward_settings', {})
        opts = settings.get('signature_options') or get_default_signature_options()
        opts['label_post'] = text.strip() or 'Ссылка на пост'
        settings['signature_options'] = opts
        user_states[user_id]['post_forward_settings'] = settings
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
        await message.reply("✅ Текст для поста сохранён.\n\n" + get_signature_options_message_text(settings), reply_markup=get_signature_options_keyboard(settings))
        return

    if state == FSM_POST_FORWARD_SIGNATURE_LABEL_SOURCE:
        settings = user_states[user_id].get('post_forward_settings', {})
        opts = settings.get('signature_options') or get_default_signature_options()
        opts['label_source'] = text.strip() or 'Источник'
        settings['signature_options'] = opts
        user_states[user_id]['post_forward_settings'] = settings
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
        await message.reply("✅ Текст для источника сохранён.\n\n" + get_signature_options_message_text(settings), reply_markup=get_signature_options_keyboard(settings))
        return

    if state == FSM_POST_FORWARD_SIGNATURE_LABEL_AUTHOR:
        settings = user_states[user_id].get('post_forward_settings', {})
        opts = settings.get('signature_options') or get_default_signature_options()
        opts['label_author'] = text.strip() or 'Обращаться по объявлению сюда:'
        settings['signature_options'] = opts
        user_states[user_id]['post_forward_settings'] = settings
        user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
        await message.reply("✅ Текст для автора сохранён.\n\n" + get_signature_options_message_text(settings), reply_markup=get_signature_options_keyboard(settings))
        return


async def show_post_forward_settings(client: Client, message: Message):
    """Show post forward settings menu."""
    user_id = message.from_user.id
    user_states[user_id]['state'] = FSM_POST_FORWARD_SETTINGS
    
    mode = user_states[user_id].get('post_forward_mode', 'parse')
    settings = user_states[user_id].get('post_forward_settings', {})
    source = user_states[user_id].get('pf_source', {})
    target = user_states[user_id].get('pf_target', {})
    
    msg_text = get_post_forward_settings_message_text(
        mode, source, target, settings,
        sessions_count=len(settings.get('selected_sessions', [])) or None
    )
    await message.reply(
        msg_text,
        reply_markup=get_post_forward_settings_keyboard(settings, mode, edit_mode=bool(user_states[user_id].get('editing_task_id')), task_id=user_states[user_id].get('editing_task_id'))
    )


async def handle_invite_settings_from_status(client: Client, callback_query):
    """Handle opening settings for an invite task from status screen."""
    user_id = int(callback_query.from_user.id)
    task_id = int(callback_query.data.split(":")[1])
    
    # Get task details (API returns flat object, no "task" key)
    result = await api_client.get_task(task_id)
    if not result.get('success'):
        await callback_query.answer("❌ Не удалось загрузить задачу", show_alert=True)
        return

    task = result.get('task') or result
    
    # Stop task if running
    if task.get('status') == 'running':
        await callback_query.answer("⏸️ Приостановка задачи...")
        await api_client.stop_task(task_id)
        task['status'] = 'paused'
    
    # Normalize available_sessions to list (API may return list or comma-separated string)
    raw_sessions = task.get('available_sessions') or []
    if isinstance(raw_sessions, str):
        raw_sessions = [s.strip() for s in raw_sessions.split(',') if s.strip()]
    
    # Populate user state
    user_states[user_id] = {} # Clear previous state
    user_states[user_id]['editing_task_id'] = task_id
    user_states[user_id]['editing_task_type'] = 'invite'
    
    # We need to map task fields to valid invite_settings keys
    settings = {
        'delay_seconds': task.get('delay_seconds', 30),
        'delay_every': task.get('delay_every', 1),
        'limit': task.get('limit'),
        'rotate_sessions': task.get('rotate_sessions', False),
        'rotate_every': task.get('rotate_every', 0),
        'use_proxy': task.get('use_proxy', True),
        'available_sessions': raw_sessions,
        'selected_sessions': raw_sessions,
        'filter_mode': task.get('filter_mode', 'all'),
        'inactive_threshold_days': task.get('inactive_threshold_days'),
        'invite_mode': task.get('invite_mode', 'member_list')
    }
    
    user_states[user_id]['invite_settings'] = settings
    
    # Populate source/target for display context (API may use source_group/target_group or source_group_title/target_group_title)
    user_states[user_id]['source_group'] = {
        'title': task.get('source_group_title') or task.get('source_group', 'N/A'),
        'id': task.get('source_group_id')
    }
    user_states[user_id]['target_group'] = {
        'title': task.get('target_group_title') or task.get('target_group', 'N/A'),
        'id': task.get('target_group_id')
    }
    if task.get('file_source'):
        user_states[user_id]['source_file'] = task.get('file_source')
        user_states[user_id]['invite_settings']['invite_mode'] = 'from_file'

    # Show settings menu (in-place)
    await callback_query.message.edit_text(
        "⚙️ **Настройки инвайтинга**\n\n"
        "Выберите параметр для изменения:",
        reply_markup=get_settings_keyboard(settings, edit_mode=True)
    )
    await callback_query.answer()


async def handle_parse_settings_from_status(client: Client, callback_query):
    """Handle opening settings for a parse task from status screen."""
    user_id = int(callback_query.from_user.id)
    task_id = int(callback_query.data.split(":")[1])
    
    # Get task details
    result = await api_client.get_parse_task(task_id)
    if not result.get('success'):
        await callback_query.answer("❌ Не удалось загрузить задачу", show_alert=True)
        return

    task = result.get('task', {})
    
    # Stop task if running
    if task.get('status') == 'running':
        await callback_query.answer("⏸️ Приостановка задачи...")
        await api_client.stop_parse_task(task_id)
        task['status'] = 'paused'
    
    # Populate user state
    user_states[user_id] = {}
    user_states[user_id]['editing_task_id'] = task_id
    user_states[user_id]['editing_task_type'] = 'parse'
    
    settings = {
        'delay_seconds': task.get('delay_seconds', 2),
        'limit': task.get('limit'),
        'save_every': task.get('save_every', 0),
        'rotate_sessions': task.get('rotate_sessions', False),
        'rotate_every': task.get('rotate_every', 0),
        'use_proxy': task.get('use_proxy', True),
        'available_sessions': task.get('available_sessions', []),
        'selected_sessions': task.get('available_sessions', []),
        'filter_admins': task.get('filter_admins', False),
        'filter_inactive': task.get('filter_inactive', False),
        'inactive_days': task.get('inactive_threshold_days', 30),
        'keyword_filter': task.get('keyword_filter', []),
        'exclude_keywords': task.get('exclude_keywords', []),
        'parse_mode': task.get('parse_mode', 'member_list'),
        # Message based settings
        'messages_limit': task.get('messages_limit'),
        'delay_every_requests': task.get('delay_every_requests', 1),
        'rotate_every_requests': task.get('rotate_every_requests', 0),
        'save_every_users': task.get('save_every_users', 0)
    }
    
    user_states[user_id]['parse_settings'] = settings
    
    # Populate context
    user_states[user_id]['parse_file_name'] = task.get('file_name', 'N/A')
    user_states[user_id]['parse_source_group'] = {
        'title': task.get('source_group_title', 'N/A'),
        'id': task.get('source_group_id'),
        'username': task.get('source_username')
    }
    user_states[user_id]['parse_source_type'] = task.get('source_type', 'group')

    # Show settings menu (in-place)
    await callback_query.message.edit_text(
        "⚙️ **Настройки парсинга**\n\n"
        "Выберите параметр для изменения:",
        reply_markup=get_parse_settings_keyboard(settings, edit_mode=True)
    )
    await callback_query.answer()


async def handle_invite_settings_save(client: Client, callback_query):
    """Save invite settings and return to status."""
    user_id = int(callback_query.from_user.id)
    task_id = user_states.get(user_id, {}).get('editing_task_id')
    
    if not task_id:
        await callback_query.answer("❌ Ошибка: ID задачи не найден", show_alert=True)
        return

    settings = user_states.get(user_id, {}).get('invite_settings', {})
    
    # Save settings via API
    result = await api_client.update_task(
        task_id=task_id,
        delay_seconds=settings.get('delay_seconds'),
        delay_every=settings.get('delay_every'),
        limit=settings.get('limit'),
        rotate_sessions=settings.get('rotate_sessions'),
        rotate_every=settings.get('rotate_every'),
        use_proxy=settings.get('use_proxy'),
        available_sessions=settings.get('selected_sessions'),
        filter_mode=settings.get('filter_mode'),
        inactive_threshold_days=settings.get('inactive_threshold_days')
    )
    
    if result.get('success'):
        await callback_query.answer("✅ Настройки сохранены")
        # Return to task details (it will show Resume/Start buttons)
        await handle_invite_status(client, callback_query, task_id)
    else:
        await callback_query.answer(f"❌ Ошибка сохранения: {result.get('error')}", show_alert=True)


async def handle_invite_settings_cancel(client: Client, callback_query):
    """Cancel invite settings editing and return to status."""
    user_id = int(callback_query.from_user.id)
    task_id = user_states.get(user_id, {}).get('editing_task_id')
    
    if not task_id:
        await callback_query.answer("❌ Ошибка: ID задачи не найден", show_alert=True)
        return

    await callback_query.answer("❌ Редактирование отменено")
    await handle_invite_status(client, callback_query, task_id)


async def handle_parse_settings_save(client: Client, callback_query):
    """Save parse settings and return to status."""
    user_id = int(callback_query.from_user.id)
    task_id = user_states.get(user_id, {}).get('editing_task_id')
    
    if not task_id:
        await callback_query.answer("❌ Ошибка: ID задачи не найден", show_alert=True)
        return

    settings = user_states.get(user_id, {}).get('parse_settings', {})
    
    # Save settings via API
    # Prepare kwargs from settings
    update_data = {
        'delay_seconds': settings.get('delay_seconds'),
        'limit': settings.get('limit'),
        'save_every': settings.get('save_every'),
        'rotate_sessions': settings.get('rotate_sessions'),
        'rotate_every': settings.get('rotate_every'),
        'use_proxy': settings.get('use_proxy'),
        'available_sessions': settings.get('selected_sessions'),
        'filter_admins': settings.get('filter_admins'),
        'filter_inactive': settings.get('filter_inactive'),
        'inactive_threshold_days': settings.get('inactive_threshold_days'),
        'keyword_filter': settings.get('keyword_filter'),
        'exclude_keywords': settings.get('exclude_keywords'),
        # Message based settings
        'messages_limit': settings.get('messages_limit'),
        'delay_every_requests': settings.get('delay_every_requests'),
        'rotate_every_requests': settings.get('rotate_every_requests'),
        'save_every_users': settings.get('save_every_users')
    }
    
    result = await api_client.update_parse_task(task_id, **update_data)
    
    if result.get('success'):
        await callback_query.answer("✅ Настройки сохранены")
        await handle_parse_status(client, callback_query, task_id)
    else:
        await callback_query.answer(f"❌ Ошибка сохранения: {result.get('error')}", show_alert=True)


async def handle_parse_settings_cancel(client: Client, callback_query):
    """Cancel parse settings editing and return to status."""
    user_id = int(callback_query.from_user.id)
    task_id = user_states.get(user_id, {}).get('editing_task_id')
    
    if not task_id:
        await callback_query.answer("❌ Ошибка: ID задачи не найден", show_alert=True)
        return

    await callback_query.answer("❌ Редактирование отменено")
    await handle_parse_status(client, callback_query, task_id)
