# -*- coding: utf-8 -*-
"""
Main main handlers for the inviter bot.
"""
import logging
from typing import Dict
from pyrogram import Client
from pyrogram.types import Message, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton

from bot.api_client import api_client
from bot.states import (
    user_states,
    FSM_MAIN_MENU, FSM_INVITE_SOURCE_GROUP, FSM_INVITE_TARGET_GROUP,
    FSM_INVITE_SESSION_SELECT, FSM_INVITE_MENU, FSM_INVITE_SETTINGS,
    FSM_SETTINGS_DELAY, FSM_SETTINGS_DELAY_EVERY, FSM_SETTINGS_LIMIT,
    FSM_SETTINGS_ROTATE_EVERY,
    get_main_keyboard, get_group_history_keyboard, get_target_group_history_keyboard,
    get_invite_menu_keyboard, get_settings_keyboard, get_session_select_keyboard,
    get_invite_running_keyboard, get_invite_paused_keyboard,
    parse_group_button, normalize_group_input, format_group_button,
    format_invite_status
)
from bot.session_handlers import sessions_command
from bot.config import ADMIN_IDS

logger = logging.getLogger(__name__)


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
    except:
        pass  # Fail silently if API not up

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
            'selected_sessions': []
        }}
    
    user_states[user_id]['state'] = FSM_INVITE_SOURCE_GROUP
    
    kb = await get_group_history_keyboard(user_id)
    
    await message.reply(
        "📤 **Выберите группу-источник**\n\n"
        "Введите ссылку на группу, username или выберите из истории:",
        reply_markup=kb or ReplyKeyboardRemove()
    )


async def handle_source_group_input(client: Client, message: Message, text: str):
    """Handle source group input."""
    user_id = message.from_user.id
    
    # Try to parse as button
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
            await message.reply(
                "❌ Не удалось найти группу. Проверьте ссылку или ID.\n"
                "Попробуйте еще раз:"
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
    
    # Show mode selection
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


async def show_tasks_status(client: Client, message: Message):
    """Show all tasks status."""
    user_id = message.from_user.id
    
    result = await api_client.get_user_tasks(user_id)
    tasks = result.get('tasks', [])
    
    if not tasks:
        await message.reply(
            "📊 **Статус задач**\n\n"
            "У вас нет активных задач инвайтинга.",
            reply_markup=get_main_keyboard()
        )
        return
    
    text = "📊 **Статус задач**\n\n"
    
    status_icons = {
        'pending': '⏳',
        'running': '🚀',
        'paused': '⏸️',
        'completed': '✅',
        'failed': '❌'
    }
    
    for task in tasks[:10]:  # Limit to 10 tasks
        icon = status_icons.get(task['status'], '❓')
        invited = task.get('invited_count', 0)
        limit = task.get('limit')
        limit_text = f"/{limit}" if limit else ""
        
        rotate_info = ""
        if task.get('rotate_sessions'):
            every = task.get('rotate_every', 0)
            rotate_info = f" | 🔄 Ротация: {'Да' if every == 0 else f'каждые {every}'}"
            
        text += f"{icon} {task['source_group']} → {task['target_group']}\n"
        text += f"   Приглашено: {invited}{limit_text} | {task['status']}{rotate_info}\n\n"
    
    buttons = []
    for task in tasks[:5]:  # Buttons for first 5 tasks
        if task['status'] == 'running':
            buttons.append([InlineKeyboardButton(
                f"⏹️ Остановить: {task['source_group'][:20]}",
                callback_data=f"invite_stop:{task['id']}"
            )])
        elif task['status'] == 'paused':
            buttons.append([InlineKeyboardButton(
                f"▶️ Продолжить: {task['source_group'][:20]}",
                callback_data=f"invite_resume:{task['id']}"
            )])
    
    buttons.append([InlineKeyboardButton("🔙 Назад", callback_data="tasks_back")])
    
    await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))


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
        
        await callback_query.edit_message_reply_markup(
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
    
    if data == "settings_sessions":
        await handle_session_selection(client, callback_query)
        return
    
    if data == "settings_back":
        user_states[user_id]['state'] = FSM_INVITE_MENU
        
        source = user_states[user_id].get('source_group', {})
        target = user_states[user_id].get('target_group', {})
        settings = user_states[user_id].get('invite_settings', {})
        
        rotate_info = 'Да' if settings.get('rotate_sessions') else 'Нет'
        if settings.get('rotate_sessions') and settings.get('rotate_every', 0) > 0:
            rotate_info += f" (каждые {settings['rotate_every']} инв.)"

        text = f"""
✅ **Настройка инвайтинга**

📤 Источник: **{source.get('title', 'N/A')}**
📥 Цель: **{target.get('title', 'N/A')}**

⚙️ **Текущие настройки:**
⏱️ Задержка: ~{settings.get('delay_seconds', 30)} сек
🔢 Каждые {settings.get('delay_every', 1)} инвайта
🔢 Лимит: {settings.get('limit') or 'Без лимита'}
🔄 Ротация сессий: {rotate_info}
"""
        
        await callback_query.edit_message_text(text, reply_markup=get_invite_menu_keyboard())
        return
    
    # ============== Session Selection ==============
    
    if data.startswith("toggle_session:"):
        await handle_toggle_session(client, callback_query)
        return
    
    if data == "sessions_done":
        await callback_query.answer("Сессии выбраны!")
        # Go back to settings
        settings = user_states[user_id].get('invite_settings', {})
        await callback_query.edit_message_text(
            "⚙️ **Настройки инвайтинга**\n\nВыберите параметр для изменения:",
            reply_markup=get_settings_keyboard(settings)
        )
        return
    
    if data == "sessions_back":
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
        await delete_confirmed_callback(client, callback_query)
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
    
    if data == "tasks_back":
        user_states[user_id] = {"state": FSM_MAIN_MENU}
        await callback_query.message.reply("Выберите действие:", reply_markup=get_main_keyboard())
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
    
    if not source or not target:
        await callback_query.answer("Сначала выберите группы!", show_alert=True)
        return
    
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
        invite_mode=settings.get('invite_mode', 'member_list'),
        delay_seconds=settings.get('delay_seconds', 30),
        delay_every=settings.get('delay_every', 1),
        limit=settings.get('limit'),
        rotate_sessions=settings.get('rotate_sessions', False),
        rotate_every=settings.get('rotate_every', 0),
        available_sessions=inviting_sessions
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
        await show_tasks_status(client, callback_query.message)
        try:
            await callback_query.message.delete()
        except:
            pass
    else:
        await callback_query.answer(f"Ошибка: {result.get('error')}", show_alert=True)


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
    
    await callback_query.edit_message_text(text, reply_markup=keyboard)
    await callback_query.answer("Статус обновлен")


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
    
    settings = user_states.get(user_id, {}).get('invite_settings', {})
    selected = settings.get('selected_sessions', [])
    
    if session_alias in selected:
        selected.remove(session_alias)
    else:
        selected.append(session_alias)
    
    settings['selected_sessions'] = selected
    user_states[user_id]['invite_settings'] = settings
    
    keyboard = await get_session_select_keyboard(selected)
    await callback_query.edit_message_reply_markup(reply_markup=keyboard)
    await callback_query.answer()
