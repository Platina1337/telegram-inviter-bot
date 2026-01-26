# -*- coding: utf-8 -*-
"""
Session management handlers for the inviter bot.
"""
import logging
import re
from typing import Dict
from pyrogram import Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from bot.api_client import api_client
from bot.states import (
    user_states, FSM_SESSION_PROXY,
    get_sessions_menu_keyboard, get_session_list_keyboard,
    get_task_assignment_keyboard, format_sessions_list
)
from bot.config import ADMIN_IDS

logger = logging.getLogger(__name__)


async def sessions_command(client: Client, message_or_query):
    """Handler for /sessions command or callback."""
    if hasattr(message_or_query, 'from_user'):
        user_id = int(message_or_query.from_user.id)
        send_func = lambda text, **kwargs: client.send_message(user_id, text, **kwargs)
    elif hasattr(message_or_query, 'message') and hasattr(message_or_query.message, 'chat'):
        user_id = int(message_or_query.message.chat.id)
        send_func = lambda text, **kwargs: client.send_message(user_id, text, **kwargs)
    else:
        logger.error("Could not determine user_id")
        return
    
    # Check admin access
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await send_func("У вас нет доступа к управлению сессиями.")
        return
    
    try:
        response = await api_client.list_sessions()
        if not response.get("success", False):
            await send_func(f"Ошибка: {response.get('error', 'Unknown error')}")
            return
        
        sessions = response.get("sessions", [])
        assignments = response.get("assignments", {})
        
        text = format_sessions_list(sessions, assignments)
        keyboard = get_sessions_menu_keyboard()
        
        await send_func(text, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Error in sessions_command: {e}")
        await send_func(f"Ошибка: {e}")


async def add_session_callback(client: Client, callback_query):
    """Callback for adding a new session."""
    user_id = int(callback_query.from_user.id)
    
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    
    # Instead of collecting session data through bot, redirect to terminal
    await callback_query.message.reply(
        "📋 **Добавление новой сессии**\n\n"
        "⚠️ Для корректной авторизации сессия должна быть добавлена **через терминал**, "
        "а не через бота.\n\n"
        "**Инструкция:**\n"
        "1. Запустите файл `add_session.bat` из папки проекта\n"
        "2. Следуйте инструкциям в консоли (название сессии, API ID, API Hash, номер телефона, код)\n"
        "3. После успешного добавления сессия появится в списке\n\n"
        "🔗 **API данные можно получить на:** https://my.telegram.org"
    )
    await callback_query.answer()



async def assign_session_callback(client: Client, callback_query):
    """Callback for assigning a session."""
    user_id = int(callback_query.from_user.id)
    
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    
    keyboard = await get_session_list_keyboard("select")
    await callback_query.edit_message_text(
        "Выберите сессию для назначения:",
        reply_markup=keyboard
    )


async def select_session_callback(client: Client, callback_query):
    """Callback for selecting a session."""
    user_id = int(callback_query.from_user.id)
    
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    
    # Extract session name from callback data
    session_name = callback_query.data.split(":", 1)[1]
    
    keyboard = get_task_assignment_keyboard(session_name)
    await callback_query.edit_message_text(
        f"Выберите действие для сессии **{session_name}**:",
        reply_markup=keyboard
    )


async def assign_task_callback(client: Client, callback_query):
    """Callback for assigning session to task."""
    user_id = int(callback_query.from_user.id)
    
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    
    parts = callback_query.data.split(":", 2)
    task = parts[1]
    session_name = parts[2]
    
    try:
        response = await api_client.assign_session(task, session_name)
        if response.get("success"):
            await callback_query.answer(f"Сессия {session_name} назначена на {task}!", show_alert=True)
            await sessions_command(client, callback_query)
        else:
            await callback_query.answer(f"Ошибка: {response.get('error')}", show_alert=True)
    except Exception as e:
        await callback_query.answer(f"Ошибка: {e}", show_alert=True)


async def remove_task_callback(client: Client, callback_query):
    """Callback for removing task assignment."""
    user_id = int(callback_query.from_user.id)
    
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    
    parts = callback_query.data.split(":", 2)
    task = parts[1]
    session_name = parts[2]
    
    try:
        response = await api_client.remove_assignment(task, session_name)
        if response.get("success"):
            await callback_query.answer(f"Сессия {session_name} убрана из {task}!", show_alert=True)
            await sessions_command(client, callback_query)
        else:
            await callback_query.answer(f"Ошибка: {response.get('error')}", show_alert=True)
    except Exception as e:
        await callback_query.answer(f"Ошибка: {e}", show_alert=True)


async def delete_session_callback(client: Client, callback_query):
    """Callback for deleting a session."""
    user_id = int(callback_query.from_user.id)
    
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    
    keyboard = await get_session_list_keyboard("confirm_delete")
    await callback_query.edit_message_text(
        "⚠️ Выберите сессию для удаления:",
        reply_markup=keyboard
    )


async def confirm_delete_callback(client: Client, callback_query):
    """Callback for confirming session deletion."""
    user_id = int(callback_query.from_user.id)
    
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    
    session_name = callback_query.data.split(":", 1)[1]
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Да, удалить", callback_data=f"delete_confirmed:{session_name}"),
            InlineKeyboardButton("❌ Отмена", callback_data="cancel_session_action")
        ]
    ])
    
    await callback_query.edit_message_text(
        f"⚠️ Вы уверены, что хотите удалить сессию **{session_name}**?",
        reply_markup=keyboard
    )


async def delete_confirmed_callback(client: Client, callback_query):
    """Callback for confirmed deletion."""
    user_id = int(callback_query.from_user.id)
    
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    
    session_name = callback_query.data.split(":", 1)[1]
    
    try:
        response = await api_client.delete_session(session_name)
        if response.get("success"):
            await callback_query.answer(f"Сессия {session_name} удалена!", show_alert=True)
            await sessions_command(client, callback_query)
        else:
            await callback_query.answer(f"Ошибка: {response.get('error')}", show_alert=True)
    except Exception as e:
        await callback_query.answer(f"Ошибка: {e}", show_alert=True)


async def test_proxy_callback(client: Client, callback_query):
    """Callback for testing proxy connection."""
    user_id = int(callback_query.from_user.id)

    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return

    session_name = callback_query.data.split(":", 1)[1]

    await callback_query.answer("🧪 Проверяю подключение...")

    try:
        # Test with proxy
        result_with_proxy = await api_client.test_session_proxy(session_name, use_proxy=True)
        # Test without proxy
        result_without_proxy = await api_client.test_session_proxy(session_name, use_proxy=False)

        message = f"🧪 **Результат проверки прокси для {session_name}**\n\n"

        # Results with proxy
        if result_with_proxy.get("success"):
            message += f"✅ **С прокси:** Успешно\n"
            if result_with_proxy.get("user_id"):
                message += f"   👤 ID: `{result_with_proxy['user_id']}`\n"
            if result_with_proxy.get("username"):
                message += f"   📝 Username: `@{result_with_proxy['username']}`\n"
            if result_with_proxy.get("ip_address"):
                message += f"   🌐 IP: `{result_with_proxy['ip_address']}`\n"
            if result_with_proxy.get("latency_ms"):
                message += f"   ⚡ Скорость: `{result_with_proxy['latency_ms']} мс`\n"
        else:
            message += f"❌ **С прокси:** {result_with_proxy.get('error', 'Ошибка')}\n"

        message += "\n"

        # Results without proxy
        if result_without_proxy.get("success"):
            message += f"✅ **Без прокси:** Успешно\n"
            if result_without_proxy.get("user_id"):
                message += f"   👤 ID: `{result_without_proxy['user_id']}`\n"
            if result_without_proxy.get("username"):
                message += f"   📝 Username: `@{result_without_proxy['username']}`\n"
            if result_without_proxy.get("ip_address"):
                message += f"   🌐 IP: `{result_without_proxy['ip_address']}`\n"
            if result_without_proxy.get("latency_ms"):
                message += f"   ⚡ Скорость: `{result_without_proxy['latency_ms']} мс`\n"
        else:
            message += f"❌ **Без прокси:** {result_without_proxy.get('error', 'Ошибка')}\n"
        
        # Add comparison if both tests succeeded
        if result_with_proxy.get("success") and result_without_proxy.get("success"):
            ip_with_proxy = result_with_proxy.get("ip_address")
            ip_without_proxy = result_without_proxy.get("ip_address")
            latency_with_proxy = result_with_proxy.get("latency_ms", 0)
            latency_without_proxy = result_without_proxy.get("latency_ms", 0)
            
            if ip_with_proxy and ip_without_proxy:
                message += f"\n   📊 **Сравнение:**\n"
                ip_changed = ip_with_proxy != ip_without_proxy
                if ip_changed:
                    message += f"   ✅ IP изменился (прокси работает)\n"
                else:
                    message += f"   ⚠️ IP не изменился (прокси может не работать)\n"
                message += f"   🌐 IP без прокси: `{ip_without_proxy}`\n"
                
                latency_diff = latency_with_proxy - latency_without_proxy
                if latency_diff > 0:
                    message += f"   ⚠️ Прокси медленнее на `{abs(latency_diff):.2f} мс`\n"
                elif latency_diff < 0:
                    message += f"   ✅ Прокси быстрее на `{abs(latency_diff):.2f} мс`\n"
                else:
                    message += f"   ➡️ Скорость одинаковая\n"

        await callback_query.message.reply(message)
    except Exception as e:
        await callback_query.message.reply(f"❌ Ошибка при проверке: {e}")


async def remove_proxy_callback(client: Client, callback_query):
    """Callback for removing proxy from session."""
    user_id = int(callback_query.from_user.id)

    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return

    session_name = callback_query.data.split(":", 1)[1]

    await callback_query.answer("🗑️ Удаляю прокси...")

    try:
        result = await api_client.remove_session_proxy(session_name)
        if result.get("success"):
            await callback_query.answer("✅ Прокси удален!", show_alert=True)
            await sessions_command(client, callback_query)
        else:
            await callback_query.answer(f"❌ Ошибка: {result.get('error')}", show_alert=True)
    except Exception as e:
        await callback_query.answer(f"❌ Ошибка: {e}", show_alert=True)


async def copy_proxy_callback(client: Client, callback_query):
    """Callback for copying proxy to another session."""
    user_id = int(callback_query.from_user.id)

    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return

    session_name = callback_query.data.split(":", 1)[1]

    # Get all sessions to show available targets
    response = await api_client.list_sessions()
    if not response.get("success"):
        await callback_query.answer("❌ Не удалось получить список сессий", show_alert=True)
        return

    sessions = response.get("sessions", [])
    # Filter sessions that have proxy configured
    source_sessions = [s for s in sessions if s.get('proxy')]
    # Filter sessions that don't have proxy (potential targets)
    target_sessions = [s for s in sessions if not s.get('proxy') and s['alias'] != session_name]

    if not source_sessions:
        await callback_query.answer("❌ Нет сессий с настроенным прокси", show_alert=True)
        return

    if not target_sessions:
        await callback_query.answer("❌ Нет сессий без прокси для копирования", show_alert=True)
        return

    # Create keyboard with target sessions
    buttons = []
    for session in target_sessions:
        alias = session.get('alias', '')
        phone = session.get('phone', '')
        buttons.append([InlineKeyboardButton(
            f"{alias} ({phone})",
            callback_data=f"copy_proxy_confirm:{session_name}:{alias}"
        )])

    buttons.append([InlineKeyboardButton("🔙 Отмена", callback_data="cancel_session_action")])

    await callback_query.edit_message_text(
        f"📋 **Копировать прокси из {session_name}**\n\n"
        "Выберите сессию, куда скопировать прокси:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def copy_proxy_confirm_callback(client: Client, callback_query):
    """Callback for confirming proxy copy."""
    user_id = int(callback_query.from_user.id)

    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return

    parts = callback_query.data.split(":", 2)
    from_alias = parts[1]
    to_alias = parts[2]

    await callback_query.answer("📋 Копирую прокси...")

    try:
        result = await api_client.copy_session_proxy(from_alias, to_alias)
        if result.get("success"):
            await callback_query.answer("✅ Прокси скопирован!", show_alert=True)
            await sessions_command(client, callback_query)
        else:
            await callback_query.answer(f"❌ Ошибка: {result.get('error')}", show_alert=True)
    except Exception as e:
        await callback_query.answer(f"❌ Ошибка: {e}", show_alert=True)


async def cancel_session_action_callback(client: Client, callback_query):
    """Callback for canceling session action."""
    user_id = int(callback_query.from_user.id)

    if user_id in user_states:
        user_states[user_id] = {}

    await sessions_command(client, callback_query)


async def set_proxy_callback(client: Client, callback_query):
    """Callback for setting proxy."""
    user_id = int(callback_query.from_user.id)
    
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await callback_query.answer("Нет доступа", show_alert=True)
        return
    
    session_name = callback_query.data.split(":", 1)[1]
    
    user_states[user_id] = {
        "state": FSM_SESSION_PROXY,
        "session_name": session_name
    }
    
    # Get current proxy status
    try:
        sessions_response = await api_client.list_sessions()
        current_proxy = None
        if sessions_response.get("success"):
            sessions = sessions_response.get("sessions", [])
            session_data = next((s for s in sessions if s.get('alias') == session_name), None)
            current_proxy = session_data.get('proxy') if session_data else None
    except:
        current_proxy = None

    proxy_status = f"Текущий прокси: `{current_proxy}`\n\n" if current_proxy else "Прокси не настроен\n\n"

    await callback_query.edit_message_text(
        f"🌐 **Настройка прокси для {session_name}**\n\n"
        f"{proxy_status}"
        "Введите строку прокси в формате:\n"
        "`scheme://user:pass@host:port`\n"
        "или\n"
        "`scheme://host:port`\n\n"
        "Поддерживаемые схемы: `socks5`, `socks4`, `http`\n\n"
        "Для удаления прокси отправьте `none` или `нет`.\n"
        "Для отмены отправьте /cancel",
    )



