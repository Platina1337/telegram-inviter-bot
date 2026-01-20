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
    user_states,
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


async def cancel_session_action_callback(client: Client, callback_query):
    """Callback for canceling session action."""
    user_id = int(callback_query.from_user.id)
    
    if user_id in user_states:
        user_states[user_id] = {}
    
    await sessions_command(client, callback_query)



