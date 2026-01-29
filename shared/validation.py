# -*- coding: utf-8 -*-
"""
Input validation utilities for the inviter bot.
"""
import re
import os
from typing import Optional, Tuple

# Filename sanitization pattern - only allow safe characters
SAFE_FILENAME_PATTERN = re.compile(r'^[a-zA-Z0-9_\-а-яА-ЯёЁ\s]+$')

# Max lengths
MAX_FILENAME_LENGTH = 100
MAX_GROUP_INPUT_LENGTH = 200
MAX_PROXY_LENGTH = 200
MAX_SESSION_ALIAS_LENGTH = 50


def sanitize_filename(filename: str) -> Tuple[bool, str, Optional[str]]:
    """
    Validate and sanitize a filename.
    
    Returns:
        Tuple of (is_valid, sanitized_name, error_message)
    """
    if not filename:
        return False, "", "Имя файла не может быть пустым"
    
    # Remove leading/trailing whitespace
    filename = filename.strip()
    
    # Check length
    if len(filename) > MAX_FILENAME_LENGTH:
        return False, "", f"Имя файла слишком длинное (макс. {MAX_FILENAME_LENGTH} символов)"
    
    # Remove any path components for security
    filename = os.path.basename(filename)
    
    # Remove .json extension if present (will be added back)
    if filename.lower().endswith('.json'):
        filename = filename[:-5]
    
    # Check for dangerous characters
    if '..' in filename or '/' in filename or '\\' in filename:
        return False, "", "Имя файла содержит недопустимые символы"
    
    # Check for only safe characters
    if not SAFE_FILENAME_PATTERN.match(filename):
        return False, "", "Имя файла содержит недопустимые символы. Используйте буквы, цифры, пробелы, _ и -"
    
    # Replace multiple spaces with single
    filename = re.sub(r'\s+', ' ', filename)
    
    return True, filename, None


def validate_group_input(group_input: str) -> Tuple[bool, str, Optional[str]]:
    """
    Validate group input (username, link, or ID).
    
    Returns:
        Tuple of (is_valid, cleaned_input, error_message)
    """
    if not group_input:
        return False, "", "Введите ссылку на группу или её ID"
    
    group_input = group_input.strip()
    
    if len(group_input) > MAX_GROUP_INPUT_LENGTH:
        return False, "", "Слишком длинный ввод"
    
    # Check for dangerous patterns
    if '<script' in group_input.lower() or 'javascript:' in group_input.lower():
        return False, "", "Недопустимый ввод"
    
    return True, group_input, None


def validate_proxy_string(proxy: str) -> Tuple[bool, str, Optional[str]]:
    """
    Validate proxy string format.
    
    Supported formats:
    - socks5://user:pass@host:port
    - socks5://host:port
    - http://host:port
    - host:port:user:pass
    
    Returns:
        Tuple of (is_valid, cleaned_proxy, error_message)
    """
    if not proxy:
        return False, "", "Прокси не может быть пустым"
    
    proxy = proxy.strip()
    
    if len(proxy) > MAX_PROXY_LENGTH:
        return False, "", "Слишком длинная строка прокси"
    
    # Check for dangerous characters
    if '<script' in proxy.lower() or 'javascript:' in proxy.lower():
        return False, "", "Недопустимый ввод"
    
    # Basic format validation
    if '://' in proxy:
        # URL format
        if not re.match(r'^(socks[45]|http|https)://[\w\.\-:@]+$', proxy, re.IGNORECASE):
            return False, "", "Неверный формат прокси. Используйте: socks5://host:port или http://host:port"
    else:
        # host:port or host:port:user:pass
        parts = proxy.split(':')
        if len(parts) < 2 or len(parts) > 4:
            return False, "", "Неверный формат прокси. Используйте: host:port или host:port:user:pass"
    
    return True, proxy, None


def validate_session_alias(alias: str) -> Tuple[bool, str, Optional[str]]:
    """
    Validate session alias.
    
    Returns:
        Tuple of (is_valid, cleaned_alias, error_message)
    """
    if not alias:
        return False, "", "Имя сессии не может быть пустым"
    
    alias = alias.strip()
    
    if len(alias) > MAX_SESSION_ALIAS_LENGTH:
        return False, "", f"Имя сессии слишком длинное (макс. {MAX_SESSION_ALIAS_LENGTH} символов)"
    
    # Only alphanumeric and underscore
    if not re.match(r'^[a-zA-Z0-9_]+$', alias):
        return False, "", "Имя сессии может содержать только буквы, цифры и _"
    
    return True, alias, None


def validate_positive_int(value: str, field_name: str = "Значение", max_value: int = None) -> Tuple[bool, int, Optional[str]]:
    """
    Validate that a string is a positive integer.
    
    Returns:
        Tuple of (is_valid, parsed_value, error_message)
    """
    try:
        num = int(value.strip())
        if num < 0:
            return False, 0, f"{field_name} должно быть положительным числом"
        if max_value and num > max_value:
            return False, 0, f"{field_name} не может быть больше {max_value}"
        return True, num, None
    except ValueError:
        return False, 0, f"{field_name} должно быть числом"


def validate_keyword(keyword: str) -> Tuple[bool, str, Optional[str]]:
    """
    Validate keyword for filtering.
    
    Returns:
        Tuple of (is_valid, cleaned_keyword, error_message)
    """
    if not keyword:
        return False, "", "Ключевое слово не может быть пустым"
    
    keyword = keyword.strip()
    
    if len(keyword) > 100:
        return False, "", "Ключевое слово слишком длинное"
    
    # Remove potentially dangerous characters
    keyword = re.sub(r'[<>"\']', '', keyword)
    
    return True, keyword, None
