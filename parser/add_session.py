# -*- coding: utf-8 -*-
"""
CLI script to add a Telegram session interactively via terminal.
Supports optional proxy: ask user, validate and test proxy, then create session through it and save proxy in DB.
"""
import sys
import os
import asyncio
from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded, PhoneCodeInvalid

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .database import Database
from .config import config
from .session_manager import SessionManager, parse_proxy_string

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from models import SessionMeta
from validation import validate_proxy_string


def _ask_yes_no(prompt: str, default_no: bool = False) -> bool:
    """Ask yes/no. Accept y/n, yes/no, д/н, да/нет."""
    while True:
        s = input(prompt).strip().lower()
        if not s:
            return not default_no
        if s in ('y', 'yes', 'д', 'да'):
            return True
        if s in ('n', 'no', 'н', 'нет'):
            return False
        print("Введите y/n (да/нет).")


async def add_session_cli():
    print("=" * 40)
    print("Telegram Session Adder CLI")
    print("=" * 40)
    print("Скрипт создаст .session файл и добавит сессию в базу.\n")

    db = Database(config.DATABASE_PATH)
    await db.connect()

    try:
        session_name = input("Введите имя сессии (alias): ").strip()
        if not session_name:
            print("Имя сессии обязательно.")
            return

        existing = await db.get_session_by_alias(session_name)
        if existing:
            print(f"Сессия '{session_name}' уже есть в базе.")
            return

        api_id_str = input("Введите API ID (my.telegram.org): ").strip()
        api_hash = input("Введите API Hash (my.telegram.org): ").strip()
        phone = input("Введите номер телефона (+79...): ").strip()

        if not api_id_str.isdigit():
            print("API ID должен быть числом.")
            return

        api_id = int(api_id_str)

        # Спросить про прокси до создания экземпляра
        use_proxy = _ask_yes_no("Добавить прокси для этой сессии? (y/n): ", default_no=True)
        proxy_str = None
        proxy_dict = None

        if use_proxy:
            while True:
                proxy_str = input(
                    "Введите строку прокси (например socks5://host:port или socks5://user:pass@host:port): "
                ).strip()
                if not proxy_str:
                    print("Строка прокси не может быть пустой.")
                    continue
                is_valid, clean_proxy, error_msg = validate_proxy_string(proxy_str)
                if not is_valid:
                    print(f"Ошибка: {error_msg}")
                    continue
                proxy_str = clean_proxy
                proxy_dict = parse_proxy_string(proxy_str)
                if not proxy_dict:
                    print("Не удалось разобрать прокси. Используйте формат scheme://host:port или scheme://user:pass@host:port")
                    continue
                print("Проверка подключения к прокси...")
                sm = SessionManager(db)
                ip = await sm._check_ip_address(proxy_str)
                if ip:
                    print(f"Прокси доступен. Внешний IP: {ip}")
                    break
                print("Не удалось подключиться к прокси. Проверьте строку и доступность прокси.")
                if not _ask_yes_no("Повторить ввод прокси? (y/n): ", default_no=True):
                    return

        session_dir_abs = os.path.abspath(config.SESSIONS_DIR)
        os.makedirs(session_dir_abs, exist_ok=True)
        session_path = os.path.join(session_dir_abs, session_name)

        print("\nПодключение к серверам Telegram...")

        client_kwargs = dict(
            name=session_path,
            api_id=api_id,
            api_hash=api_hash,
            phone_number=phone,
            workdir=session_dir_abs,
        )
        if proxy_dict:
            client_kwargs["proxy"] = proxy_dict

        client = Client(**client_kwargs)

        await client.connect()

        try:
            sent_code = await client.send_code(phone)
            print(f"Код отправлен на {phone} (тип: {sent_code.type})")

            code = input("Введите полученный код: ").strip()

            try:
                await client.sign_in(phone, sent_code.phone_code_hash, code)
            except SessionPasswordNeeded:
                password = input("Пароль двухэтапной аутентификации: ").strip()
                await client.check_password(password)
            except PhoneCodeInvalid:
                print("Неверный код.")
                return

            me = await client.get_me()
            print(f"\nВход выполнен: {me.first_name} (@{me.username}) ID: {me.id}")

            session = SessionMeta(
                id=0,
                alias=session_name,
                api_id=api_id,
                api_hash=api_hash,
                phone=phone,
                session_path=session_name,
                is_active=True,
                user_id=me.id,
                proxy=proxy_str,
            )
            await db.create_session(session)
            print(f"Сессия '{session_name}' сохранена в базе.")
            if proxy_str:
                print("Прокси назначен сессии и будет использоваться так же, как при настройке через менеджер сессий.")

        except Exception as e:
            print(f"Ошибка при авторизации: {e}")
        finally:
            await client.disconnect()

    except KeyboardInterrupt:
        print("\nОтменено.")
    except Exception as e:
        print(f"\nОшибка: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(add_session_cli())
