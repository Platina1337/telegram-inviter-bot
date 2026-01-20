# -*- coding: utf-8 -*-
"""
CLI script to add a Telegram session interactively via terminal.
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
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from models import SessionMeta


async def add_session_cli():
    print("=" * 40)
    print("Telegram Session Adder CLI")
    print("=" * 40)
    print("This script will create a .session file and add it to the database.\n")
    
    # Init DB
    db = Database(config.DATABASE_PATH)
    await db.connect()
    
    try:
        # Input
        session_name = input("Enter session name (alias): ").strip()
        if not session_name:
            print("Session name required.")
            return

        # Check existing
        existing = await db.get_session_by_alias(session_name)
        if existing:
            print(f"Session '{session_name}' already exists in DB.")
            return

        api_id_str = input("Enter API ID (my.telegram.org): ").strip()
        api_hash = input("Enter API Hash (my.telegram.org): ").strip()
        phone = input("Enter Phone Number (+12345...): ").strip()
        
        if not api_id_str.isdigit():
            print("API ID must be an integer.")
            return
        
        api_id = int(api_id_str)
        
        # Session path
        session_dir_abs = os.path.abspath(config.SESSIONS_DIR)
        os.makedirs(session_dir_abs, exist_ok=True)
        session_path = os.path.join(session_dir_abs, session_name)
        
        print("\nConnecting to Telegram servers...")
        
        client = Client(
            name=session_path,
            api_id=api_id,
            api_hash=api_hash,
            phone_number=phone,
            workdir=session_dir_abs
        )
        
        await client.connect()
        
        try:
            sent_code = await client.send_code(phone)
            print(f"Code sent to {phone} (type: {sent_code.type})")
            
            code = input("Enter the code you received: ").strip()
            
            try:
                await client.sign_in(phone, sent_code.phone_code_hash, code)
            except SessionPasswordNeeded:
                password = input("Two-Step Verification Password: ").strip()
                await client.check_password(password)
            except PhoneCodeInvalid:
                print("Invalid code.")
                return
            
            me = await client.get_me()
            print(f"\nSuccessfully logged in as: {me.first_name} (@{me.username}) ID: {me.id}")
            
            # Save to DB
            session = SessionMeta(
                id=0,
                alias=session_name,
                api_id=api_id,
                api_hash=api_hash,
                phone=phone,
                session_path=session_name,
                is_active=True,
                user_id=me.id
            )
            await db.create_session(session)
            print(f"Session '{session_name}' saved to database.")
            
        except Exception as e:
            print(f"Error during auth: {e}")
        finally:
            await client.disconnect()
            
    except KeyboardInterrupt:
        print("\nCancelled.")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(add_session_cli())
