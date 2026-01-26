# -*- coding: utf-8 -*-
"""
FastAPI main application for the parser/worker service.
"""
import os
import sys
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parser.config import config
from parser.database import Database
from parser.session_manager import SessionManager
from parser.inviter_worker import InviterWorker
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from models import InviteTask, InviteSettings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
db: Database = None
session_manager: SessionManager = None
inviter_worker: InviterWorker = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global db, session_manager, inviter_worker
    
    # Startup
    logger.info("Запуск сервиса Inviter Parser...")
    
    # Initialize database
    db = Database(config.DATABASE_PATH)
    await db.connect()
    
    # Initialize session manager
    session_manager = SessionManager(db, config.SESSIONS_DIR)
    await session_manager.import_sessions_from_files()
    await session_manager.load_clients()
    
    # Initialize inviter worker
    inviter_worker = InviterWorker(db, session_manager)
    
    # Resume any running tasks
    running_tasks = await db.get_running_tasks()
    for task in running_tasks:
        logger.info(f"Resuming task {task.id}")
        await inviter_worker.start_invite_task(task.id)
        logger.info(f"Возобновление задачи {task.id}")
    
    logger.info("Сервис Inviter Parser успешно запущен")
    
    yield
    
    # Shutdown
    logger.info("Остановка сервиса Inviter Parser...")
    await session_manager.stop_all()
    await db.close()
    logger.info("Сервис Inviter Parser остановлен")


app = FastAPI(
    title="Inviter Parser Service",
    description="API for managing Telegram inviting tasks",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============== Pydantic Models ==============

class SessionInfo(BaseModel):
    alias: str
    phone: str = ""
    is_active: bool = True
    user_id: Optional[int] = None
    created_at: Optional[str] = None
    proxy: Optional[str] = None


class AddSessionRequest(BaseModel):
    alias: str
    api_id: int
    api_hash: str
    phone: str


class AssignSessionRequest(BaseModel):
    task: str


class SendCodeRequest(BaseModel):
    phone: str


class SignInRequest(BaseModel):
    phone: str
    code: str
    phone_code_hash: str


class SignInPasswordRequest(BaseModel):
    password: str


class SetSessionProxyRequest(BaseModel):
    proxy: str


class CreateTaskRequest(BaseModel):
    user_id: int
    source_group_id: int
    source_group_title: str
    source_username: Optional[str] = None
    target_group_id: int
    target_group_title: str
    target_username: Optional[str] = None
    session_alias: str
    invite_mode: str = 'member_list'
    delay_seconds: int = 30
    delay_every: int = 1
    limit: Optional[int] = None
    rotate_sessions: bool = False
    rotate_every: int = 0
    use_proxy: bool = False
    available_sessions: List[str] = []
    filter_mode: str = 'all'
    inactive_threshold_days: Optional[int] = None


class UpdateTaskRequest(BaseModel):
    delay_seconds: Optional[int] = None
    delay_every: Optional[int] = None
    limit: Optional[int] = None
    rotate_sessions: Optional[bool] = None
    rotate_sessions: Optional[bool] = None
    rotate_every: Optional[int] = None
    use_proxy: Optional[bool] = None
    available_sessions: Optional[List[str]] = None


class GroupInput(BaseModel):
    group_input: str  # username, link, or ID


class AddGroupRequest(BaseModel):
    user_id: int
    group_id: str
    group_title: str
    username: Optional[str] = None


# ============== Session Endpoints ==============

@app.get("/sessions")
async def list_sessions():
    """List all sessions."""
    sessions = await session_manager.get_all_sessions()
    assignments = await session_manager.get_assignments()
    
    return {
        "success": True,
        "sessions": [
            {
                "alias": s.alias,
                "phone": s.phone,
                "is_active": s.is_active,
                "is_active": s.is_active,
                "user_id": s.user_id,
                "created_at": s.created_at,
                "proxy": s.proxy
            }
            for s in sessions
        ],
        "assignments": assignments
    }


@app.post("/sessions")
async def add_session(request: AddSessionRequest):
    """Add a new session."""
    result = await session_manager.add_account(
        request.alias,
        request.api_id,
        request.api_hash,
        request.phone
    )
    return result


@app.delete("/sessions/{alias}")
async def delete_session(alias: str):
    """Delete a session."""
    result = await session_manager.delete_session(alias)
    return result


@app.post("/sessions/{alias}/assign")
async def assign_session(alias: str, request: AssignSessionRequest):
    """Assign a session to a task."""
    result = await session_manager.assign_task(alias, request.task)
    return result


@app.delete("/sessions/{alias}/assign/{task}")
async def remove_assignment(alias: str, task: str):
    """Remove task assignment from session."""
    result = await session_manager.remove_assignment(alias, task)
    return result


@app.post("/sessions/{alias}/send_code")
async def send_code(alias: str, request: SendCodeRequest):
    """Send authentication code."""
    result = await session_manager.send_code(alias, request.phone)
    return result


@app.post("/sessions/{alias}/sign_in")
async def sign_in(alias: str, request: SignInRequest):
    """Sign in with code."""
    result = await session_manager.sign_in(
        alias, request.phone, request.code, request.phone_code_hash
    )
    return result


@app.post("/sessions/{alias}/sign_in_password")
async def sign_in_password(alias: str, request: SignInPasswordRequest):
    """Sign in with 2FA password."""
    result = await session_manager.sign_in_with_password(alias, request.password)
    result = await session_manager.sign_in_with_password(alias, request.password)
    return result


@app.post("/sessions/{alias}/proxy")
async def set_session_proxy(alias: str, request: SetSessionProxyRequest):
    """Set proxy for a session."""
    result = await session_manager.set_session_proxy(alias, request.proxy)
    return result


@app.delete("/sessions/{alias}/proxy")
async def remove_session_proxy(alias: str):
    """Remove proxy from a session."""
    result = await session_manager.remove_session_proxy(alias)
    return result


@app.post("/sessions/{alias}/proxy/test")
async def test_session_proxy(alias: str, use_proxy: bool = True):
    """Test proxy connection for a session."""
    result = await session_manager.test_proxy_connection(alias, use_proxy)
    return result


@app.post("/sessions/copy_proxy")
async def copy_proxy(from_alias: str, to_alias: str):
    """Copy proxy configuration from one session to another."""
    result = await session_manager.copy_proxy_to_session(from_alias, to_alias)
    return result


@app.post("/sessions/init/{alias}")
async def init_session(alias: str):
    """Initialize a new session (for interactive auth in console)."""
    session = await db.get_session_by_alias(alias)
    if session:
        return {"success": False, "error": "Session already exists"}
    
    # Create placeholder session
    from models import SessionMeta
    new_session = SessionMeta(
        id=0,
        alias=alias,
        api_id=0,
        api_hash='',
        phone='',
        session_path=alias,
        is_active=False
    )
    session_id = await db.create_session(new_session)
    return {"success": True, "session_id": session_id, "alias": alias}


# ============== Group Endpoints ==============

@app.get("/groups/{session_alias}/info")
async def get_group_info(session_alias: str, group_input: str):
    """Get information about a group."""
    client = await session_manager.get_client(session_alias)
    if not client:
        raise HTTPException(status_code=400, detail="Session not available")
    
    try:
        # Handle different input formats
        if group_input.startswith('@'):
            group_input = group_input[1:]
        elif 't.me/' in group_input:
            group_input = group_input.split('t.me/')[-1].split('?')[0]
        
        # Try to get chat
        try:
            chat = await client.get_chat(group_input)
        except:
            # Try as numeric ID
            try:
                chat = await client.get_chat(int(group_input))
            except:
                return {"success": False, "error": "Group not found"}
        
        return {
            "success": True,
            "id": chat.id,
            "title": chat.title,
            "username": getattr(chat, 'username', None),
            "members_count": getattr(chat, 'members_count', None),
            "type": str(chat.type)
        }
    except Exception as e:
        logger.error(f"Ошибка получения информации о группе: {e}")
        return {"success": False, "error": str(e)}


@app.get("/groups/{session_alias}/members/{group_id}")
async def get_group_members(session_alias: str, group_id: int, limit: int = 200):
    """Get members from a group."""
    members = await session_manager.get_group_members(session_alias, group_id, limit)
    return {"success": True, "members": members, "count": len(members)}


@app.get("/groups/{session_alias}/check_access/{group_id}")
async def check_group_access(session_alias: str, group_id: int):
    """Check if session has access to a group."""
    result = await session_manager.check_group_access(session_alias, group_id)
    return result


# ============== User Groups History ==============

@app.get("/user/{user_id}/groups")
async def get_user_groups(user_id: int):
    """Get user's source group history."""
    groups = await db.get_user_groups(user_id)
    return {"success": True, "groups": groups}


@app.post("/user/{user_id}/groups")
async def add_user_group(user_id: int, request: AddGroupRequest):
    """Add group to user's history."""
    await db.add_user_group(user_id, request.group_id, request.group_title, request.username)
    return {"success": True}


@app.put("/user/{user_id}/groups/{group_id}/last_used")
async def update_group_last_used(user_id: int, group_id: str):
    """Update last used timestamp for a group."""
    await db.update_user_group_last_used(user_id, group_id)
    return {"success": True}


@app.get("/user/{user_id}/target_groups")
async def get_user_target_groups(user_id: int):
    """Get user's target group history."""
    groups = await db.get_user_target_groups(user_id)
    return {"success": True, "groups": groups}


@app.post("/user/{user_id}/target_groups")
async def add_user_target_group(user_id: int, request: AddGroupRequest):
    """Add target group to user's history."""
    await db.add_user_target_group(user_id, request.group_id, request.group_title, request.username)
    return {"success": True}


@app.put("/user/{user_id}/target_groups/{group_id}/last_used")
async def update_target_group_last_used(user_id: int, group_id: str):
    """Update last used timestamp for a target group."""
    await db.update_user_target_group_last_used(user_id, group_id)
    return {"success": True}


# ============== Invite Task Endpoints ==============

@app.post("/tasks")
async def create_task(request: CreateTaskRequest):
    """Create a new invite task."""
    task = InviteTask(
        id=0,
        user_id=request.user_id,
        source_group_id=request.source_group_id,
        source_group_title=request.source_group_title,
        source_username=request.source_username,
        target_group_id=request.target_group_id,
        target_group_title=request.target_group_title,
        target_username=request.target_username,
        session_alias=request.session_alias,
        invite_mode=request.invite_mode,
        delay_seconds=request.delay_seconds,
        delay_every=request.delay_every,
        limit=request.limit,
        rotate_sessions=request.rotate_sessions,
        rotate_every=request.rotate_every,
        use_proxy=request.use_proxy,
        available_sessions=request.available_sessions,
        filter_mode=request.filter_mode,
        inactive_threshold_days=request.inactive_threshold_days
    )
    task_id = await db.create_invite_task(task)
    return {"success": True, "task_id": task_id}


@app.get("/tasks/{task_id}")
async def get_task(task_id: int):
    """Get task details."""
    result = await inviter_worker.get_task_status(task_id)
    return result


@app.get("/tasks/user/{user_id}")
async def get_user_tasks(user_id: int, status: Optional[str] = None):
    """Get all tasks for a user."""
    tasks = await db.get_user_invite_tasks(user_id, status)
    return {
        "success": True,
        "tasks": [
            {
                "id": t.id,
                "source_group": t.source_group_title,
                "target_group": t.target_group_title,
                "session": t.session_alias,
                "status": t.status,
                "invited_count": t.invited_count,
                "limit": t.limit,
                "rotate_sessions": t.rotate_sessions,
                "rotate_every": t.rotate_every,
                "use_proxy": t.use_proxy,
                "created_at": t.created_at
            }
            for t in tasks
        ]
    }


@app.put("/tasks/{task_id}")
async def update_task(task_id: int, request: UpdateTaskRequest):
    """Update task settings."""
    updates = {}
    if request.delay_seconds is not None:
        updates['delay_seconds'] = request.delay_seconds
    if request.delay_every is not None:
        updates['delay_every'] = request.delay_every
    if request.limit is not None:
        updates['invite_limit'] = request.limit
    if request.rotate_sessions is not None:
        updates['rotate_sessions'] = 1 if request.rotate_sessions else 0
    if request.rotate_every is not None:
        updates['rotate_every'] = request.rotate_every
    if request.use_proxy is not None:
        updates['use_proxy'] = 1 if request.use_proxy else 0
    if request.available_sessions is not None:
        updates['available_sessions'] = ','.join(request.available_sessions)
    
    if updates:
        await db.update_invite_task(task_id, **updates)
    
    return {"success": True}


@app.post("/tasks/{task_id}/start")
async def start_task(task_id: int):
    """Start an invite task."""
    result = await inviter_worker.start_invite_task(task_id)
    return result


@app.post("/tasks/{task_id}/stop")
async def stop_task(task_id: int):
    """Stop an invite task."""
    result = await inviter_worker.stop_invite_task(task_id)
    return result


@app.delete("/tasks/{task_id}")
async def delete_task(task_id: int):
    """Delete a task."""
    # Stop if running
    await inviter_worker.stop_invite_task(task_id)
    await db.delete_invite_task(task_id)
    return {"success": True}


@app.get("/tasks/{task_id}/history")
async def get_task_history(task_id: int):
    """Get invite history for a task."""
    history = await db.get_task_invite_history(task_id)
    return {"success": True, "history": history}


@app.get("/running_tasks")
async def get_running_tasks():
    """Get all running tasks."""
    tasks = await inviter_worker.get_all_running_tasks()
    return {"success": True, "tasks": tasks}


# ============== Health Check ==============

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "inviter-parser"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)
