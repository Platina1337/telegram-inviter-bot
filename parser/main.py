# -*- coding: utf-8 -*-
"""
FastAPI main application for the parser/worker service.
"""
import os
import sys
import time
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
from parser.parser_worker import ParserWorker
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
parser_worker: ParserWorker = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global db, session_manager, inviter_worker, parser_worker
    
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
    
    # Initialize parser worker
    parser_worker = ParserWorker(db, session_manager)
    
    # Resume any running invite tasks
    running_tasks = await db.get_running_tasks()
    for task in running_tasks:
        logger.info(f"Resuming invite task {task.id}")
        await inviter_worker.start_invite_task(task.id)
    
    # Resume any running parse tasks
    running_parse_tasks = await db.get_running_parse_tasks()
    for task in running_parse_tasks:
        logger.info(f"Resuming parse task {task.id}")
        await parser_worker.start_parse_task(task.id)
    
    logger.info("Сервис Inviter Parser успешно запущен")
    
    yield
    
    # ============== Graceful Shutdown ==============
    logger.info("Остановка сервиса Inviter Parser...")
    
    # Stop all running invite tasks gracefully
    try:
        running_invite_tasks = await db.get_running_tasks()
        for task in running_invite_tasks:
            logger.info(f"Gracefully stopping invite task {task.id}...")
            try:
                await inviter_worker.stop_invite_task(task.id)
                # Mark as paused so it can resume on restart
                await db.update_invite_task(task.id, status='paused')
                logger.info(f"Invite task {task.id} paused for graceful shutdown")
            except Exception as e:
                logger.error(f"Error stopping invite task {task.id}: {e}")
    except Exception as e:
        logger.error(f"Error during invite tasks shutdown: {e}")
    
    # Stop all running parse tasks gracefully
    try:
        running_parse_tasks = await db.get_running_parse_tasks()
        for task in running_parse_tasks:
            logger.info(f"Gracefully stopping parse task {task.id}...")
            try:
                await parser_worker.stop_parse_task(task.id)
                # Mark as paused so it can resume on restart
                await db.update_parse_task(task.id, status='paused')
                logger.info(f"Parse task {task.id} paused for graceful shutdown")
            except Exception as e:
                logger.error(f"Error stopping parse task {task.id}: {e}")
    except Exception as e:
        logger.error(f"Error during parse tasks shutdown: {e}")
    
    # Give tasks time to finish current operation
    await asyncio.sleep(1)
    
    # Stop session manager
    await session_manager.stop_all()
    
    # Close database
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

# Rate limiting middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from parser.rate_limiter import rate_limiter

class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # Use client IP as identifier (or default for local)
        client_id = request.client.host if request.client else "default"
        
        is_allowed, retry_after = rate_limiter.is_allowed(client_id)
        
        if not is_allowed:
            return JSONResponse(
                status_code=429,
                content={"error": "Too many requests", "retry_after": retry_after},
                headers={"Retry-After": str(int(retry_after) + 1)}
            )
        
        return await call_next(request)

app.add_middleware(RateLimitMiddleware)


# Error handling middleware for centralized error logging
import traceback

class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        try:
            response = await call_next(request)
            return response
        except Exception as e:
            # Log the full error with traceback
            error_id = f"{int(time.time())}"
            logger.error(f"[Error ID: {error_id}] Unhandled exception in {request.method} {request.url.path}: {e}")
            logger.error(f"[Error ID: {error_id}] Traceback: {traceback.format_exc()}")
            
            # Return a generic error response
            return JSONResponse(
                status_code=500,
                content={
                    "success": False,
                    "error": "Internal server error",
                    "error_id": error_id
                }
            )


app.add_middleware(ErrorHandlingMiddleware)

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
    file_source: Optional[str] = None
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


class CreateParseTaskRequest(BaseModel):
    user_id: int
    file_name: str
    source_group_id: int
    source_group_title: str
    source_username: Optional[str] = None
    source_type: str = "group"  # "group" or "channel"
    session_alias: str
    delay_seconds: int = 2
    limit: Optional[int] = None
    save_every: int = 0  # Save to file after every N users (0 = only at end)
    rotate_sessions: bool = False
    rotate_every: int = 0
    use_proxy: bool = True
    available_sessions: List[str] = []
    filter_admins: bool = False
    filter_inactive: bool = False
    inactive_threshold_days: int = 30
    # Parse mode: member_list (default) or message_based
    parse_mode: str = "member_list"
    # Keyword filter for message_based mode
    keyword_filter: List[str] = []
    # Exclude keywords
    exclude_keywords: List[str] = []
    # Message-based mode specific fields
    messages_limit: Optional[int] = None
    delay_every_requests: int = 1
    rotate_every_requests: int = 0
    save_every_users: int = 0


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
        api_id=config.API_ID,
        api_hash=config.API_HASH,
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
    # Проверяем наличие сессии в БД (на хосте сессии могут быть не импортированы)
    session_meta = await db.get_session_by_alias(session_alias)
    if not session_meta:
        logger.warning(f"GET /groups/{session_alias}/info: сессия не найдена в БД (нет файла в sessions/ или другой БД)")
        raise HTTPException(status_code=404, detail="Session not found. Add session on parser host or copy sessions folder and DB.")
    client = await session_manager.get_client(session_alias)
    if not client:
        logger.warning(f"GET /groups/{session_alias}/info: сессия в БД есть, но клиент недоступен (не удалось запустить)")
        raise HTTPException(status_code=503, detail="Session not available (client failed to start). Check session file and auth on parser host.")
    
    try:
        # Handle different input formats
        if group_input.startswith('@'):
            group_input = group_input[1:]
        elif 't.me/' in group_input:
            group_input = group_input.split('t.me/')[-1].split('?')[0]
        
        # Try to get chat
        try:
            chat = await client.get_chat(group_input)
        except Exception as e:
            logger.debug(f"Failed to get chat by input '{group_input}': {e}")
            # Try as numeric ID
            try:
                chat = await client.get_chat(int(group_input))
            except Exception as e2:
                logger.debug(f"Failed to get chat by numeric ID '{group_input}': {e2}")
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
async def get_group_members(session_alias: str, group_id: int, limit: int = 200, offset: int = 0):
    """Get members from a group."""
    members = await session_manager.get_group_members(session_alias, group_id, limit, offset)
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
        file_source=request.file_source,
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


# ============== Parse Task Endpoints ==============

@app.post("/parse_tasks")
async def create_parse_task(request: CreateParseTaskRequest):
    """Create a new parse task."""
    from models import ParseTask
    
    task = ParseTask(
        id=0,
        user_id=request.user_id,
        file_name=request.file_name,
        source_group_id=request.source_group_id,
        source_group_title=request.source_group_title,
        source_username=request.source_username,
        source_type=request.source_type,  # Add source type
        session_alias=request.session_alias,
        delay_seconds=request.delay_seconds,
        limit=request.limit,
        save_every=request.save_every,
        rotate_sessions=request.rotate_sessions,
        rotate_every=request.rotate_every,
        use_proxy=request.use_proxy,
        available_sessions=request.available_sessions,
        filter_admins=request.filter_admins,
        filter_inactive=request.filter_inactive,
        inactive_threshold_days=request.inactive_threshold_days,
        parse_mode=request.parse_mode,
        keyword_filter=request.keyword_filter,
        exclude_keywords=request.exclude_keywords,
        messages_limit=request.messages_limit,
        delay_every_requests=request.delay_every_requests,
        rotate_every_requests=request.rotate_every_requests,
        save_every_users=request.save_every_users
    )
    task_id = await db.create_parse_task(task)
    return {"success": True, "task_id": task_id}


@app.get("/parse_tasks/{task_id}")
async def get_parse_task(task_id: int):
    """Get parse task details."""
    task = await db.get_parse_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "success": True,
        "task": {
            "id": task.id,
            "user_id": task.user_id,
            "file_name": task.file_name,
            "source_group": task.source_group_title,
            "source_type": getattr(task, 'source_type', 'group'),
            "session": task.session_alias,
            "status": task.status,
            "parsed_count": task.parsed_count,
            "saved_count": task.saved_count,
            "limit": task.limit,
            "delay_seconds": task.delay_seconds,
            "save_every": task.save_every,
            "rotate_sessions": task.rotate_sessions,
            "rotate_every": task.rotate_every,
            "use_proxy": task.use_proxy,
            "available_sessions": task.available_sessions,
            "filter_admins": task.filter_admins,
            "filter_inactive": task.filter_inactive,
            "inactive_threshold_days": task.inactive_threshold_days,
            "parse_mode": task.parse_mode,
            "keyword_filter": task.keyword_filter,
            "exclude_keywords": task.exclude_keywords,
            "messages_limit": task.messages_limit,
            "delay_every_requests": task.delay_every_requests,
            "rotate_every_requests": task.rotate_every_requests,
            "save_every_users": task.save_every_users,
            "messages_offset": task.messages_offset,
            "created_at": task.created_at,
            "error_message": task.error_message,
            "delay_every": task.delay_every,
            "last_action_time": task.last_action_time,
            "current_session": task.current_session
        }
    }


@app.get("/parse_tasks/user/{user_id}")
async def get_user_parse_tasks(user_id: int, status: Optional[str] = None):
    """Get all parse tasks for a user."""
    tasks = await db.get_user_parse_tasks(user_id, status)
    return {
        "success": True,
        "tasks": [
            {
                "id": t.id,
                "file_name": t.file_name,
                "source_group": t.source_group_title,
                "session": t.session_alias,
                "status": t.status,
                "parsed_count": t.parsed_count,
                "saved_count": t.saved_count,
                "limit": t.limit,
                "save_every": t.save_every,
                "rotate_sessions": t.rotate_sessions,
                "rotate_every": t.rotate_every,
                "use_proxy": t.use_proxy,
                "available_sessions": t.available_sessions,
                "filter_admins": t.filter_admins,
                "filter_inactive": t.filter_inactive,
                "inactive_threshold_days": t.inactive_threshold_days,
                "parse_mode": t.parse_mode,
                "keyword_filter": t.keyword_filter,
                "exclude_keywords": t.exclude_keywords,
                "messages_limit": t.messages_limit,
                "delay_every_requests": t.delay_every_requests,
                "rotate_every_requests": t.rotate_every_requests,
                "save_every_users": t.save_every_users,
                "messages_offset": t.messages_offset,
                "created_at": t.created_at
            }
            for t in tasks
        ]
    }


@app.post("/parse_tasks/{task_id}/start")
async def start_parse_task(task_id: int):
    """Start a parse task."""
    result = await parser_worker.start_parse_task(task_id)
    return result


@app.post("/parse_tasks/{task_id}/stop")
async def stop_parse_task(task_id: int):
    """Stop a parse task."""
    result = await parser_worker.stop_parse_task(task_id)
    return result


@app.delete("/parse_tasks/{task_id}")
async def delete_parse_task(task_id: int):
    """Delete a parse task."""
    # Stop if running
    task = await db.get_parse_task(task_id)
    if task and task.status == 'running':
        await db.update_parse_task(task_id, status='paused')
    
    await db.delete_parse_task(task_id)
    return {"success": True}


# ============== Health Check ==============

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "inviter-parser"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)
