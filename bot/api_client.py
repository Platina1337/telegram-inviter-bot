# -*- coding: utf-8 -*-
"""
API client for communicating with the parser service.
"""
import httpx
import logging
import time
import asyncio
from typing import Dict, List, Optional, Any

from bot.config import config

logger = logging.getLogger(__name__)

# Cache storage for API responses
_api_cache: Dict[str, tuple] = {}  # {key: (value, expire_time)}
CACHE_TTL_SECONDS = 5  # Default cache TTL


def _get_cached(key: str) -> Optional[Any]:
    """Get value from cache if not expired."""
    if key in _api_cache:
        value, expire_time = _api_cache[key]
        if time.time() < expire_time:
            return value
        del _api_cache[key]
    return None


def _set_cached(key: str, value: Any, ttl: int = CACHE_TTL_SECONDS):
    """Set value in cache with TTL."""
    _api_cache[key] = (value, time.time() + ttl)


def _invalidate_cache(prefix: str = None):
    """Invalidate cache entries. If prefix provided, only matching keys."""
    global _api_cache
    if prefix:
        _api_cache = {k: v for k, v in _api_cache.items() if not k.startswith(prefix)}
    else:
        _api_cache.clear()


class APIClient:
    """HTTP client for the parser service API with connection pooling."""
    
    _client: Optional[httpx.AsyncClient] = None
    _client_lock: asyncio.Lock = None
    
    def __init__(self):
        self.base_url = config.PARSER_SERVICE_URL
        self.timeout = 60.0
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the shared HTTP client with connection pooling."""
        if APIClient._client_lock is None:
            APIClient._client_lock = asyncio.Lock()
        
        async with APIClient._client_lock:
            if APIClient._client is None or APIClient._client.is_closed:
                APIClient._client = httpx.AsyncClient(
                    timeout=self.timeout,
                    limits=httpx.Limits(
                        max_keepalive_connections=10,
                        max_connections=20,
                        keepalive_expiry=30.0
                    )
                )
        return APIClient._client
    
    async def close(self):
        """Close the shared HTTP client."""
        if APIClient._client and not APIClient._client.is_closed:
            await APIClient._client.aclose()
            APIClient._client = None
    
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request to API using connection pool."""
        url = f"{self.base_url}{endpoint}"
        try:
            client = await self._get_client()
            response = await client.request(method, url, **kwargs)
            
            if response.status_code >= 400:
                logger.error(f"API error: {response.status_code} - {response.text}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
            
            return response.json()
        except httpx.TimeoutException:
            logger.error(f"Timeout requesting {url}")
            return {"success": False, "error": "Request timeout"}
        except httpx.ConnectError:
            logger.error(f"Connection error to {url}")
            return {"success": False, "error": "Connection error"}
        except Exception as e:
            logger.error(f"Request error: {e}")
            return {"success": False, "error": str(e)}
    
    # ============== Sessions ==============
    
    async def list_sessions(self) -> Dict[str, Any]:
        """Get list of all sessions (cached for 5 seconds)."""
        cache_key = "list_sessions"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached
        
        result = await self._make_request("GET", "/sessions")
        if result.get("success"):
            _set_cached(cache_key, result, ttl=5)
        return result
    
    async def add_session(self, alias: str, api_id: int, api_hash: str, phone: str) -> Dict[str, Any]:
        """Add a new session."""
        return await self._make_request("POST", "/sessions", json={
            "alias": alias,
            "api_id": api_id,
            "api_hash": api_hash,
            "phone": phone
        })
    
    async def delete_session(self, alias: str) -> Dict[str, Any]:
        """Delete a session."""
        return await self._make_request("DELETE", f"/sessions/{alias}")
    
    async def assign_session(self, task: str, alias: str) -> Dict[str, Any]:
        """Assign a session to a task."""
        return await self._make_request("POST", f"/sessions/{alias}/assign", json={"task": task})
    
    async def remove_assignment(self, task: str, alias: str) -> Dict[str, Any]:
        """Remove task assignment from session."""
        return await self._make_request("DELETE", f"/sessions/{alias}/assign/{task}")
    
    async def send_code(self, alias: str, phone: str) -> Dict[str, Any]:
        """Send authentication code."""
        return await self._make_request("POST", f"/sessions/{alias}/send_code", json={"phone": phone})
    
    async def sign_in(self, alias: str, phone: str, code: str, phone_code_hash: str) -> Dict[str, Any]:
        """Sign in with code."""
        return await self._make_request("POST", f"/sessions/{alias}/sign_in", json={
            "phone": phone,
            "code": code,
            "phone_code_hash": phone_code_hash
        })
    
    async def sign_in_password(self, alias: str, password: str) -> Dict[str, Any]:
        """Sign in with 2FA password."""
        return await self._make_request("POST", f"/sessions/{alias}/sign_in_password", 
                                        json={"password": password})
    
    async def init_session(self, alias: str) -> Dict[str, Any]:
        """Initialize a new session."""
        return await self._make_request("POST", f"/sessions/init/{alias}")
    
    async def set_session_proxy(self, alias: str, proxy: str) -> Dict[str, Any]:
        """Set proxy for a session."""
        return await self._make_request("POST", f"/sessions/{alias}/proxy", json={"proxy": proxy})

    async def remove_session_proxy(self, alias: str) -> Dict[str, Any]:
        """Remove proxy from a session."""
        return await self._make_request("DELETE", f"/sessions/{alias}/proxy")

    async def test_session_proxy(self, alias: str, use_proxy: bool = True) -> Dict[str, Any]:
        """Test proxy connection for a session."""
        return await self._make_request("POST", f"/sessions/{alias}/proxy/test", params={"use_proxy": use_proxy})

    async def copy_session_proxy(self, from_alias: str, to_alias: str) -> Dict[str, Any]:
        """Copy proxy configuration from one session to another."""
        return await self._make_request("POST", "/sessions/copy_proxy",
                                      params={"from_alias": from_alias, "to_alias": to_alias})

    # ============== Groups ==============
    
    async def get_group_info(self, session_alias: str, group_input: str) -> Dict[str, Any]:
        """Get information about a group."""
        return await self._make_request("GET", f"/groups/{session_alias}/info", 
                                        params={"group_input": group_input})
    
    async def get_group_members(self, session_alias: str, group_id: int, limit: int = 200) -> Dict[str, Any]:
        """Get members from a group."""
        return await self._make_request("GET", f"/groups/{session_alias}/members/{group_id}",
                                        params={"limit": limit})
    
    async def check_group_access(self, session_alias: str, group_id: int) -> Dict[str, Any]:
        """Check if session has access to a group."""
        return await self._make_request("GET", f"/groups/{session_alias}/check_access/{group_id}")
    
    # ============== User Groups History ==============
    
    async def get_user_groups(self, user_id: int) -> List[Dict]:
        """Get user's source group history (cached for 10 seconds)."""
        cache_key = f"user_groups:{user_id}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached
        
        result = await self._make_request("GET", f"/user/{user_id}/groups")
        groups = result.get("groups", [])
        _set_cached(cache_key, groups, ttl=10)
        return groups
    
    async def add_user_group(self, user_id: int, group_id: str, title: str, username: str = None) -> Dict[str, Any]:
        """Add group to user's history."""
        # Invalidate cache
        _invalidate_cache(f"user_groups:{user_id}")
        return await self._make_request("POST", f"/user/{user_id}/groups", json={
            "user_id": user_id,
            "group_id": group_id,
            "group_title": title,
            "username": username
        })
    
    async def update_user_group_last_used(self, user_id: int, group_id: str) -> Dict[str, Any]:
        """Update last used timestamp for a group."""
        _invalidate_cache(f"user_groups:{user_id}")
        return await self._make_request("PUT", f"/user/{user_id}/groups/{group_id}/last_used")
    
    async def get_user_target_groups(self, user_id: int) -> List[Dict]:
        """Get user's target group history (cached for 10 seconds)."""
        cache_key = f"user_target_groups:{user_id}"
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached
        
        result = await self._make_request("GET", f"/user/{user_id}/target_groups")
        groups = result.get("groups", [])
        _set_cached(cache_key, groups, ttl=10)
        return groups
    
    async def add_user_target_group(self, user_id: int, group_id: str, title: str, username: str = None) -> Dict[str, Any]:
        """Add target group to user's history."""
        # Invalidate cache
        _invalidate_cache(f"user_target_groups:{user_id}")
        return await self._make_request("POST", f"/user/{user_id}/target_groups", json={
            "user_id": user_id,
            "group_id": group_id,
            "group_title": title,
            "username": username
        })
    
    async def update_user_target_group_last_used(self, user_id: int, group_id: str) -> Dict[str, Any]:
        """Update last used timestamp for a target group."""
        _invalidate_cache(f"user_target_groups:{user_id}")
        return await self._make_request("PUT", f"/user/{user_id}/target_groups/{group_id}/last_used")
    
    # ============== Invite Tasks ==============
    
    async def create_task(self, user_id: int, source_group_id: int, source_group_title: str,
                          target_group_id: int, target_group_title: str, session_alias: str,
                          source_username: str = None, target_username: str = None,
                          invite_mode: str = 'member_list',
                          file_source: str = None,
                          delay_seconds: int = 30, delay_every: int = 1, limit: int = None, 
                          rotate_sessions: bool = False, rotate_every: int = 0,
                          use_proxy: bool = False,
                          available_sessions: List[str] = None,
                          filter_mode: str = 'all',
                          inactive_threshold_days: int = None) -> Dict[str, Any]:
        """Create a new invite task."""
        return await self._make_request("POST", "/tasks", json={
            "user_id": user_id,
            "source_group_id": source_group_id,
            "source_group_title": source_group_title,
            "source_username": source_username,
            "target_group_id": target_group_id,
            "target_group_title": target_group_title,
            "target_username": target_username,
            "session_alias": session_alias,
            "invite_mode": invite_mode,
            "file_source": file_source,
            "delay_seconds": delay_seconds,
            "delay_every": delay_every,
            "limit": limit,
            "rotate_sessions": rotate_sessions,
            "rotate_every": rotate_every,
            "use_proxy": use_proxy,
            "available_sessions": available_sessions or [],
            "filter_mode": filter_mode,
            "inactive_threshold_days": inactive_threshold_days
        })

    
    async def get_task(self, task_id: int) -> Dict[str, Any]:
        """Get task details."""
        return await self._make_request("GET", f"/tasks/{task_id}")
    
    async def get_user_tasks(self, user_id: int, status: str = None) -> Dict[str, Any]:
        """Get all tasks for a user."""
        params = {"status": status} if status else {}
        return await self._make_request("GET", f"/tasks/user/{user_id}", params=params)
    
    async def update_task(self, task_id: int, delay_seconds: int = None, delay_every: int = None,
                          limit: int = None, rotate_sessions: bool = None, rotate_every: int = None,
                          use_proxy: bool = None,
                          available_sessions: List[str] = None) -> Dict[str, Any]:
        """Update task settings."""
        data = {}
        if delay_seconds is not None:
            data['delay_seconds'] = delay_seconds
        if delay_every is not None:
            data['delay_every'] = delay_every
        if limit is not None:
            data['limit'] = limit
        if rotate_sessions is not None:
            data['rotate_sessions'] = rotate_sessions
        if rotate_every is not None:
            data['rotate_every'] = rotate_every
        if use_proxy is not None:
            data['use_proxy'] = use_proxy
        if available_sessions is not None:
            data['available_sessions'] = available_sessions
        
        return await self._make_request("PUT", f"/tasks/{task_id}", json=data)
    
    async def start_task(self, task_id: int) -> Dict[str, Any]:
        """Start an invite task."""
        return await self._make_request("POST", f"/tasks/{task_id}/start")
    
    async def stop_task(self, task_id: int) -> Dict[str, Any]:
        """Stop an invite task."""
        return await self._make_request("POST", f"/tasks/{task_id}/stop")
    
    async def delete_task(self, task_id: int) -> Dict[str, Any]:
        """Delete a task."""
        return await self._make_request("DELETE", f"/tasks/{task_id}")
    
    async def get_task_history(self, task_id: int) -> Dict[str, Any]:
        """Get invite history for a task."""
        return await self._make_request("GET", f"/tasks/{task_id}/history")
    
    async def get_running_tasks(self) -> Dict[str, Any]:
        """Get all running tasks."""
        return await self._make_request("GET", "/running_tasks")
    
    # ============== Parse Tasks ==============
    
    async def create_parse_task(self, user_id: int, file_name: str,
                                source_group_id: int, source_group_title: str,
                                session_alias: str,
                                source_username: str = None,
                                source_type: str = "group",  # "group" or "channel"
                                delay_seconds: int = 2, limit: int = None,
                                save_every: int = 0,
                                rotate_sessions: bool = False, rotate_every: int = 0,
                                use_proxy: bool = True,
                                available_sessions: List[str] = None,
                                filter_admins: bool = False,
                                filter_inactive: bool = False,
                                inactive_threshold_days: int = 30,
                                parse_mode: str = "member_list",
                                keyword_filter: List[str] = None,
                                exclude_keywords: List[str] = None,
                                # Message-based mode specific params
                                messages_limit: int = None,
                                delay_every_requests: int = 1,
                                rotate_every_requests: int = 0,
                                save_every_users: int = 0) -> Dict[str, Any]:
        """Create a new parse task."""
        return await self._make_request("POST", "/parse_tasks", json={
            "user_id": user_id,
            "file_name": file_name,
            "source_group_id": source_group_id,
            "source_group_title": source_group_title,
            "source_username": source_username,
            "source_type": source_type,  # Add source type
            "session_alias": session_alias,
            "delay_seconds": delay_seconds,
            "limit": limit,
            "save_every": save_every,
            "rotate_sessions": rotate_sessions,
            "rotate_every": rotate_every,
            "use_proxy": use_proxy,
            "available_sessions": available_sessions or [],
            "filter_admins": filter_admins,
            "filter_inactive": filter_inactive,
            "inactive_threshold_days": inactive_threshold_days,
            "parse_mode": parse_mode,
            "keyword_filter": keyword_filter or [],
            "exclude_keywords": exclude_keywords or [],
            "messages_limit": messages_limit,
            "delay_every_requests": delay_every_requests,
            "rotate_every_requests": rotate_every_requests,
            "save_every_users": save_every_users
        })
    
    async def get_parse_task(self, task_id: int) -> Dict[str, Any]:
        """Get parse task details."""
        return await self._make_request("GET", f"/parse_tasks/{task_id}")
    
    async def get_user_parse_tasks(self, user_id: int, status: str = None) -> Dict[str, Any]:
        """Get all parse tasks for a user."""
        params = {"status": status} if status else {}
        return await self._make_request("GET", f"/parse_tasks/user/{user_id}", params=params)
    
    async def start_parse_task(self, task_id: int) -> Dict[str, Any]:
        """Start a parse task."""
        return await self._make_request("POST", f"/parse_tasks/{task_id}/start")
    
    async def stop_parse_task(self, task_id: int) -> Dict[str, Any]:
        """Stop a parse task."""
        return await self._make_request("POST", f"/parse_tasks/{task_id}/stop")
    
    async def delete_parse_task(self, task_id: int) -> Dict[str, Any]:
        """Delete a parse task."""
        return await self._make_request("DELETE", f"/parse_tasks/{task_id}")
    
    # ============== Health Check ==============
    
    async def health_check(self) -> Dict[str, Any]:
        """Check parser service health."""
        return await self._make_request("GET", "/health")


# Global API client instance
api_client = APIClient()
