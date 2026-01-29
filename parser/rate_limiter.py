# -*- coding: utf-8 -*-
"""
Rate limiting middleware for FastAPI.
"""
import time
import asyncio
from typing import Dict, Optional
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Simple in-memory rate limiter.
    
    Uses a sliding window algorithm to limit requests per client.
    """
    
    def __init__(self, requests_per_minute: int = 60, requests_per_second: int = 10):
        self.requests_per_minute = requests_per_minute
        self.requests_per_second = requests_per_second
        self.minute_requests: Dict[str, list] = defaultdict(list)
        self.second_requests: Dict[str, list] = defaultdict(list)
        self._cleanup_task = None
    
    def _cleanup_old_requests(self, client_id: str, window_seconds: int, storage: dict):
        """Remove requests older than the window."""
        now = time.time()
        cutoff = now - window_seconds
        storage[client_id] = [t for t in storage[client_id] if t > cutoff]
    
    def is_allowed(self, client_id: str = "default") -> tuple[bool, Optional[float]]:
        """
        Check if request is allowed for this client.
        
        Returns:
            Tuple of (is_allowed, retry_after_seconds)
        """
        now = time.time()
        
        # Cleanup old requests
        self._cleanup_old_requests(client_id, 60, self.minute_requests)
        self._cleanup_old_requests(client_id, 1, self.second_requests)
        
        # Check per-second limit
        if len(self.second_requests[client_id]) >= self.requests_per_second:
            oldest = self.second_requests[client_id][0]
            retry_after = 1 - (now - oldest)
            if retry_after > 0:
                return False, retry_after
        
        # Check per-minute limit
        if len(self.minute_requests[client_id]) >= self.requests_per_minute:
            oldest = self.minute_requests[client_id][0]
            retry_after = 60 - (now - oldest)
            if retry_after > 0:
                return False, retry_after
        
        # Record this request
        self.minute_requests[client_id].append(now)
        self.second_requests[client_id].append(now)
        
        return True, None
    
    def get_stats(self, client_id: str = "default") -> dict:
        """Get current rate limit stats for a client."""
        now = time.time()
        self._cleanup_old_requests(client_id, 60, self.minute_requests)
        self._cleanup_old_requests(client_id, 1, self.second_requests)
        
        return {
            "requests_last_minute": len(self.minute_requests[client_id]),
            "requests_last_second": len(self.second_requests[client_id]),
            "limit_per_minute": self.requests_per_minute,
            "limit_per_second": self.requests_per_second
        }


# Global rate limiter instance
rate_limiter = RateLimiter(requests_per_minute=120, requests_per_second=20)


async def check_rate_limit(client_id: str = "default") -> tuple[bool, Optional[float]]:
    """
    Async wrapper for rate limit check.
    
    Returns:
        Tuple of (is_allowed, retry_after_seconds)
    """
    return rate_limiter.is_allowed(client_id)
