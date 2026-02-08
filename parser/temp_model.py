
class UpdateParseTaskRequest(BaseModel):
    """Request model for updating parse task settings."""
    delay_seconds: Optional[int] = None
    limit: Optional[int] = None
    save_every: Optional[int] = None
    rotate_sessions: Optional[bool] = None
    rotate_every: Optional[int] = None
    use_proxy: Optional[bool] = None
    available_sessions: Optional[List[str]] = None
    filter_admins: Optional[bool] = None
    filter_inactive: Optional[bool] = None
    inactive_threshold_days: Optional[int] = None
    parse_mode: Optional[str] = None
    keyword_filter: Optional[List[str]] = None
    exclude_keywords: Optional[List[str]] = None
    messages_limit: Optional[int] = None
    delay_every_requests: Optional[int] = None
    rotate_every_requests: Optional[int] = None
    save_every_users: Optional[int] = None
