
def _format_validation_info(task_data: Dict) -> str:
    """Format session validation information."""
    validated_sessions = task_data.get('validated_sessions', [])
    validation_errors = task_data.get('validation_errors')
    
    # Check if we have anything to show
    if not validation_errors and not validated_sessions:
        # If both are empty, check if we have failed status and error message related to sessions
        if task_data.get('status') == 'failed' and "valid sessions" in str(task_data.get('error_message', '')):
             pass # Will be shown in error message
        else:
             return ""

    if not validation_errors and not validated_sessions:
        return ""

    text = ""
    
    # Show validated count if we have errors (to show some passed), OR if all passed but we want to confirm
    # Actually, showing validated count is good always if available
    if validated_sessions:
        if isinstance(validated_sessions, str): 
            try:
                validated_sessions = validated_sessions.split(',')
            except:
                validated_sessions = []
        if validated_sessions and isinstance(validated_sessions, list):
             # Don't show if list is huge? 
             # Just show "Checked: X sessions" is better than list if we list them above in "Sessions"
             pass

    # MAIN GOAL: Show errors
    if validation_errors:
        import json
        if isinstance(validation_errors, str):
            try:
                validation_errors = json.loads(validation_errors)
            except:
                validation_errors = {}
        
        if validation_errors and isinstance(validation_errors, dict):
            text += "\n🚫 **Ошибки валидации сессий:**\n"
            limit = 5
            count = 0
            for session, error in validation_errors.items():
                # Translate common errors
                err_msg = str(error)
                if "No access" in err_msg: err_msg = "Нет доступа"
                elif "Connection failed" in err_msg: err_msg = "Ошибка подключения"
                elif "Session failed" in err_msg: err_msg = "Сбой сессии"
                elif "users mismatch" in err_msg: err_msg = "Неверные права"
                
                text += f"- {session}: {err_msg}\n"
                count += 1
                if count >= limit:
                    remaining = len(validation_errors) - limit
                    if remaining > 0:
                        text += f" и еще {remaining}...\n"
                    break
    
    return text
