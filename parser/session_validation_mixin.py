
    async def validate_sessions_for_task(self, task_type: str, task: Any) -> Dict[str, Any]:
        """
        Validate sessions for a task (pre-start check).
        Checks if sessions have access to source/target chats.
        
        Args:
            task_type: 'invite', 'parse', 'post_parse', 'post_monitoring'
            task: Task object with fields: available_sessions, source_id/group_id, etc.
            
        Returns:
            Dict with 'valid': List[str], 'invalid': Dict[str, str] (alias -> error)
        """
        valid = []
        invalid = {}
        
        # Determine sessions to check
        sessions_to_check = task.available_sessions if task.available_sessions else []
        if not sessions_to_check:
             return {"valid": [], "invalid": {"global": "No sessions assigned"}}

        for alias in sessions_to_check:
            try:
                client = await self.get_client(alias, use_proxy=task.use_proxy)
                if not client:
                    invalid[alias] = "Session failed to initialize"
                    continue
                
                # Check connection (client start is handled in get_client if not started)
                if not client.is_connected:
                     # get_client should have started it. If not, it failed.
                     invalid[alias] = "Client not connected"
                     continue
                
                # Validation based on task type
                if task_type == 'invite':
                    # Source check (only if not file mode)
                    if task.invite_mode != 'from_file':
                         src = await ensure_peer_resolved(client, task.source_group_id, task.source_username)
                         if not src:
                             invalid[alias] = "No access to source group"
                             continue
                         # Try join
                         await self.join_chat_if_needed(client, task.source_group_id, task.source_username)
                    
                    # Target check
                    dst = await ensure_peer_resolved(client, task.target_group_id, task.target_username)
                    if not dst:
                        invalid[alias] = "No access to target group"
                        continue
                    await self.join_chat_if_needed(client, task.target_group_id, task.target_username)
                    
                elif task_type == 'parse':
                    # Source check
                    src = await ensure_peer_resolved(client, task.source_group_id, task.source_username)
                    if not src:
                        invalid[alias] = "No access to source group"
                        continue
                    await self.join_chat_if_needed(client, task.source_group_id, task.source_username)
                    
                    # Check member list visibility for member_list mode
                    if task.parse_mode == 'member_list':
                        try:
                             # Try to get 1 member to check privileges
                             # get_chat_members(limit=1)
                             has_members = False
                             async for _ in client.get_chat_members(task.source_group_id, limit=1):
                                 has_members = True
                                 break
                             if not has_members:
                                 # It might be empty group or hidden members
                                 # We can't distinguish easily without more info, but assuming empty is fine
                                 pass
                        except Exception as e:
                            invalid[alias] = f"Cannot fetch members: {e}"
                            continue

                elif task_type in ['post_parse', 'post_monitoring']:
                    # Source check
                    src = await ensure_peer_resolved(client, task.source_id, task.source_username)
                    if not src:
                        invalid[alias] = "No access to source channel"
                        continue
                    await self.join_chat_if_needed(client, task.source_id, task.source_username)
                    
                    # Target check
                    dst = await ensure_peer_resolved(client, task.target_id, task.target_username)
                    if not dst:
                        invalid[alias] = "No access to target channel"
                        continue
                    await self.join_chat_if_needed(client, task.target_id, task.target_username)

                valid.append(alias)

            except Exception as e:
                invalid[alias] = str(e)
        
        return {"valid": valid, "invalid": invalid}
