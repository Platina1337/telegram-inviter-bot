# -*- coding: utf-8 -*-
"""
Manager for user files (parsed users storage).
"""
import os
import json
import logging
from typing import List, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class UserFilesManager:
    """Manages user files for parsed users."""
    
    def __init__(self, base_dir: str = "user_files"):
        """Initialize manager with base directory."""
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)
    
    def save_users_to_file(self, filename: str, users: List[Dict], metadata: Dict = None) -> str:
        """
        Save users to a JSON file.
        
        Args:
            filename: Name of the file (without extension)
            users: List of user dictionaries
            metadata: Optional metadata about the parsing
            
        Returns:
            Full path to the saved file
        """
        # Sanitize filename
        safe_filename = "".join(c for c in filename if c.isalnum() or c in ('_', '-', ' ')).strip()
        if not safe_filename:
            safe_filename = f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        filepath = os.path.join(self.base_dir, f"{safe_filename}.json")
        
        # Prepare data
        data = {
            "created_at": datetime.now().isoformat(),
            "metadata": metadata or {},
            "users": users,
            "count": len(users)
        }
        
        # Save to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved {len(users)} users to {filepath}")
        return filepath
    
    def append_users_to_file(self, filename: str, users: List[Dict], metadata: Dict = None) -> tuple[str, int]:
        """
        Append users to an existing file or create new one.
        
        Args:
            filename: Name of the file (without extension)
            users: List of user dictionaries to append
            metadata: Optional metadata about the parsing
            
        Returns:
            Tuple of (filepath, total_count)
        """
        # Sanitize filename
        safe_filename = "".join(c for c in filename if c.isalnum() or c in ('_', '-', ' ')).strip()
        if not safe_filename:
            safe_filename = f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        filepath = os.path.join(self.base_dir, f"{safe_filename}.json")
        
        existing_users = []
        existing_user_ids = set()
        
        # Try to load existing file
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                existing_users = data.get('users', [])
                existing_user_ids = {u.get('id') for u in existing_users if u.get('id')}
                logger.info(f"Loaded {len(existing_users)} existing users from {filepath}")
            except Exception as e:
                logger.warning(f"Could not load existing file {filepath}: {e}")
        
        # Filter out duplicates and append new users
        new_users = [u for u in users if u.get('id') not in existing_user_ids]
        all_users = existing_users + new_users
        
        # Prepare data
        data = {
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "metadata": metadata or {},
            "users": all_users,
            "count": len(all_users)
        }
        
        # Save to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Appended {len(new_users)} new users to {filepath} (total: {len(all_users)})")
        return filepath, len(all_users)
    
    def get_saved_user_ids(self, filename: str) -> set:
        """
        Get set of user IDs already saved in a file.
        Used to skip already parsed users when resuming a task.
        
        Args:
            filename: Name of the file (without extension)
            
        Returns:
            Set of user IDs
        """
        # Sanitize filename
        safe_filename = "".join(c for c in filename if c.isalnum() or c in ('_', '-', ' ')).strip()
        if not safe_filename:
            return set()
        
        filepath = os.path.join(self.base_dir, f"{safe_filename}.json")
        
        if not os.path.exists(filepath):
            return set()
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            users = data.get('users', [])
            user_ids = {u.get('id') for u in users if u.get('id')}
            logger.info(f"Loaded {len(user_ids)} existing user IDs from {filepath}")
            return user_ids
        except Exception as e:
            logger.warning(f"Could not load user IDs from {filepath}: {e}")
            return set()
    
    def load_users_from_file(self, filename: str) -> Optional[Dict]:
        """
        Load users from a JSON file.
        
        Args:
            filename: Name of the file (with or without .json extension)
            
        Returns:
            Dictionary with users and metadata, or None if file not found
        """
        # Add .json if not present
        if not filename.endswith('.json'):
            filename = f"{filename}.json"
        
        filepath = os.path.join(self.base_dir, filename)
        
        if not os.path.exists(filepath):
            logger.error(f"File not found: {filepath}")
            return None
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Loaded {data.get('count', 0)} users from {filepath}")
            return data
        except Exception as e:
            logger.error(f"Error loading file {filepath}: {e}")
            return None
    
    def list_user_files(self) -> List[Dict]:
        """
        List all available user files.
        
        Returns:
            List of dictionaries with file info
        """
        files = []
        
        if not os.path.exists(self.base_dir):
            return files
        
        for filename in os.listdir(self.base_dir):
            if not filename.endswith('.json'):
                continue
            
            filepath = os.path.join(self.base_dir, filename)
            
            try:
                # Get file stats
                stat = os.stat(filepath)
                
                # Try to load metadata
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                files.append({
                    "filename": filename,
                    "name": filename[:-5],  # Without .json
                    "created_at": data.get("created_at", "Unknown"),
                    "count": data.get("count", 0),
                    "metadata": data.get("metadata", {}),
                    "size_bytes": stat.st_size
                })
            except Exception as e:
                logger.error(f"Error reading file {filename}: {e}")
                continue
        
        # Sort by creation date (newest first)
        files.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        
        return files
    
    def delete_file(self, filename: str) -> bool:
        """
        Delete a user file.
        
        Args:
            filename: Name of the file (with or without .json extension)
            
        Returns:
            True if deleted successfully, False otherwise
        """
        if not filename.endswith('.json'):
            filename = f"{filename}.json"
        
        filepath = os.path.join(self.base_dir, filename)
        
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
                logger.info(f"Deleted file: {filepath}")
                return True
            else:
                logger.warning(f"File not found: {filepath}")
                return False
        except Exception as e:
            logger.error(f"Error deleting file {filepath}: {e}")
            return False
    
    def copy_file(self, source_filename: str, dest_filename: str) -> Optional[str]:
        """
        Create a copy of a user file with a new name.
        
        Args:
            source_filename: Name of the source file (with or without .json extension)
            dest_filename: Name for the new file (without extension)
            
        Returns:
            Path to the new file, or None on error
        """
        # Normalize source filename
        if not source_filename.endswith('.json'):
            source_filename = f"{source_filename}.json"
        
        source_path = os.path.join(self.base_dir, source_filename)
        
        if not os.path.exists(source_path):
            logger.error(f"Source file not found: {source_path}")
            return None
        
        # Sanitize destination filename
        safe_dest = "".join(c for c in dest_filename if c.isalnum() or c in ('_', '-', ' ')).strip()
        if not safe_dest:
            safe_dest = f"copy_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        dest_path = os.path.join(self.base_dir, f"{safe_dest}.json")
        
        # If destination exists, add suffix
        counter = 1
        original_dest = safe_dest
        while os.path.exists(dest_path):
            safe_dest = f"{original_dest}_{counter}"
            dest_path = os.path.join(self.base_dir, f"{safe_dest}.json")
            counter += 1
        
        try:
            with open(source_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Update metadata
            data['created_at'] = datetime.now().isoformat()
            data['updated_at'] = datetime.now().isoformat()
            if 'metadata' not in data:
                data['metadata'] = {}
            data['metadata']['copied_from'] = source_filename[:-5]  # Remove .json
            data['metadata']['copy_date'] = datetime.now().isoformat()
            
            with open(dest_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Copied file {source_path} to {dest_path}")
            return dest_path
        except Exception as e:
            logger.error(f"Error copying file: {e}")
            return None
    
    def rename_file(self, old_filename: str, new_filename: str) -> Optional[str]:
        """
        Rename a user file.
        
        Args:
            old_filename: Current name of the file (with or without .json extension)
            new_filename: New name for the file (without extension)
            
        Returns:
            Path to the renamed file, or None on error
        """
        # Normalize old filename
        if not old_filename.endswith('.json'):
            old_filename = f"{old_filename}.json"
        
        old_path = os.path.join(self.base_dir, old_filename)
        
        if not os.path.exists(old_path):
            logger.error(f"File not found: {old_path}")
            return None
        
        # Sanitize new filename
        safe_new = "".join(c for c in new_filename if c.isalnum() or c in ('_', '-', ' ')).strip()
        if not safe_new:
            logger.error("Invalid new filename")
            return None
        
        new_path = os.path.join(self.base_dir, f"{safe_new}.json")
        
        # Check if destination exists
        if os.path.exists(new_path):
            logger.error(f"File already exists: {new_path}")
            return None
        
        try:
            os.rename(old_path, new_path)
            logger.info(f"Renamed file {old_path} to {new_path}")
            return new_path
        except Exception as e:
            logger.error(f"Error renaming file: {e}")
            return None
    
    def filter_users_in_file(self, filename: str, filter_type: str, **kwargs) -> Dict:
        """
        Filter users in a file based on criteria and save the result.
        
        Args:
            filename: Name of the file (with or without .json extension)
            filter_type: Type of filter to apply:
                - 'remove_no_username': Remove users without username
                - 'remove_no_first_name': Remove users without first_name
                - 'keep_with_username': Keep only users with username
                - 'remove_duplicates': Remove duplicate user IDs
                - 'remove_by_keyword': Remove users whose name contains keyword
                - 'keep_by_keyword': Keep only users whose name contains keyword
            **kwargs: Additional arguments for filters (e.g., keyword='text')
            
        Returns:
            Dict with 'success', 'original_count', 'new_count', 'removed_count'
        """
        if not filename.endswith('.json'):
            filename = f"{filename}.json"
        
        filepath = os.path.join(self.base_dir, filename)
        
        if not os.path.exists(filepath):
            return {'success': False, 'error': 'File not found'}
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            users = data.get('users', [])
            original_count = len(users)
            
            if filter_type == 'remove_no_username':
                users = [u for u in users if u.get('username')]
            
            elif filter_type == 'remove_no_first_name':
                users = [u for u in users if u.get('first_name')]
            
            elif filter_type == 'keep_with_username':
                users = [u for u in users if u.get('username')]
            
            elif filter_type == 'remove_duplicates':
                seen_ids = set()
                unique_users = []
                for u in users:
                    uid = u.get('id')
                    if uid and uid not in seen_ids:
                        seen_ids.add(uid)
                        unique_users.append(u)
                users = unique_users
            
            elif filter_type == 'remove_by_keyword':
                keyword = kwargs.get('keyword', '').lower()
                if keyword:
                    users = [u for u in users if keyword not in (
                        (u.get('first_name', '') or '') + ' ' + (u.get('last_name', '') or '')
                    ).lower()]
            
            elif filter_type == 'keep_by_keyword':
                keyword = kwargs.get('keyword', '').lower()
                if keyword:
                    users = [u for u in users if keyword in (
                        (u.get('first_name', '') or '') + ' ' + (u.get('last_name', '') or '')
                    ).lower()]
            
            else:
                return {'success': False, 'error': f'Unknown filter type: {filter_type}'}
            
            new_count = len(users)
            removed_count = original_count - new_count
            
            # Update file
            data['users'] = users
            data['count'] = new_count
            data['updated_at'] = datetime.now().isoformat()
            if 'metadata' not in data:
                data['metadata'] = {}
            data['metadata']['last_filter'] = {
                'type': filter_type,
                'date': datetime.now().isoformat(),
                'removed_count': removed_count
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Filtered file {filename}: {original_count} -> {new_count} users (removed {removed_count})")
            
            return {
                'success': True,
                'original_count': original_count,
                'new_count': new_count,
                'removed_count': removed_count
            }
        
        except Exception as e:
            logger.error(f"Error filtering file {filename}: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_file_stats(self, filename: str) -> Optional[Dict]:
        """
        Get detailed statistics about a user file.
        
        Args:
            filename: Name of the file (with or without .json extension)
            
        Returns:
            Dict with file statistics, or None on error
        """
        if not filename.endswith('.json'):
            filename = f"{filename}.json"
        
        filepath = os.path.join(self.base_dir, filename)
        
        if not os.path.exists(filepath):
            return None
        
        try:
            stat = os.stat(filepath)
            
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            users = data.get('users', [])
            
            # Calculate statistics
            with_username = sum(1 for u in users if u.get('username'))
            with_first_name = sum(1 for u in users if u.get('first_name'))
            with_last_name = sum(1 for u in users if u.get('last_name'))
            
            # Check for unique IDs
            ids = [u.get('id') for u in users if u.get('id')]
            unique_ids = len(set(ids))
            duplicates = len(ids) - unique_ids
            
            return {
                'filename': filename,
                'name': filename[:-5],
                'size_bytes': stat.st_size,
                'created_at': data.get('created_at'),
                'updated_at': data.get('updated_at'),
                'metadata': data.get('metadata', {}),
                'total_users': len(users),
                'with_username': with_username,
                'without_username': len(users) - with_username,
                'with_first_name': with_first_name,
                'with_last_name': with_last_name,
                'unique_ids': unique_ids,
                'duplicates': duplicates
            }
        except Exception as e:
            logger.error(f"Error getting file stats: {e}")
            return None
