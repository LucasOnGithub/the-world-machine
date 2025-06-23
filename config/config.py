from typing import Dict, List, Optional, TypedDict, Any, Union
import discord
import sqlite3
import yaml

# Database setup
def get_db_connection():
    conn = sqlite3.connect('guild_configs.db')
    conn.row_factory = sqlite3.Row
    return conn

class Config:
    # Bot settings
    TOKEN: str = ''  # Your bot token here
    MANAGER_IDS: List[int] = [1125692090123309056]  # Manager user ID
    
    # Default config structure
    DEFAULT_CONFIG = {
        'staff_roles': [],
        'toss_category': None,
        'toss_logs': None,
        'voice_category': None,
        'apartment_lobby': None,
        'owner_roles': {}
    }
    
    @classmethod
    def get_guild_config(cls, guild_id: int) -> Dict[str, Any]:
        """Get the config for a guild."""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT config_data FROM guild_configs WHERE guild_id = ?', (guild_id,))
            row = cursor.fetchone()
            
            if row:
                return yaml.safe_load(row['config_data'])
            return cls.DEFAULT_CONFIG.copy()
            
    @classmethod
    def get_guild_setting(cls, guild_id: int, key: str, default: Any = None) -> Any:
        """Get a specific setting for a guild."""
        config = cls.get_guild_config(guild_id)
        return config.get(key, default)
        
    @classmethod
    def get_owner_roles(cls, guild_id: int) -> Dict[int, int]:
        """Get owner roles for a guild."""
        return cls.get_guild_setting(guild_id, 'owner_roles', {})
    
    @classmethod
    def get_protection_level(cls, member: discord.Member) -> str:
        """Get the protection level of a user.
        
        Returns:
            str: 'owner', 'co_owner', 'staff', or None
        """
        # Check if user is a manager or server owner (highest protection)
        if member.id in cls.MANAGER_IDS or member.id == member.guild.owner_id:
            return 'owner'
            
        # Get owner roles for this guild
        owner_roles = cls.get_owner_roles(member.guild.id)
        
        # Check if user is an owner or co-owner
        if member.id in owner_roles or member.id in owner_roles.values():
            return 'owner' if member.id in owner_roles else 'co_owner'
            
        # Check if user is staff
        if cls.is_staff(member):
            return 'staff'
            
        return None
    
    @classmethod
    def can_moderate(cls, moderator: discord.Member, target: discord.Member) -> tuple[bool, str]:
        """Check if moderator can moderate the target user.
        
        Returns:
            tuple: (can_moderate: bool, reason: str)
        """
        # No one can moderate themselves
        if moderator.id == target.id:
            return False, "You cannot moderate yourself."
            
        # Check if moderator is a bot manager (can moderate anyone)
        if moderator.id in cls.MANAGER_IDS:
            return True, "Bot manager"
            
        # Server owner can moderate anyone
        if moderator.id == moderator.guild.owner_id:
            return True, "Server owner"
            
        # Get owner roles for this guild
        owner_roles = cls.get_owner_roles(moderator.guild.id)
        
        # Check if moderator is an owner
        if moderator.id in owner_roles:
            # Owners can't moderate other owners or co-owners
            if target.id in owner_roles or target.id in owner_roles.values():
                return False, "Cannot moderate other owners/co-owners"
            return True, "Owner"
            
        # Check if moderator is a co-owner
        if moderator.id in owner_roles.values():
            # Co-owners can only moderate non-owners and non-co-owners
            if target.id in owner_roles or target.id in owner_roles.values():
                return False, "Cannot moderate owners/co-owners"
            return True, "Co-owner"
            
        # Check if moderator is staff
        if cls.is_staff(moderator):
            # Staff can only moderate non-staff, non-owners, and non-co-owners
            if cls.is_staff(target) or target.id in owner_roles or target.id in owner_roles.values():
                return False, "Cannot moderate other staff/owners/co-owners"
            return True, "Staff"
            
        return False, "Insufficient permissions"
    
    @classmethod
    def get_token(cls) -> str:
        """Get the bot token."""
        token = cls.TOKEN
        if not token or token == 'your_token_here':
            raise ValueError('Please set your bot token in config.py')
        return token
    
    @classmethod
    def get_toss_category(cls, guild_id: int) -> Optional[int]:
        """Get the toss category ID for a guild."""
        return cls.get_guild_setting(guild_id, 'toss_category')
    
    @classmethod
    def get_toss_logs(cls, guild_id: int) -> Optional[int]:
        """Get the toss logs channel ID for a guild."""
        return cls.get_guild_setting(guild_id, 'toss_logs')
    
    @classmethod
    def get_voice_category(cls, guild_id: int) -> Optional[int]:
        """Get the voice category ID for a guild."""
        return cls.get_guild_setting(guild_id, 'voice_category')
    
    @classmethod
    def get_apartment_lobby(cls, guild_id: int) -> Optional[int]:
        """Get the apartment lobby channel ID for a guild."""
        return cls.get_guild_setting(guild_id, 'apartment_lobby')
    
    @classmethod
    def is_staff(cls, member: discord.Member) -> bool:
        """Check if a member has staff permissions."""
        # Check if member is the server owner
        if member.guild.owner_id == member.id:
            return True
            
        # Check if member has any staff roles
        staff_roles = cls.get_guild_setting(member.guild.id, 'staff_roles', [])
        return any(role.id in staff_roles for role in member.roles)
    
    @classmethod
    def validate_config(cls):
        """Validate required configuration."""
        if not cls.TOKEN or cls.TOKEN == 'e':
            raise ValueError('Please set your bot token in config.py')
        return None

    # Colors
    EMBED_COLOR: int = 0x3498db  # Blue
    ERROR_COLOR: int = 0xe74c3c  # Red
    SUCCESS_COLOR: int = 0x2ecc71  # Green
