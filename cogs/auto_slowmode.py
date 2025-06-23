import json
import os
import time
from typing import Dict, List, Optional, Deque
from collections import deque, defaultdict
from datetime import datetime, timedelta

import discord
from discord.ext import commands, tasks
from discord import app_commands

from config.config import Config
from config.errors import ErrorMessages

# Constants
SLOWMODE_DATA_FILE = "auto_slowmode.json"
DEFAULT_SENSITIVITY = 5.0
MIN_SENSITIVITY = 1.0
MAX_SENSITIVITY = 10.0
DEFAULT_MIN_SLOWMODE = 0
DEFAULT_MAX_SLOWMODE = 30
DEFAULT_CACHE_SIZE = 15

class AutoSlowmodeSettings:
    """Stores settings for auto slowmode in a channel."""
    
    def __init__(self, channel_id: int, sensitivity: float = DEFAULT_SENSITIVITY):
        self.channel_id = channel_id
        self.sensitivity = max(MIN_SENSITIVITY, min(MAX_SENSITIVITY, sensitivity))
        self.enabled = True
        self.message_timestamps: Deque[float] = deque(maxlen=DEFAULT_CACHE_SIZE)
        self.last_slowmode = 0

    def to_dict(self) -> dict:
        """Convert settings to a dictionary for JSON serialization."""
        return {
            'channel_id': self.channel_id,
            'sensitivity': self.sensitivity,
            'enabled': self.enabled,
            'last_slowmode': self.last_slowmode
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'AutoSlowmodeSettings':
        """Create settings from a dictionary."""
        settings = cls(data['channel_id'], data.get('sensitivity', DEFAULT_SENSITIVITY))
        settings.enabled = data.get('enabled', True)
        settings.last_slowmode = data.get('last_slowmode', 0)
        return settings

class AutoSlowmode(commands.Cog):
    """Automatic slowmode management based on message frequency."""
    
    def __init__(self, bot):
        self.bot = bot
        # channel_id -> AutoSlowmodeSettings
        self.channel_settings: Dict[int, AutoSlowmodeSettings] = {}
        self.load_data()
        
        # Start the background task
        self.update_slowmodes.start()
    
    def cog_unload(self):
        """Stop the background task when the cog is unloaded."""
        self.update_slowmodes.cancel()
    
    def load_data(self) -> None:
        """Load auto slowmode data from JSON file."""
        if not os.path.exists(SLOWMODE_DATA_FILE):
            return
            
        try:
            with open(SLOWMODE_DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            for channel_data in data.get('channels', []):
                settings = AutoSlowmodeSettings.from_dict(channel_data)
                self.channel_settings[settings.channel_id] = settings
                
        except Exception as e:
            print(f"Error loading auto slowmode data: {e}")
    
    def save_data(self) -> None:
        """Save auto slowmode data to JSON file."""
        data = {
            'channels': [settings.to_dict() for settings in self.channel_settings.values()]
        }
        
        try:
            with open(SLOWMODE_DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"Error saving auto slowmode data: {e}")
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        """Track message timestamps for auto slowmode."""
        # Ignore bots and system messages
        if message.author.bot or not message.guild or not isinstance(message.author, discord.Member):
            return
            
        # Check if auto slowmode is enabled for this channel
        if message.channel.id not in self.channel_settings:
            return
            
        settings = self.channel_settings[message.channel.id]
        if not settings.enabled:
            return
            
        # Record the message timestamp
        settings.message_timestamps.append(time.time())
    
    @tasks.loop(seconds=30.0)
    async def update_slowmodes(self) -> None:
        """Update slowmode settings for all monitored channels."""
        for channel_id, settings in list(self.channel_settings.items()):
            if not settings.enabled:
                continue
                
            channel = self.bot.get_channel(channel_id)
            if not channel or not isinstance(channel, discord.TextChannel):
                continue
                
            try:
                # Calculate message rate (messages per minute)
                now = time.time()
                messages_last_minute = sum(1 for ts in settings.message_timestamps if now - ts < 60)
                
                # Calculate desired slowmode based on message rate and sensitivity
                # Higher sensitivity = slower response to message rate changes
                if messages_last_minute <= 1:
                    # No messages or very slow, set to minimum
                    new_slowmode = DEFAULT_MIN_SLOWMODE
                else:
                    # Scale slowmode based on message rate and sensitivity
                    # This formula can be adjusted based on desired behavior
                    rate_factor = min(messages_last_minute / (settings.sensitivity * 2), 3.0)
                    new_slowmode = min(DEFAULT_MAX_SLOWMODE, int(rate_factor * 5))
                
                # Only update if the slowmode would change
                if new_slowmode != settings.last_slowmode:
                    await channel.edit(slowmode_delay=new_slowmode)
                    settings.last_slowmode = new_slowmode
                    
            except Exception as e:
                print(f"Error updating slowmode for channel {channel_id}: {e}")
    
    @update_slowmodes.before_loop
    async def before_update_slowmodes(self) -> None:
        """Wait until the bot is ready before starting the task."""
        await self.bot.wait_until_ready()
    
    @commands.hybrid_group(name="slowmode", invoke_without_command=True)
    @commands.guild_only()
    @commands.has_permissions(manage_channels=True)
    async def slowmode_group(self, ctx: commands.Context) -> None:
        """Manage auto slowmode settings for this channel (Staff only)."""
        # Check if user is staff
        from config.config import Config
        if not Config.is_staff(ctx.author):
            await ctx.send("❌ This command is restricted to staff members only.", ephemeral=True)
            return
            
        await ctx.send_help(ctx.command)
    
    @slowmode_group.command(name="add")
    @app_commands.describe(sensitivity="Sensitivity level (1.0-10.0, default: 5.0)")
    @commands.guild_only()
    @commands.has_permissions(manage_channels=True)
    async def slowmode_add(self, ctx: commands.Context, sensitivity: float = DEFAULT_SENSITIVITY) -> None:
        """Enable auto slowmode for this channel with the given sensitivity (Staff only)."""
        # Check if user is staff
        from config.config import Config
        if not Config.is_staff(ctx.author):
            await ctx.send("❌ This command is restricted to staff members only.", ephemeral=True)
            return
        sensitivity = max(MIN_SENSITIVITY, min(MAX_SENSITIVITY, sensitivity))
        
        if ctx.channel.id in self.channel_settings:
            settings = self.channel_settings[ctx.channel.id]
            settings.enabled = True
            settings.sensitivity = sensitivity
            await ctx.send(f"✅ Updated auto slowmode settings for this channel.\n"
                         f"Sensitivity: {sensitivity:.3f}", ephemeral=True)
        else:
            settings = AutoSlowmodeSettings(ctx.channel.id, sensitivity)
            self.channel_settings[ctx.channel.id] = settings
            await ctx.send(f"✅ Enabled auto slowmode for this channel.\n"
                         f"Sensitivity: {sensitivity:.3f}\n"
                         f"Min/Max: {DEFAULT_MIN_SLOWMODE}/{DEFAULT_MAX_SLOWMODE}s\n"
                         f"Cache Size: {DEFAULT_CACHE_SIZE}", ephemeral=True)
        
        self.save_data()
    
    @slowmode_group.command(name="remove")
    @commands.guild_only()
    @commands.has_permissions(manage_channels=True)
    async def slowmode_remove(self, ctx: commands.Context) -> None:
        """Disable auto slowmode for this channel (Staff only)."""
        # Check if user is staff
        from config.config import Config
        if not Config.is_staff(ctx.author):
            await ctx.send("❌ This command is restricted to staff members only.", ephemeral=True)
            return
        if ctx.channel.id not in self.channel_settings:
            await ctx.send("❌ Auto slowmode is not enabled in this channel.", ephemeral=True)
            return
            
        settings = self.channel_settings[ctx.channel.id]
        if not settings.enabled:
            await ctx.send("❌ Auto slowmode is already disabled in this channel.", ephemeral=True)
            return
            
        settings.enabled = False
        self.save_data()
        
        # Reset slowmode to 0 when disabling
        try:
            await ctx.channel.edit(slowmode_delay=0)
            await ctx.send("✅ Disabled auto slowmode for this channel and reset slowmode to 0s.", ephemeral=True)
        except Exception as e:
            await ctx.send(f"✅ Disabled auto slowmode, but failed to reset slowmode: {e}", ephemeral=True)
    
    @slowmode_group.command(name="set")
    @app_commands.describe(sensitivity="New sensitivity level (1.0-10.0)")
    @commands.guild_only()
    @commands.has_permissions(manage_channels=True)
    async def slowmode_set_sensitivity(self, ctx: commands.Context, sensitivity: float) -> None:
        """Set the sensitivity for auto slowmode in this channel (Staff only)."""
        # Check if user is staff
        from config.config import Config
        if not Config.is_staff(ctx.author):
            await ctx.send("❌ This command is restricted to staff members only.", ephemeral=True)
            return
        if ctx.channel.id not in self.channel_settings:
            await ctx.send("❌ Auto slowmode is not enabled in this channel. Use `pls slowmode add` first.", ephemeral=True)
            return
            
        sensitivity = max(MIN_SENSITIVITY, min(MAX_SENSITIVITY, sensitivity))
        settings = self.channel_settings[ctx.channel.id]
        settings.sensitivity = sensitivity
        self.save_data()
        
        await ctx.send(f"✅ Set auto slowmode sensitivity to {sensitivity:.3f} for this channel.", ephemeral=True)
    
    @slowmode_group.command(name="status")
    @commands.guild_only()
    @commands.has_permissions(manage_channels=True)
    async def slowmode_status(self, ctx: commands.Context) -> None:
        """Show the current auto slowmode status for this channel (Staff only)."""
        # Check if user is staff
        from config.config import Config
        if not Config.is_staff(ctx.author):
            await ctx.send("❌ This command is restricted to staff members only.", ephemeral=True)
            return
        if ctx.channel.id not in self.channel_settings:
            await ctx.send("❌ Auto slowmode is not enabled in this channel.", ephemeral=True)
            return
            
        settings = self.channel_settings[ctx.channel.id]
        status = "🟢 Enabled" if settings.enabled else "🔴 Disabled"
        
        embed = discord.Embed(
            title="Auto Slowmode Status",
            description=f"**Status:** {status}\n"
                      f"**Sensitivity:** {settings.sensitivity:.3f}\n"
                      f"**Current Slowmode:** {ctx.channel.slowmode_delay}s\n"
                      f"**Min/Max:** {DEFAULT_MIN_SLOWMODE}s/{DEFAULT_MAX_SLOWMODE}s\n"
                      f"**Cache Size:** {DEFAULT_CACHE_SIZE}",
            color=discord.Color.blue()
        )
        
        await ctx.send(embed=embed, ephemeral=True)

async def setup(bot):
    await bot.add_cog(AutoSlowmode(bot))
