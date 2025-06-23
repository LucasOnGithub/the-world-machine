import os
import asyncio
import discord
from discord.ext import commands
from dotenv import load_dotenv
from config.config import Config

load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.presences = True

class Bot(commands.Bot):
    def __init__(self):
        def get_prefix(bot, message):
            prefixes = ['pls ']
            return commands.when_mentioned_or(*prefixes)(bot, message)
            
        super().__init__(
            command_prefix=get_prefix,
            intents=intents,
            help_command=None,
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name=f"my program start..."
            )
        )
        self._first_run = True
        self.excluded_guild_id = 1313847386925170778
        self.initial_extensions = [
            'cogs.tossing',
            'cogs.moderation',
            'cogs.kane_commands',
            'cogs.apartment_rooms',
            'cogs.auto_slowmode',
            'cogs.basic',
            'cogs.vc_tts',
            'cogs.gif_logger',
            'cogs.config_cog',
            'cogs.welcome_cog',
            'cogs.bot_reports',
            'cogs.ssc_ranking_commands',

        ]

    async def setup_hook(self):
        """Load extensions and setup tasks."""
        for ext in self.initial_extensions:
            try:
                await self.load_extension(ext)
                print(f'Loaded extension: {ext}')
            except Exception as e:
                print(f'Failed to load extension {ext}: {e}')
        

        await self.tree.sync()
        print("Commands synced globally")
        

        self.bg_task = self.loop.create_task(self.update_member_count_loop())
        
    async def on_ready(self):
        """Called when the bot is ready."""
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        await self.update_member_count()
        
    async def on_command_error(self, ctx, error):
        """Handle command errors globally."""
        from discord.ext import commands
        from config.errors import ErrorMessages
        
        # Get the command's cog if it exists
        if ctx.command and hasattr(ctx.command, 'cog') and ctx.command.cog:
            # Let the cog handle its own errors first
            if hasattr(ctx.command.cog, 'cog_command_error'):
                # Skip if the error is already handled by the cog
                if not getattr(error, 'handled', False):
                    return await ctx.command.cog.cog_command_error(ctx, error)
        
        # Global error handling for commands without cog-specific handlers
        if isinstance(error, commands.MissingPermissions):
            await ctx.send(ErrorMessages.get_error('missing_perms'))
        elif isinstance(error, commands.NoPrivateMessage):
            await ctx.send(ErrorMessages.get_error('servers_only'))
        else:
            # Log other errors
            print(f"Error in command {ctx.command}: {error}")
            await ctx.send(ErrorMessages.get_error('generic'))
    
    async def update_member_count_loop(self):
        """Background task to update member count every minute."""
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await self.update_member_count()
                await asyncio.sleep(60)
            except Exception as e:
                print(f"Error in update loop: {e}")
                await asyncio.sleep(60)
    
    async def on_member_join(self, member):
        """Update member count when a new member joins."""
        await self.update_member_count()
    
    async def on_member_remove(self, member):
        """Update member count when a member leaves."""
        await self.update_member_count()
    
    async def update_member_count(self):
        """Update the bot's activity with the current member count."""
        total_members = set()
        total_guilds = 0
        
        if self._first_run:
            print("\n=== Starting member count update ===")
            print(f"Bot is in {len(self.guilds)} guilds")
        
        for guild in self.guilds:
            if guild.id == self.excluded_guild_id:
                if self._first_run:
                    print(f"\nSkipping excluded guild: {guild.name} ({guild.id})")
                continue
                
            total_guilds += 1
            
            cached_members = {member.id for member in guild.members if not member.bot}
            
            try:
                if self._first_run:
                    print(f"\nChecking guild: {guild.name} ({guild.id})")
                    print(f"Cached members: {len(guild.members)} (Non-bot: {len(cached_members)})")
                    print("Fetching fresh member data...")
                
                fetched_members = set()
                async for member in guild.fetch_members(limit=None):
                    if not member.bot:
                        fetched_members.add(member.id)
                
                guild_members = cached_members.union(fetched_members)
                
                if self._first_run:
                    print(f"Total unique non-bot members: {len(guild_members)}")
                
                total_members.update(guild_members)
                
            except Exception as e:
                total_members.update(cached_members)
                if not self._first_run:
                    print(f"Error updating member count: {e}")
        
        total_count = len(total_members)
        
        if self._first_run:
            print(f"\n=== Initial count: {total_count} total members across {total_guilds} servers ===")
            self._first_run = False
        
        activity = discord.Activity(
            type=discord.ActivityType.watching,
            name=f"{total_count} members in {total_guilds} servers"
        )
        await self.change_presence(activity=activity)


bot = Bot()


if __name__ == '__main__':
    Config.validate_config()
    bot.run(Config.TOKEN)
