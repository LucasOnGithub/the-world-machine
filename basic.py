import discord
from discord.ext import commands
from discord import app_commands
from googlesearch import search
import asyncio
import sqlite3
import re
from datetime import datetime
import pytz
from typing import Optional, Tuple, Union, Dict, List
import aiohttp
import json
import os

# Timezone aliases cache
timezone_aliases: Dict[str, str] = {}

async def load_timezone_aliases() -> Dict[str, str]:
    """Load timezone aliases from moment-timezone data"""
    global timezone_aliases
    
    if timezone_aliases:
        return timezone_aliases
        
    url = "https://cdnjs.cloudflare.com/ajax/libs/moment-timezone/0.5.25/moment-timezone-with-data.min.js"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    js_content = await response.text()
                    # Extract the links data from the JS file
                    links_match = re.search(r'links:\[([^\]]+)\]', js_content)
                    if links_match:
                        links_str = links_match.group(1)
                        # Parse the links array
                        for link in re.findall(r'"([^"]+)\|([^"]+)"', links_str):
                            alias, tz = link
                            timezone_aliases[alias.lower()] = tz
                    
                    # Add common aliases
                    common_aliases = {
                        'est': 'America/New_York',
                        'edt': 'America/New_York',
                        'pst': 'America/Los_Angeles',
                        'pdt': 'America/Los_Angeles',
                        'cst': 'America/Chicago',
                        'cdt': 'America/Chicago',
                        'mst': 'America/Denver',
                        'mdt': 'America/Denver',
                        'gmt': 'GMT',
                        'utc': 'UTC'
                    }
                    timezone_aliases.update(common_aliases)
                    
    except Exception as e:
        print(f"Error loading timezone aliases: {e}")
        # Fallback to common aliases if loading fails
        timezone_aliases = {
            'est': 'America/New_York',
            'edt': 'America/New_York',
            'pst': 'America/Los_Angeles',
            'pdt': 'America/Los_Angeles',
            'cst': 'America/Chicago',
            'cdt': 'America/Chicago',
            'mst': 'America/Denver',
            'mdt': 'America/Denver',
            'gmt': 'GMT',
            'utc': 'UTC'
        }
    
    return timezone_aliases

# Timezone database setup
def get_db_connection():
    conn = sqlite3.connect('timezones.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db_connection() as conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS user_timezones (
            user_id INTEGER PRIMARY KEY,
            timezone TEXT NOT NULL
        )
        ''')
        conn.commit()

# Initialize the database when the module loads
init_db()

def parse_time(time_str: str) -> Optional[Tuple[int, int, str]]:
    """Parse time string into (hours, minutes, period)"""
    time_str = time_str.upper().strip()
    
    # Handle formats: 12AM, 12 AM, 12:00 AM, 00:00
    match = re.match(r'(\d{1,2})(?::(\d{2}))?\s*([AP]M)?', time_str)
    if not match:
        return None
        
    hours = int(match.group(1))
    minutes = int(match.group(2) or 0)
    period = match.group(3) or ''
    
    # Convert 12-hour to 24-hour if period is specified
    if period:
        if period == 'PM' and hours < 12:
            hours += 12
        elif period == 'AM' and hours == 12:
            hours = 0
    
    return hours, minutes, period

def format_time(hours: int, minutes: int) -> str:
    """Format time into 12-hour format"""
    period = 'AM' if hours < 12 else 'PM'
    display_hour = hours % 12 or 12
    return f"{display_hour}:{minutes:02d} {period}"

def get_user_timezone(user_id: int) -> Optional[str]:
    """Get a user's timezone from the database"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT timezone FROM user_timezones WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        return result['timezone'] if result else None

def set_user_timezone(user_id: int, timezone: str) -> bool:
    """Set a user's timezone in the database"""
    try:
        # Validate timezone
        pytz.timezone(timezone)
        with get_db_connection() as conn:
            conn.execute('''
            INSERT OR REPLACE INTO user_timezones (user_id, timezone)
            VALUES (?, ?)
            ''', (user_id, timezone))
            conn.commit()
        return True
    except pytz.exceptions.UnknownTimeZoneError:
        return False

class Basic(commands.Cog):
    """Basic utility commands."""
    
    def __init__(self, bot):
        self.bot = bot
        self.search_results = {}
        # Timezone aliases will be loaded on first use
        self._timezone_aliases_loaded = False
        
    @commands.hybrid_command(name="about", description="Learn about this bot")
    @app_commands.describe()
    async def about(self, ctx: commands.Context) -> None:
        """Display information about the bot."""
        await ctx.send(
            "I am The World Machine. My main purpose is for moderation and providing "
            "useful utilities for server management and member interaction. "
            "Use `pls help` to see all available commands."
        )
        
    @commands.hybrid_command(name="help", description="Show help information and command documentation")
    @app_commands.describe()
    async def help_command(self, ctx: commands.Context) -> None:
        """Show a link to the command documentation."""
        embed = discord.Embed(
            title="📚 The World Machine - Command Documentation",
            description="Click the link below to view all available commands and their usage:",
            color=discord.Color(0x8E6578)  # Using the purple color from the docs
        )
        embed.add_field(
            name="Documentation",
            value="[View Documentation](https://lucasongithub.github.io/the-world-machine/)",
            inline=False
        )
        embed.add_field(
            name="Prefix",
            value="• The default prefix is `pls`, but `.` is also supported.\n",
            inline=False
        )
        embed.set_footer(text="Documentation is updated regularly. Check back for new commands and features!")
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="roll", description="Roll dice in XdY format (e.g., 2d6)")
    @app_commands.describe(dice="Dice to roll in XdY format (e.g., 2d6)")
    async def roll(self, ctx: commands.Context, *, dice: str) -> None:
        """Roll dice in XdY format.
        
        Examples:
        - `pls roll 1d6` - Roll one 6-sided die
        - `pls roll 2d20` - Roll two 20-sided dice
        - `pls roll 4d6` - Roll four 6-sided dice
        """
        try:
            # Parse the dice notation
            if 'd' not in dice.lower():
                raise ValueError("Please use XdY format (e.g., 1d6, 2d20)")
                
            count, sides = dice.lower().split('d')
            count = int(count) if count else 1
            sides = int(sides)
            
            # Validate inputs
            if count < 1 or count > 100:
                raise ValueError("Number of dice must be between 1 and 100")
            if sides < 2 or sides > 1000:
                raise ValueError("Number of sides must be between 2 and 1000")
                
            # Roll the dice
            import random
            rolls = [random.randint(1, sides) for _ in range(count)]
            total = sum(rolls)
            
            # Format the results
            if count == 1:
                result = f"🎲 You rolled a **{rolls[0]}**"
            else:
                rolls_str = ', '.join(str(r) for r in rolls)
                result = f"🎲 You rolled {count}d{sides}: {rolls_str}\n**Total:** {total}"
                
            await ctx.send(result)
            
        except ValueError as e:
            await ctx.send(f"❌ {str(e)}. Example: `pls roll 2d6`", ephemeral=True)
        except Exception as e:
            await ctx.send("❌ An error occurred while rolling the dice.", ephemeral=True)
    
    @commands.hybrid_command(name="staff", description="Show currently active staff members")
    @commands.guild_only()
    @app_commands.describe()
    async def staff(self, ctx: commands.Context) -> None:
        """Display currently active staff members."""
        if not ctx.guild:
            return await ctx.send("This command can only be used in a server.", ephemeral=True)
            
        # Get staff roles from config
        from config.config import Config
        guild_config = Config.get_guild_config(ctx.guild.id)
        if not guild_config or not guild_config.get('staff_roles'):
            return await ctx.send("No staff roles are configured for this server.", ephemeral=True)
            
        # Find all staff members
        staff_members = []
        for role_id in guild_config['staff_roles']:
            role = ctx.guild.get_role(role_id)
            if role:
                staff_members.extend(role.members)
        
        # Remove duplicates (in case someone has multiple staff roles)
        unique_staff = {member.id: member for member in staff_members}.values()
        
        if not unique_staff:
            return await ctx.send("No staff members found.")
            
        # Sort by status (online first) and then by display name
        status_order = {
            discord.Status.online: 0,
            discord.Status.idle: 1,
            discord.Status.dnd: 2,
            discord.Status.offline: 3,
            discord.Status.invisible: 3
        }
        
        sorted_staff = sorted(
            unique_staff,
            key=lambda m: (status_order.get(m.status, 4), m.display_name.lower())
        )
        
        # Create embed
        embed = discord.Embed(
            title=f"👥 {ctx.guild.name} Staff Team",
            color=discord.Color.blue(),
            timestamp=ctx.message.created_at
        )
        
        # Group by status
        status_groups = {
            'Online': [],
            'Idle': [],
            'Do Not Disturb': [],
            'Offline': []
        }
        
        status_map = {
            discord.Status.online: 'Online',
            discord.Status.idle: 'Idle',
            discord.Status.dnd: 'Do Not Disturb',
            discord.Status.offline: 'Offline',
            discord.Status.invisible: 'Offline'
        }
        
        for member in sorted_staff:
            status = status_map.get(member.status, 'Offline')
            status_groups[status].append(member.display_name)
        
        # Add fields for each status group
        for status, members in status_groups.items():
            if members:
                embed.add_field(
                    name=f"{status} ({len(members)})",
                    value="\n".join(f"• {name}" for name in members),
                    inline=False
                )
        
        await ctx.send(embed=embed)
    
    @commands.hybrid_command(name="timezone", aliases=['tz'], description="Get or set your timezone")
    @app_commands.describe(timezone="Optional: Your timezone code (e.g., 'America/New_York')")
    async def timezone(self, ctx: commands.Context, *, timezone: Optional[str] = None) -> None:
        """Get or set your timezone for time commands.
        
        Without arguments: Shows help for getting your timezone code.
        With timezone: Sets your timezone.
        
        Examples:
        - `pls timezone` - Shows timezone help
        - `pls timezone America/New_York` - Sets your timezone
        - `pls tz Europe/London` - Sets your timezone (shorter alias)
        """
        if timezone is None:
            # Show help for getting timezone
            embed = discord.Embed(
                title="🌐 Get Your Timezone",
                description=(
                    "To set your timezone, use: `pls timezone <timezone>`\n"
                    "Example: `pls timezone America/New_York`\n\n"
                    "To find your timezone code:\n"
                    "1. [🌍 Click here to get your timezone code](https://xske.github.io/tz/)\n"
                    "2. Click the timezone code to copy it\n"
                    "3. Use `pls timezone <paste code>` to set it"
                ),
                color=discord.Color.blue()
            )
            await ctx.send(embed=embed)
            return
            
        # Set timezone
        try:
            # Check for aliases first
            aliases = await self._get_timezone_aliases()
            timezone_lower = timezone.lower()
            timezone = aliases.get(timezone_lower, timezone)
            
            # Validate timezone
            tz = pytz.timezone(timezone)
            
            if set_user_timezone(ctx.author.id, str(tz)):
                await ctx.send(f"✅ Your timezone has been set to: `{str(tz)}`")
            else:
                await ctx.send("❌ Failed to set timezone. Please try again.")
                
        except pytz.exceptions.UnknownTimeZoneError:
            await ctx.send(
                "❌ Invalid timezone code. Please use a valid timezone code from "
                "[this website](https://xske.github.io/tz/).\n"
                "Example: `pls timezone America/New_York`"
            )
    
    async def _get_timezone_aliases(self) -> Dict[str, str]:
        """Get timezone aliases, loading them if necessary"""
        if not self._timezone_aliases_loaded:
            await load_timezone_aliases()
            self._timezone_aliases_loaded = True
        return timezone_aliases
        """Set your timezone using a timezone code.
        
        First get your timezone code using `pls timezone`.
        Then use this command to set it.
        
        Example: `pls settz America/New_York`
        """
        try:
            # Check for aliases first
            aliases = await self._get_timezone_aliases()
            timezone_lower = timezone.lower()
            timezone = aliases.get(timezone_lower, timezone)
            
            # Validate timezone
            tz = pytz.timezone(timezone)
            
            if set_user_timezone(ctx.author.id, str(tz)):
                await ctx.send(f"✅ Your timezone has been set to: `{str(tz)}`")
            else:
                await ctx.send("❌ Failed to set timezone. Please try again.")
                
        except pytz.exceptions.UnknownTimeZoneError:
            await ctx.send(
                "❌ Invalid timezone code. Please use a valid timezone code from "
                "[this website](https://xske.github.io/tz/).\n"
                "Example: `pls settz America/New_York`"
            )
    
    @commands.hybrid_command(name="timefor", aliases=['tf'], description="Check someone's local time")
    @app_commands.describe(
        user="The user to check (mention or ID)",
        time_str="Optional time in 12h or 24h format (e.g., '2PM', '14:00')"
    )
    async def timefor(self, ctx: commands.Context, user: Optional[discord.Member] = None, time_str: Optional[str] = None) -> None:
        """Check the local time for you or another user.
        
        Examples:
        - `pls timefor` - Show your current local time
        - `pls timefor @User` - Show someone else's local time
        - `pls timefor 2PM` - Convert 2PM to other users' timezones
        - `pls timefor @User 2PM` - Convert 2PM in user's timezone to your time
        """
        # If no user mentioned, default to the command author
        target_user = user or ctx.author
        
        # Get the target user's timezone
        target_tz = get_user_timezone(target_user.id)
        
        if not target_tz:
            if target_user == ctx.author:
                await ctx.send("❌ You haven't set your timezone yet. Use `pls timezone` to set it.")
            else:
                await ctx.send(f"❌ {target_user.display_name} hasn't set their timezone yet.")
            return
        
        try:
            tz = pytz.timezone(target_tz)
            now = datetime.now(tz)
            
            if not time_str:
                # Just show the current time in their timezone
                time_str = now.strftime('%I:%M %p').lstrip('0')
                await ctx.send(f"🕒 It's currently **{time_str}** in {target_user.display_name}'s timezone (`{target_tz}`).")
                return
                
            # Parse the input time
            parsed_time = parse_time(time_str)
            if not parsed_time:
                await ctx.send("❌ Invalid time format. Use formats like '2PM', '2:30 PM', or '14:30'.")
                return
                
            hours, minutes, _ = parsed_time
            
            # Create a datetime object with the parsed time in the target timezone
            target_dt = tz.localize(datetime(
                now.year, now.month, now.day,
                hours, minutes, 0
            ))
            
            # Get the command author's timezone
            author_tz = get_user_timezone(ctx.author.id)
            if not author_tz:
                await ctx.send("❌ You need to set your timezone first with `pls timezone`.")
                return
                
            # Convert to author's timezone
            author_tz_obj = pytz.timezone(author_tz)
            author_dt = target_dt.astimezone(author_tz_obj)
            
            # Format the output with platform-agnostic time formatting
            def format_time(dt):
                time_str = dt.strftime('%I:%M %p')
                # Remove leading zero for hours
                if time_str[0] == '0':
                    time_str = time_str[1:]
                return time_str
                
            target_time = format_time(target_dt)
            author_time = format_time(author_dt)
            
            if target_user == ctx.author:
                await ctx.send(f"🕒 **{target_time}** in your timezone (`{target_tz}`) is **{author_time}** in {author_tz}.")
            else:
                await ctx.send(
                    f"🕒 **{target_time}** in {target_user.mention}'s timezone (`{target_tz}`) "
                    f"is **{author_time}** in your timezone (`{author_tz}`)."
                )
                
        except Exception as e:
            await ctx.send(f"❌ An error occurred: {str(e)}")
    
    @commands.hybrid_command(name="ping", description="Check the bot's latency")
    @app_commands.describe()
    async def ping(self, ctx: commands.Context) -> None:
        """Show the bot's current latency."""
        latency = round(self.bot.latency * 1000)  # Convert to milliseconds
        await ctx.send(f"🏓 Pong! Latency: `{latency}ms`")
    
    @commands.hybrid_command(name="membercount", description="Show server member count")
    @app_commands.describe()
    @commands.guild_only()
    async def membercount(self, ctx: commands.Context) -> None:
        """Display the total number of members in this server."""
        if not ctx.guild:
            return await ctx.send("This command can only be used in a server.", ephemeral=True)
    @commands.hybrid_command(name="google", description="Search Google")
    @app_commands.describe(query="What to search for")
    async def google(self, ctx: commands.Context, *, query: str) -> None:
        """Search Google and return the top results"""
        try:
            # Show typing indicator while searching
            async with ctx.typing():
                # Clean the query to avoid any potential issues
                query = query.strip()
                if not query:
                    await ctx.send("Please provide a search query.", ephemeral=True)
                    return
                
                # Perform the search
                search_results = []
                try:
                    search_results = list(search(query, num_results=5, advanced=True))
                except Exception as e:
                    # Fall back to simple search if advanced fails
                    try:
                        search_results = list(search(query, num_results=5))
                    except Exception as e:
                        return await ctx.send("❌ Failed to perform search. Please try again later.", ephemeral=True)
                
                if not search_results:
                    return await ctx.send("No results found. Try a different search query.", ephemeral=True)
                
                # Store results for pagination
                self.search_results[ctx.author.id] = {
                    'results': search_results,
                    'current_page': 0,
                    'query': query
                }
                
                # Format and show first result
                await self._send_search_result(ctx, 0)
                
        except Exception as e:
            print(f"Error in google command: {str(e)}")
            await ctx.send("❌ An error occurred while performing the search. Please try again later.", ephemeral=True)
    
    async def _send_search_result(self, ctx: commands.Context, page: int) -> None:
        """Helper method to send a search result page"""
        if ctx.author.id not in self.search_results:
            return
            
        results = self.search_results[ctx.author.id]['results']
        query = self.search_results[ctx.author.id].get('query', 'Search Results')
        
        if not results or page < 0 or page >= len(results):
            return
            
        self.search_results[ctx.author.id]['current_page'] = page
        result = results[page]
        
        try:
            if isinstance(result, str):
                # Simple string result
                description = result
                title = f"Search Result {page + 1}/{len(results)}"
                url = ""
            else:
                # Advanced search result object
                description = getattr(result, 'description', "No description available")
                title = getattr(result, 'title', f"Search Result {page + 1}/{len(results)}")
                url = getattr(result, 'url', "")
                description = f"{url}\n\n{description}"
            
            # Truncate to avoid embed limits
            title = title[:256]
            description = description[:4000]
            
            embed = discord.Embed(
                title=title,
                description=description,
                color=discord.Color.blue()
            )
            
            # Add URL if available
            if url:
                embed.url = url
                
            # Add search query to footer
            query_display = query[:100] + '...' if len(query) > 100 else query
            embed.set_footer(text=f"Search: {query_display} • Result {page + 1} of {len(results)}")
            
            message = await ctx.send(embed=embed)
            self.search_results[ctx.author.id]['message'] = message
            
        except Exception as e:
            print(f"Error formatting search result: {str(e)}")
            await ctx.send("❌ An error occurred while formatting the search results.", ephemeral=True)
    
    @commands.Cog.listener()
    async def on_reaction_add(self, reaction, user):
        """Handle reaction-based pagination for search results."""
        # Ignore reactions from the bot itself
        if user == self.bot.user:
            return
            
        # Check if this is a reaction to a search result message
        for user_id, data in list(self.search_results.items()):
            if 'message' in data and reaction.message.id == data.get('message').id:
                # Check if the reaction is from the original author
                if user.id != user_id:
                    try:
                        await reaction.remove(user)
                    except:
                        pass
                    return
                
                # Handle navigation
                try:
                    if str(reaction.emoji) == '⏹':
                        # Stop button - clean up
                        if user_id in self.search_results:
                            del self.search_results[user_id]
                        await reaction.message.delete()
                        return
                        
                    elif str(reaction.emoji) == '⬅️':
                        # Previous result
                        current_page = data.get('current_page', 0)
                        if current_page > 0:
                            await self._send_search_result(reaction.message.channel, current_page - 1)
                            
                    elif str(reaction.emoji) == '➡️':
                        # Next result
                        current_page = data.get('current_page', 0)
                        if current_page < len(data.get('results', [])) - 1:
                            await self._send_search_result(reaction.message.channel, current_page + 1)
                    
                    # Remove the reaction after handling
                    await reaction.remove(user)
                    
                except Exception as e:
                    print(f"Error handling reaction: {e}")
                    if user_id in self.search_results:
                        del self.search_results[user_id]
    
    @commands.hybrid_command(name="membercount", description="Show server member count")
    @app_commands.describe()
    @commands.guild_only()
    async def membercount(self, ctx: commands.Context) -> None:
        """Display the total number of members in this server."""
        if not ctx.guild:
            return await ctx.send("This command can only be used in a server.", ephemeral=True)

    @commands.hybrid_command(name="pingstaff", description="Ping the staff team")
    @app_commands.describe(reason="Reason for pinging staff")
    @commands.guild_only()
    @commands.cooldown(1, 120, commands.BucketType.default)  # 2 minute global cooldown
    async def pingstaff(self, ctx: commands.Context, *, reason: str = None) -> None:
        """Ping the staff team with an optional reason.
        
        This command has a 2-minute global cooldown for all users.
        """
        if not ctx.guild:
            return await ctx.send("This command can only be used in a server.", ephemeral=True)
        
        # Get staff role IDs from config
        from config.config import Config
        staff_roles = Config.get_guild_setting(ctx.guild.id, 'staff_roles', [])
        
        if not staff_roles:
            return await ctx.send("❌ No staff roles are configured for this server.", ephemeral=True)
        
        # Get staff mentions
        staff_mentions = []
        for role_id in staff_roles:
            role = ctx.guild.get_role(role_id)
            if role:
                staff_mentions.append(role.mention)
        
        if not staff_mentions:
            return await ctx.send("❌ Could not find any staff roles to ping.", ephemeral=True)
        
        # Create the embed
        embed = discord.Embed(
            title="📢 Staff Ping",
            description=f"{', '.join(staff_mentions)}\n\n**Reason:** {reason or 'No reason provided'}",
            color=discord.Color.blue()
        )
        embed.set_footer(text=f"Requested by {ctx.author}", icon_url=ctx.author.display_avatar.url)
        
        # Send the ping
        await ctx.send(embed=embed)
        
        # Also send a confirmation to the user
        await ctx.send("✅ Staff have been notified!", ephemeral=True)

    @pingstaff.error
    async def pingstaff_error(self, ctx: commands.Context, error):
        """Handle cooldown errors for the pingstaff command."""
        if isinstance(error, commands.CommandOnCooldown):
            # Convert remaining time to minutes and seconds
            minutes, seconds = divmod(int(error.retry_after), 60)
            await ctx.send(
                f"⏳ This command is on cooldown. Please try again in {minutes}m {seconds}s.",
                ephemeral=True
            )
        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send("❌ Please provide a reason for pinging staff.", ephemeral=True)
        else:
            await ctx.send(f"❌ An error occurred: {str(error)}", ephemeral=True)

async def setup(bot):
    try:
        await bot.add_cog(Basic(bot))
    except Exception as e:
        print(f"Error loading Basic cog: {e}")
