import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional, Dict, List
import asyncio
from datetime import datetime

# Local imports
from config.config import Config
from config.errors import ErrorMessages

# Constants
MAX_TOSS_CHANNELS = 25
TOSSED_ROLE_NAME = "Tossed"

class TossingError(Exception):
    """Custom exception for Tossing cog errors."""
    def __init__(self, error_key: str, **kwargs):
        self.message = ErrorMessages.get_error(error_key, **kwargs)
        super().__init__(self.message)

class Tossing(commands.Cog):
    """Commands for managing tossed users."""
    
    def __init__(self, bot):
        self.bot = bot
        self.tossed_users: Dict[int, Dict] = {}  # {user_id: {"channel": channel, "roles": [roles]}}

    async def cog_check(self, ctx: commands.Context) -> bool:
        """Check if user is a staff member."""
        if not Config.is_staff(ctx.author):
            raise commands.MissingPermissions(["staff"])
        return True

    async def cog_command_error(self, ctx: commands.Context, error: Exception) -> None:
        """Handle errors for all commands in this cog."""
        try:
            if isinstance(error, TossingError):
                await ctx.send(str(error), ephemeral=True)
            elif isinstance(error, commands.MissingPermissions):
                await ctx.send(ErrorMessages.get_error('missing_role'), ephemeral=True)
            elif isinstance(error, commands.BotMissingPermissions):
                await ctx.send(ErrorMessages.get_error('missing_perms'), ephemeral=True)
            elif isinstance(error, commands.MemberNotFound):
                await ctx.send(ErrorMessages.get_error('member_not_found'), ephemeral=True)
            elif isinstance(error, commands.CommandError):
                await ctx.send(f"Command error: {str(error)}", ephemeral=True)
            else:
                print(f"Unhandled error in {ctx.command}: {error}")
                await ctx.send(ErrorMessages.get_error('generic'), ephemeral=True)
        except Exception as e:
            print(f"Error in error handler: {e}")
            try:
                await ctx.send(":en_bsod: Something broke while I was doing that... I let Lucas know...", ephemeral=True)
            except:
                pass

    def _format_time(self, dt: datetime) -> str:
        """Format datetime in a readable format."""
        return dt.strftime("%B %d, %Y %I:%M %p")

    def _time_ago(self, dt: datetime) -> str:
        """Get time ago string."""
        now = datetime.utcnow()
        delta = now - dt
        
        if delta.days >= 365:
            years = delta.days // 365
            return f"{years} year{'s' if years > 1 else ''} ago"
        elif delta.days >= 30:
            months = delta.days // 30
            return f"{months} month{'s' if months > 1 else ''} ago"
        elif delta.days > 0:
            return f"{delta.days} day{'s' if delta.days > 1 else ''} ago"
        elif delta.seconds >= 3600:
            hours = delta.seconds // 3600
            return f"{hours} hour{'s' if hours > 1 else ''} ago"
        else:
            minutes = max(1, delta.seconds // 60)
            return f"{minutes} minute{'s' if minutes > 1 else ''} ago"

    async def log_toss_action(self, action: str, member: discord.Member, moderator: discord.Member, **kwargs) -> None:
        """Log toss actions to the logging channel using embeds."""
        from config.config import Config
        guild_id = member.guild.id
        log_channel_id = Config.get_toss_logs(guild_id)
        
        if not log_channel_id:
            raise TossingError("toss_logs_not_configured")
            
        log_channel = self.bot.get_channel(log_channel_id)
        if not log_channel:
            raise TossingError("Could not find the toss logs channel.")

        if action == "tossed":
            # Create embed
            embed = discord.Embed(
                title=f"🚷 User Tossed",
                color=discord.Color.orange(),
                timestamp=discord.utils.utcnow()
            )
            
            # Set author with member's avatar
            embed.set_author(name=f"{member} (ID: {member.id})", icon_url=member.display_avatar.url)
            
            # Add user and moderator info
            embed.add_field(name="👤 User", value=f"{member.mention} ({member.id})", inline=False)
            embed.add_field(name="🛡️ Moderator", value=moderator.mention, inline=True)
            
            # Add reason if provided
            if 'reason' in kwargs and kwargs['reason']:
                embed.add_field(name="📝 Reason", value=kwargs['reason'], inline=False)
            
            # Add channel info if available
            if 'channel' in kwargs and kwargs['channel']:
                channel = kwargs['channel']
                embed.add_field(name="💬 Channel", value=f"{channel.mention} ({channel.name})", inline=True)
            
            # Add account creation and join date
            created_at = member.created_at.replace(tzinfo=None)
            embed.add_field(
                name="⏰ Account Created",
                value=f"{self._format_time(created_at)}\n({self._time_ago(created_at)})",
                inline=True
            )
            
            if member.joined_at:
                joined_at = member.joined_at.replace(tzinfo=None)
                embed.add_field(
                    name="📅 Joined Server",
                    value=f"{self._format_time(joined_at)}\n({self._time_ago(joined_at)})",
                    inline=True
                )
            
            # Add previous roles if any
            previous_roles = [role for role in kwargs.get("roles", []) 
                           if not role.is_default() and role.name != TOSSED_ROLE_NAME]
            if previous_roles:
                roles_text = ", ".join(role.mention for role in previous_roles[:5])
                if len(previous_roles) > 5:
                    roles_text += f" and {len(previous_roles) - 5} more roles"
                embed.add_field(
                    name=f"🎨 Previous Roles ({len(previous_roles)})",
                    value=roles_text,
                    inline=False
                )
            
            # Set footer with server info
            embed.set_footer(text=f"Server: {member.guild.name} • Tossed at")
            
            # Send the embed
            await log_channel.send(embed=embed)
        
        elif action == "released":
            # Create embed for release
            embed = discord.Embed(
                title=f"✅ User Released",
                color=discord.Color.green(),
                timestamp=discord.utils.utcnow()
            )
            
            # Set author with member's avatar
            embed.set_author(name=f"{member} (ID: {member.id})", icon_url=member.display_avatar.url)
            
            # Add user and moderator info
            embed.add_field(name="👤 User", value=f"{member.mention} ({member.id})", inline=True)
            embed.add_field(name="🛡️ Moderator", value=moderator.mention, inline=True)
            
            # Add reason if provided
            if 'reason' in kwargs and kwargs['reason']:
                embed.add_field(name="📝 Reason", value=kwargs['reason'], inline=False)
            
            # Add channel info if available
            if 'channel' in kwargs and kwargs['channel']:
                channel = kwargs['channel']
                embed.add_field(name="💬 Channel", value=f"{channel.mention} (closed)", inline=True)
            
            # Set footer with server info
            embed.set_footer(text=f"Server: {member.guild.name} • Released at")
            
            # Send the embed
            await log_channel.send(embed=embed)

    @commands.hybrid_command(name="toss", description="Toss a user into a private channel")
    @commands.guild_only()
    @app_commands.describe(
        member="The member to toss (ID or mention)",
        reason="Reason for tossing the user (optional)"
    )
    async def toss(self, ctx: commands.Context, member: str, *, reason: str = None) -> None:
        """Toss a user into a private channel.
        
        Parameters
        ----------
        member: str
            The member to toss (ID or mention)
        reason: Optional[str]
            Reason for tossing the user (will be shown in logs)
        """
        try:
            # Try to convert the input to a Member object
            member = await commands.MemberConverter().convert(ctx, member)
            
            if member.id in self.tossed_users:
                await ctx.send(ErrorMessages.get_error('already_tossed', user=member.mention), ephemeral=True)
                return
                
            # Check if the moderator can moderate the target
            can_mod, reason = Config.can_moderate(ctx.author, member)
            if not can_mod:
                await ctx.send(ErrorMessages.get_error('protection_denied', reason=reason), ephemeral=True)
                return
                
            # If target is staff, require confirmation
            if Config.get_protection_level(member) == 'staff':
                confirm_msg = await ctx.send(ErrorMessages.get_error('confirm_moderate_staff'))
                
                def check(m):
                    return m.author == ctx.author and m.channel == ctx.channel and m.content.lower() in ['yes', 'no']
                
                try:
                    msg = await self.bot.wait_for('message', check=check, timeout=30.0)
                    if msg.content.lower() != 'yes':
                        await ctx.send("Action cancelled.", ephemeral=True)
                        return
                except asyncio.TimeoutError:
                    await ctx.send("Confirmation timed out. Please try again.", ephemeral=True)
                    return
                
            # Get server-specific configuration
            guild_id = ctx.guild.id
            category_id = Config.get_toss_category(guild_id)
            
            if not category_id:
                raise TossingError("toss_category_not_configured")
                
            category = ctx.guild.get_channel(category_id)
            if not category:
                raise TossingError("Could not find the toss category.")
                
            if len(category.channels) >= MAX_TOSS_CHANNELS:
                await ctx.send(ErrorMessages.get_error('max_channels'), ephemeral=True)
                return
                
            # Store user's current roles
            user_roles = [role for role in member.roles if not role.is_default()]
            
        except commands.MemberNotFound:
            await ctx.send(ErrorMessages.get_error('member_not_found'), ephemeral=True)
            return
        except TossingError as e:
            await ctx.send(str(e), ephemeral=True)
            return
        except Exception as e:
            await ctx.send(f"Error: {str(e)}", ephemeral=True)
            return
        
        if not category_id:
            raise TossingError("toss_category_not_configured")
            
        category = ctx.guild.get_channel(category_id)
        if not category:
            raise TossingError("Could not find the toss category.")
            
        if len(category.channels) >= MAX_TOSS_CHANNELS:
            await ctx.send(ErrorMessages.get_error('max_channels'), ephemeral=True)
            return

        # Store user's current roles
        user_roles = [role for role in member.roles if not role.is_default()]
        
        # Create toss channel
        overwrites = {
            ctx.guild.default_role: discord.PermissionOverwrite(read_messages=False),
            member: discord.PermissionOverwrite(
                read_messages=True,
                send_messages=True,
                read_message_history=True
            ),
            ctx.guild.me: discord.PermissionOverwrite(
                read_messages=True,
                send_messages=True,
                manage_channels=True
            )
        }

        # Add moderators to the channel
        for role in ctx.guild.roles:
            if role.permissions.manage_roles:
                overwrites[role] = discord.PermissionOverwrite(
                    read_messages=True,
                    send_messages=True
                )

        try:
            channel = await ctx.guild.create_text_channel(
                name=f"toss-{member.name}",
                category=category,
                overwrites=overwrites,
                reason=f"Toss channel for {member}"
            )
            
            # Add to tossed users
            self.tossed_users[member.id] = {
                "channel": channel,
                "roles": user_roles
            }
            
            # Remove all roles and add Tossed role
            tossed_role = discord.utils.get(ctx.guild.roles, name=TOSSED_ROLE_NAME)
            if not tossed_role:
                tossed_role = await ctx.guild.create_role(
                    name=TOSSED_ROLE_NAME,
                    reason="Tossed role for tossed users"
                )
            
            await member.edit(roles=[tossed_role])
            
            # Log the toss action
            await self.log_toss_action(
                "tossed",
                member,
                ctx.author,
                channel=channel,
                roles=user_roles,
                reason=reason
            )
            
            # Add reaction to the command message
            try:
                await ctx.message.add_reaction("🚯")  # :do_not_litter: emoji
            except:
                pass  # Skip if we can't add the reaction
            
        except Exception as e:
            await ctx.send(ErrorMessages.get_error('generic'), ephemeral=True)
            if 'channel' in locals():
                await channel.delete()
            if member.id in self.tossed_users:
                del self.tossed_users[member.id]

    @commands.hybrid_command(name="untoss", description="Untoss a user")
    @commands.guild_only()
    @app_commands.describe(
        member="The member to untoss",
        reason="Reason for untossing the user (optional)"
    )
    async def untoss(self, ctx: commands.Context, member: discord.Member, *, reason: str = None) -> None:
        """Untoss a user and give back their roles.
        
        Parameters
        ----------
        member: discord.Member
            The member to untoss
        reason: Optional[str]
            Reason for untossing the user (will be shown in logs)
        """
        if member.id not in self.tossed_users:
            await ctx.send(ErrorMessages.get_error('not_tossed', user=member.mention), ephemeral=True)
            return
            
        await self._release_user(member, ctx.author, ctx, reason)

    @commands.hybrid_command(name="close", description="Close the current toss channel")
    @commands.guild_only()
    @app_commands.describe(reason="Reason for closing the channel (optional)")
    async def close(self, ctx: commands.Context, *, reason: str = None) -> None:
        """Close the current toss channel.
        
        Parameters
        ----------
        reason: Optional[str]
            Reason for closing the channel (will be shown in logs)
        """
        # Find the user this channel belongs to
        member = None
        for user_id, data in self.tossed_users.items():
            if data["channel"].id == ctx.channel.id:
                member = ctx.guild.get_member(user_id)
                break
                
        if not member:
            await ctx.send(ErrorMessages.get_error('not_toss_channel'), ephemeral=True)
            return
            
        if Config.is_staff(member):
            await ctx.send(ErrorMessages.get_error('staff_protected'), ephemeral=True)
            return
            
        await self._release_user(member, ctx.author, ctx, reason)

    async def _release_user(self, member: discord.Member, moderator: discord.Member, ctx: commands.Context, reason: str = None) -> None:
        """Helper function to release a tossed user.
        
        Parameters
        ----------
        member: discord.Member
            The member being released
        moderator: discord.Member
            The moderator performing the action
        ctx: commands.Context
            The command context
        reason: Optional[str]
            Reason for the release (will be shown in logs)
        """
        if member.id not in self.tossed_users:
            return
            
        user_data = self.tossed_users[member.id]
        channel = user_data["channel"]
        roles = user_data["roles"]
        
        try:
            # Restore roles and remove Tossed role
            tossed_role = discord.utils.get(member.guild.roles, name=TOSSED_ROLE_NAME)
            if tossed_role:
                await member.remove_roles(tossed_role)
                
            # Restore original roles
            if roles:
                await member.add_roles(*roles)
            
            # Log the release
            await self.log_toss_action(
                "released",
                member,
                moderator,
                channel=channel,
                reason=reason
            )
            
            # Add checkmark reaction to the command message if available
            if hasattr(ctx, 'message'):
                try:
                    await ctx.message.add_reaction("✅")  # White checkmark emoji
                except:
                    pass  # Skip if we can't add the reaction
            
            # Delete the channel and clean up
            await channel.delete(reason=f"Toss channel closed by {moderator}")
            del self.tossed_users[member.id]
            
            # Only send response if we're not in the channel being deleted
            if ctx.channel.id != channel.id:
                await ctx.send(f"Successfully released {member.mention}", ephemeral=True)
                
        except Exception as e:
            await ctx.send(f"An error occurred: {e}", ephemeral=True)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member) -> None:
        """Clean up if a tossed user leaves the server."""
        if member.id in self.tossed_users:
            channel = self.tossed_users[member.id]["channel"]
            await channel.delete(reason=f"Tossed user left the server")
            del self.tossed_users[member.id]

async def setup(bot):
    await bot.add_cog(Tossing(bot))
