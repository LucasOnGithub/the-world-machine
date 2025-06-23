import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime, timedelta
import re
from typing import Optional, Union
from config.errors import ErrorMessages

class Moderation(commands.Cog):
    """Moderation commands for server management."""
    
    def __init__(self, bot):
        self.bot = bot
        self.time_regex = re.compile(r"(\d+)([smhdw])")
        self.time_dict = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

    async def cog_check(self, ctx: commands.Context) -> bool:
        """Check if user is a staff member."""
        from config.config import Config
        
        # If this is a DM, don't allow the command
        if not ctx.guild:
            raise commands.NoPrivateMessage(ErrorMessages.get_error('servers_only'))
            
        # Check if user is staff
        if not Config.is_staff(ctx.author):
            raise commands.MissingPermissions(["staff"])
            
        return True
        
    async def cog_command_error(self, ctx: commands.Context, error: Exception) -> None:
        """Handle errors for all commands in this cog."""
        # Mark the error as handled
        error.handled = True
        
        try:
            if isinstance(error, commands.NoPrivateMessage):
                # This is raised by cog_check for DMs
                await ctx.send(error.args[0], ephemeral=True)
            elif isinstance(error, commands.MissingPermissions):
                await ctx.send(ErrorMessages.get_error('missing_role'), ephemeral=True)
            elif isinstance(error, commands.MissingRequiredArgument):
                await ctx.send(ErrorMessages.get_error('missing_required_argument'), ephemeral=True)
            elif isinstance(error, commands.BotMissingPermissions):
                await ctx.send(ErrorMessages.get_error('missing_perms'), ephemeral=True)
            elif isinstance(error, commands.CommandOnCooldown):
                await ctx.send(ErrorMessages.get_error('cooldown', time=int(error.retry_after)), ephemeral=True)
            elif isinstance(error, commands.MemberNotFound):
                await ctx.send(ErrorMessages.get_error('member_not_found'), ephemeral=True)
            elif isinstance(error, commands.BadArgument):
                await ctx.send(str(error), ephemeral=True)
            else:
                # Re-raise the error to be handled by the global error handler
                error.handled = False
                raise error
        except Exception as e:
            # If we get here, there was an error in our error handler
            print(f"Error in cog_command_error: {e}")
            if not hasattr(error, 'handled') or not error.handled:
                error.handled = True
                await ctx.send(ErrorMessages.get_error('generic'), ephemeral=True)
            raise

    def parse_time(self, time_str: str) -> Optional[int]:
        """Convert time string to seconds."""
        if not time_str:
            return None
            
        total_seconds = 0
        matches = self.time_regex.findall(time_str.lower())
        
        if not matches:
            return None
            
        for amount, unit in matches:
            try:
                total_seconds += int(amount) * self.time_dict[unit]
            except (KeyError, ValueError):
                continue
                
        return total_seconds if total_seconds > 0 else None

    async def log_mod_action(
        self,
        action: str,
        moderator: discord.Member,
        user: Union[discord.Member, discord.User],
        **kwargs
    ) -> None:
        """Log moderation actions to the mod log channel."""
        from config.config import Config
        guild = moderator.guild
        
        # Define server-specific log channels
        log_channels = {
            1362133752658133150: 1362133752658133150,  # First server logs
            1379277826926313562: 1379277826926313562  # Second server logs
        }
        
        # Get the log channel ID for this guild
        log_channel_id = log_channels.get(guild.id)
        if not log_channel_id:
            return  # No log channel configured for this guild
            
        # Get the channel object
        log_channel = guild.get_channel(log_channel_id)
        if not log_channel or not hasattr(log_channel, 'send'):
            return  # Channel not found or can't send messages
            
        # Format the reason with any provided context
        reason = kwargs.get('reason', 'No reason provided')
        if 'duration' in kwargs:
            reason += f"\nDuration: {kwargs['duration']}"
            
        # Create embed
        embed = discord.Embed(
            title=f"User {action.capitalize()}",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        
        # Add fields
        embed.add_field(name="User", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Moderator", value=moderator.mention, inline=False)
        embed.add_field(name="Reason", value=reason, inline=False)
        
        # Add footer with timestamp
        embed.set_footer(text=f"User ID: {user.id}")
        
        try:
            await log_channel.send(embed=embed)
        except discord.Forbidden:
            # If we can't send embeds, try sending a plain text message
            try:
                await log_channel.send(
                    f"**{action.upper()}** | {user} (ID: {user.id})\n"
                    f"**Moderator:** {moderator}\n"
                    f"**Reason:** {reason}"
                )
            except discord.Forbidden:
                pass  # We've done our best to log this action

        embed = discord.Embed(
            title=f"User {action.capitalize()}",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        
        embed.add_field(name="User", value=f"{user.mention} ({user.id})", inline=True)
        embed.add_field(name="Moderator", value=moderator.mention, inline=True)
        
        if 'reason' in kwargs and kwargs['reason']:
            embed.add_field(name="Reason", value=kwargs['reason'], inline=False)
            
        if 'duration' in kwargs and kwargs['duration']:
            embed.add_field(name="Duration", value=kwargs['duration'], inline=True)
        
        await log_channel.send(embed=embed)

    @commands.hybrid_command(name="ban", description="Ban a user from the server")
    @commands.guild_only()
    @app_commands.describe(
        user="The user to ban",
        duration="Duration (e.g., 1d, 2h, 30m, 1w)",
        reason="Reason for the ban"
    )
    async def ban(
        self,
        ctx: commands.Context,
        user: discord.Member,
        duration: Optional[str] = None,
        *,
        reason: Optional[str] = None
    ) -> None:
        """Ban a user from the server."""
        if user == ctx.author:
            await ctx.send("You cannot ban yourself!", ephemeral=True)
            return
            
        if user.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.send("You cannot ban someone with an equal or higher role!", ephemeral=True)
            return

        seconds = self.parse_time(duration) if duration else None
        ban_duration = f"{duration}" if duration else "Permanent"
        
        try:
            await user.ban(reason=reason, delete_message_days=0)
            await ctx.send(f"✅ Banned {user.mention} for {ban_duration}" + 
                         (f" for: {reason}" if reason else ""), ephemeral=True)
            
            # Log the action
            await self.log_mod_action(
                "banned",
                ctx.author,
                user,
                reason=reason,
                duration=ban_duration
            )
            
            # If temporary ban, schedule unban
            if seconds:
                await asyncio.sleep(seconds)
                try:
                    await ctx.guild.unban(user, reason="Temporary ban expired")
                except:
                    pass  # User might be already unbanned or other error
                    
        except discord.Forbidden:
            await ctx.send("I don't have permission to ban this user!", ephemeral=True)
        except Exception as e:
            await ctx.send(f"An error occurred: {e}", ephemeral=True)

    @commands.hybrid_command(name="unban", description="Unban a user from the server")
    @commands.guild_only()
    @app_commands.describe(
        user_id="The ID of the user to unban (required)",
        reason="Reason for the unban"
    )
    async def unban(self, ctx: commands.Context, user_id: str = None, *, reason: str = None) -> None:
        """Unban a user from the server.
        
        Parameters
        ----------
        user_id: str
            The ID of the user to unban (required)
        reason: Optional[str]
            Reason for the unban (will be shown in logs)
        """
        # Check if user_id is provided
        if user_id is None:
            await ctx.send(ErrorMessages.get_error('missing_required_argument'), ephemeral=True)
            return
            
        try:
            # Convert user_id to int
            try:
                user_id = int(user_id)
                user = await self.bot.fetch_user(user_id)
            except (ValueError, discord.NotFound):
                await ctx.send("❌ Invalid user ID. Please provide a valid user ID.", ephemeral=True)
                return
            
            # Check if user is actually banned
            try:
                ban_entry = await ctx.guild.fetch_ban(user)
            except discord.NotFound:
                await ctx.send(f"❌ {user} is not banned.", ephemeral=True)
                return
                
            await ctx.guild.unban(user, reason=reason)
            await ctx.send(f"✅ Unbanned {user.mention}" + 
                         (f" for: {reason}" if reason else ""), ephemeral=True)
            
            # Log the action
            await self.log_mod_action(
                "unbanned",
                ctx.author,
                user,
                reason=reason
            )
            
        except ValueError:
            await ctx.send("❌ Invalid user ID. Please provide a valid user ID.", ephemeral=True)
        except discord.Forbidden:
            await ctx.send("I don't have permission to unban users!", ephemeral=True)
        except discord.NotFound:
            await ctx.send("User not found or not banned.", ephemeral=True)
        except Exception as e:
            await ctx.send(f"An error occurred: {e}", ephemeral=True)
    
    @commands.hybrid_command(name="unmute", description="Unmute a user")
    @commands.guild_only()
    @app_commands.describe(
        member="The member to unmute (ID or mention)",
        reason="Reason for unmuting"
    )
    async def unmute(
        self,
        ctx: commands.Context,
        member: discord.Member = None,
        *,
        reason: str = None
    ) -> None:
        """Unmute a user.
        
        Parameters
        ----------
        member: discord.Member
            The member to unmute (ID or mention)
        reason: Optional[str]
            Reason for unmuting (will be shown in logs)
        """
        # Check if member is provided
        if member is None:
            await ctx.send(ErrorMessages.get_error('missing_required_argument'), ephemeral=True)
            return
            
        # Check if user has a mute role
        from config.config import Config
        guild_config = Config.get_guild_config(ctx.guild.id)
        mute_role_id = guild_config.get('mute_role')
        
        if not mute_role_id:
            await ctx.send("❌ Mute role is not configured for this server.", ephemeral=True)
            return
            
        mute_role = ctx.guild.get_role(mute_role_id)
        if not mute_role:
            await ctx.send("❌ Mute role not found. Please reconfigure the mute role.", ephemeral=True)
            return
            
        if mute_role not in member.roles:
            await ctx.send(f"❌ {member.mention} is not muted.", ephemeral=True)
            return
            
        try:
            await member.remove_roles(mute_role, reason=reason)
            await ctx.send(f"✅ Unmuted {member.mention}" + 
                         (f" for: {reason}" if reason else ""), ephemeral=True)
            
            # Log the action
            await self.log_mod_action(
                "unmuted",
                ctx.author,
                member,
                reason=reason
            )
            
        except discord.Forbidden:
            await ctx.send("I don't have permission to unmute this user!", ephemeral=True)
        except Exception as e:
            await ctx.send(f"An error occurred: {e}", ephemeral=True)
    
    @commands.hybrid_command(name="kick", description="Kick a user from the server")
    @commands.guild_only()
    @app_commands.describe(
        user="The user to kick",
        reason="Reason for the kick"
    )
    async def kick(
        self,
        ctx: commands.Context,
        user: discord.Member,
        *,
        reason: Optional[str] = None
    ) -> None:
        """Kick a user from the server."""
        if user == ctx.author:
            await ctx.send("You cannot kick yourself!", ephemeral=True)
            return
            
        if user.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.send("You cannot kick someone with an equal or higher role!", ephemeral=True)
            return

        try:
            await user.kick(reason=reason)
            await ctx.send(f"✅ Kicked {user.mention}" + 
                         (f" for: {reason}" if reason else ""), ephemeral=True)
            
            # Log the action
            await self.log_mod_action(
                "kicked",
                ctx.author,
                user,
                reason=reason
            )
            
        except discord.Forbidden:
            await ctx.send("I don't have permission to kick this user!", ephemeral=True)
        except Exception as e:
            await ctx.send(f"An error occurred: {e}", ephemeral=True)

    @commands.hybrid_command(name="mute", description="Timeout a user")
    @commands.guild_only()
    @app_commands.describe(
        user="The user to timeout",
        duration="Duration (e.g., 1d, 2h, 30m)",
        reason="Reason for the timeout"
    )
    async def mute(
        self,
        ctx: commands.Context,
        user: discord.Member,
        duration: str,
        *,
        reason: Optional[str] = None
    ) -> None:
        """Timeout a user."""
        if user == ctx.author:
            await ctx.send("You cannot mute yourself!", ephemeral=True)
            return
            
        if user.top_role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.send("You cannot mute someone with an equal or higher role!", ephemeral=True)
            return

        seconds = self.parse_time(duration)
        if not seconds or seconds > 2419200:  # Max 28 days
            await ctx.send("Invalid duration! Please use format like 1h, 30m, 1d, etc. (max 28d)", ephemeral=True)
            return

        try:
            until = discord.utils.utcnow() + timedelta(seconds=seconds)
            await user.timeout(until, reason=reason)
            
            duration_str = f"{duration}"
            await ctx.send(f"✅ Muted {user.mention} for {duration_str}" + 
                         (f" for: {reason}" if reason else ""), ephemeral=True)
            
            # Log the action
            await self.log_mod_action(
                "muted",
                ctx.author,
                user,
                reason=reason,
                duration=duration_str
            )
            
        except discord.Forbidden:
            await ctx.send("I don't have permission to mute this user!", ephemeral=True)
        except Exception as e:
            await ctx.send(f"An error occurred: {e}", ephemeral=True)

    @commands.hybrid_command(name="addrole", description="Add a role to a user")
    @commands.guild_only()
    @app_commands.describe(
        user="The user to add the role to",
        role="The role to add"
    )
    async def addrole(
        self,
        ctx: commands.Context,
        user: discord.Member,
        role: discord.Role
    ) -> None:
        """Add a role to a user."""
        if role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.send("You cannot add a role that is higher than or equal to your highest role!", ephemeral=True)
            return
            
        if role in user.roles:
            await ctx.send(f"{user.mention} already has the {role.mention} role!", ephemeral=True)
            return

        try:
            await user.add_roles(role, reason=f"Added by {ctx.author}")
            await ctx.send(f"✅ Added role '{role.name}' to {user.mention}", ephemeral=True)
            
            # Log the action
            await self.log_mod_action(
                "role added",
                ctx.author,
                user,
                reason=f"Role: {role.name}"
            )
            
        except discord.Forbidden:
            await ctx.send("I don't have permission to add that role!", ephemeral=True)
        except Exception as e:
            await ctx.send(f"An error occurred: {e}", ephemeral=True)

    @commands.hybrid_command(name="removerole", description="Remove a role from a user")
    @commands.guild_only()
    @app_commands.describe(
        user="The user to remove the role from",
        role="The role to remove"
    )
    async def removerole(
        self,
        ctx: commands.Context,
        user: discord.Member,
        role: discord.Role
    ) -> None:
        """Remove a role from a user."""
        if role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.send(ErrorMessages.get_error('missing_perms'), ephemeral=True)
            return
            
        if role not in user.roles:
            await ctx.send(ErrorMessages.get_error('missing_role'), ephemeral=True)
            return

        try:
            await user.remove_roles(role, reason=f"Removed by {ctx.author}")
            await ctx.send(f"✅ Removed role '{role.name}' from {user.mention}", ephemeral=True)
            
            # Log the action
            await self.log_mod_action(
                "role removed",
                ctx.author,
                user,
                reason=f"Role: {role.name}"
            )
            
        except discord.Forbidden:
            await ctx.send(ErrorMessages.get_error('missing_perms'), ephemeral=True)
        except Exception as e:
            await ctx.send(ErrorMessages.get_error('generic'), ephemeral=True)
            
    @commands.hybrid_command(name="say", description="Make the bot say something")
    @commands.guild_only()
    @app_commands.describe(
        channel="The channel to send the message to (mention or ID)",
        message="The message to send"
    )
    async def say(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None, *, message: str) -> None:
        """Make the bot say something in the current or specified channel."""
        try:
            target_channel = channel or ctx.channel
            
            # Check if we have permission to send messages in the target channel
            if not target_channel.permissions_for(ctx.guild.me).send_messages:
                await ctx.send("I don't have permission to send messages in that channel!", ephemeral=True)
                return
                
            # Delete the command message if it's in a guild text channel
            if isinstance(ctx.channel, discord.TextChannel) and ctx.channel.permissions_for(ctx.guild.me).manage_messages:
                try:
                    await ctx.message.delete()
                except:
                    pass  # If we can't delete the message, continue anyway
                    
            # Send the message to the target channel
            await target_channel.send(message)
            
            # If we sent to a different channel, send a confirmation
            if target_channel != ctx.channel:
                await ctx.send(f"✅ Message sent to {target_channel.mention}", ephemeral=True, delete_after=5)
            
        except discord.Forbidden:
            await ctx.send("I don't have permission to send messages there!", ephemeral=True)
        except Exception as e:
            await ctx.send(f"An error occurred: {e}", ephemeral=True)

    @commands.hybrid_command(name='purge', description='Delete a specified number of messages in the current channel')
    @commands.guild_only()
    @app_commands.describe(amount='Number of messages to delete (1-100)')
    @commands.cooldown(1, 5, commands.BucketType.user)
    @commands.has_permissions(manage_messages=True)
    async def purge(self, ctx: commands.Context, amount: int):
        """Delete a specified number of messages in the current channel"""
        # Ensure the amount is within Discord's limit (1-100)
        amount = max(1, min(100, amount))
        
        # Check if we have permission to manage messages
        if not ctx.channel.permissions_for(ctx.me).manage_messages:
            await ctx.send("I don't have permission to delete messages in this channel!", ephemeral=True)
            return
            
        try:
            # Delete the command message first
            try:
                await ctx.message.delete()
            except:
                pass
                
            # Delete the specified number of messages
            deleted = await ctx.channel.purge(limit=amount)
            
            # Send a confirmation message that will auto-delete
            msg = await ctx.send(f"Deleted {len(deleted)} messages.", delete_after=5.0)
            
        except discord.Forbidden:
            await ctx.send("I don't have permission to delete messages in this channel!", ephemeral=True)
        except discord.HTTPException as e:
            await ctx.send(f"An error occurred while deleting messages: {e}", ephemeral=True)

    @commands.hybrid_command(name="nickname", description="Change a user's nickname.")
    @app_commands.describe(
        user="The user to change the nickname of (ID or mention)",
        nickname="The new nickname (leave empty to remove)"
    )
    @commands.guild_only()
    @commands.has_permissions(manage_nicknames=True)
    async def nickname(self, ctx: commands.Context, user: discord.Member, *, nickname: Optional[str] = None) -> None:
        """Change a user's nickname.
        
        Parameters
        ----------
        user: discord.Member
            The user to change the nickname of (ID or mention)
        nickname: Optional[str]
            The new nickname (leave empty to remove)
        """
        # Check if the target is protected
        from config.config import Config
        can_mod, reason = Config.can_moderate(ctx.author, user)
        if not can_mod:
            await ctx.send(ErrorMessages.get_error('protection_denied', reason=reason), ephemeral=True)
            return
            
        # Validate nickname length if provided
        if nickname and len(nickname) > 32:
            await ctx.send("Nickname must be 32 characters or less.", ephemeral=True)
            return
            
        try:
            # Change the nickname
            await user.edit(nick=nickname)
            
            # Send confirmation
            action = f"changed to '{nickname}'" if nickname else "removed"
            await ctx.send(f"✅ Nickname {action} for {user.mention}", ephemeral=True)
            
        except discord.Forbidden:
            await ctx.send("I don't have permission to change that user's nickname!", ephemeral=True)
        except Exception as e:
            await ctx.send(f"An error occurred: {e}", ephemeral=True)

async def setup(bot):
    await bot.add_cog(Moderation(bot))
