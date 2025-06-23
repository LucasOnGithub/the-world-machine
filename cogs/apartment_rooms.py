import json
import os
from typing import Dict, List, Optional, Set
from datetime import datetime

import discord
from discord.ext import commands
from discord import app_commands

from config.config import Config
from config.errors import ErrorMessages

# Constants
APARTMENT_DATA_FILE = "apartment_rooms.json"
DEFAULT_USER_LIMIT = 0

# Data structure for apartment room settings
class ApartmentSettings:
    """Stores settings for a user's apartment room."""
    
    def __init__(self, guild_id: int, user_id: int, name: str = "Apartment"):
        self.guild_id = guild_id
        self.user_id = user_id  # This is the owner's ID
        self.name = name
        self.user_limit = DEFAULT_USER_LIMIT
        self.locked = False
        self.banned_users: Set[int] = set()
        self.allowed_users: Set[int] = set()
        self.instruction_message_id: Optional[int] = None
        self.active_channel_id: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "user_limit": self.user_limit,
            "locked": self.locked,
            "banned_users": list(self.banned_users),
            "allowed_users": list(self.allowed_users),
            "instruction_message_id": self.instruction_message_id
            # Note: We don't save active_channel_id as it's temporary
            # Note: We don't save user_id as it's the key in the parent dict
        }

    @classmethod
    def from_dict(cls, data: dict, guild_id: int, user_id: int) -> 'ApartmentSettings':
        """Create ApartmentSettings from dict data.
        
        Args:
            data: The dictionary containing settings data
            guild_id: The guild ID that was the key in the outer dict
            user_id: The user ID that was the key in the guild dict (this is the owner)
        """
        if not guild_id:
            raise ValueError("guild_id is required")
        if not user_id:
            raise ValueError("user_id is required")
            
        # Create settings with required fields
        settings = cls(
            guild_id=guild_id,
            user_id=user_id,
            name=data.get("name", "Apartment")
        )
        
        # Set optional fields with defaults
        settings.user_limit = data.get("user_limit", DEFAULT_USER_LIMIT)
        settings.locked = data.get("locked", False)
        settings.banned_users = set(data.get("banned_users", []))
        settings.allowed_users = set(data.get("allowed_users", []))
        settings.instruction_message_id = data.get("instruction_message_id")
        return settings

class ApartmentRooms(commands.Cog):
    """Manage apartment room voice channels."""
    
    def __init__(self, bot):
        self.bot = bot
        # guild_id -> { user_id: ApartmentSettings }
        self.guild_apartments: Dict[int, Dict[int, ApartmentSettings]] = {}
        # user_id -> (guild_id, ApartmentSettings) for quick lookup
        self.user_apartments: Dict[int, Tuple[int, ApartmentSettings]] = {}
        self.load_data()
        
        # Track active channels and instruction messages
        self.active_channels: Dict[int, ApartmentSettings] = {}  # channel_id -> ApartmentSettings
        self.instruction_messages: Dict[int, int] = {}  # channel_id -> message_id
    
    def load_data(self) -> None:
        """Load apartment data from JSON file, organized by guild ID and user ID."""
        if not os.path.exists(APARTMENT_DATA_FILE):
            return
            
        try:
            with open(APARTMENT_DATA_FILE, 'r') as f:
                data = json.load(f)
                
            for guild_id_str, guild_data in data.items():
                try:
                    guild_id = int(guild_id_str)
                    self.guild_apartments[guild_id] = {}
                    
                    for user_id_str, apt_data in guild_data.items():
                        try:
                            user_id = int(user_id_str)
                            settings = ApartmentSettings.from_dict(apt_data, guild_id=guild_id, user_id=user_id)
                            
                            self.guild_apartments[guild_id][user_id] = settings
                            self.user_apartments[user_id] = (guild_id, settings)
                            
                        except (KeyError, ValueError) as e:
                            print(f"Error loading apartment for user {user_id_str} in guild {guild_id}: {e}")
                            continue
                            
                except (KeyError, ValueError) as e:
                    print(f"Error loading guild {guild_id_str}: {e}")
                    continue
                    
        except Exception as e:
            print(f"Error loading apartment data: {e}")
    
    def save_data(self) -> None:
        """Save apartment data to JSON file, organized by guild ID and user ID."""
        data = {}
        for guild_id, user_settings in self.guild_apartments.items():
            guild_key = str(guild_id)
            data[guild_key] = {}
            for user_id, settings in user_settings.items():
                data[guild_key][str(user_id)] = settings.to_dict()
        
        try:
            with open(APARTMENT_DATA_FILE, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Error saving apartment data: {e}")
    
    def get_user_apartment(self, user_id: int) -> Optional[ApartmentSettings]:
        """Get apartment settings for a user, if they own one."""
        user_data = self.user_apartments.get(user_id)
        if not user_data:
            return None
            
        guild_id, settings = user_data
        return settings
        
    def get_apartment_by_channel(self, channel_id: int) -> Optional[ApartmentSettings]:
        """Get apartment settings by channel ID."""
        return self.active_channels.get(channel_id)
    
    async def create_apartment_room(self, member: discord.Member) -> Optional[discord.VoiceChannel]:
        """Create a new apartment room for a member."""
        guild_config = Config.get_guild_config(member.guild.id)
        if not guild_config:
            return None
            
        category = member.guild.get_channel(guild_config['voice_category'])
        if not category or not isinstance(category, discord.CategoryChannel):
            return None
        
        # Get or create user settings
        if member.guild.id not in self.guild_apartments:
            self.guild_apartments[member.guild.id] = {}
            
        settings = self.guild_apartments[member.guild.id].get(member.id)
        if not settings:
            settings = ApartmentSettings(member.guild.id, member.id)
            self.guild_apartments[member.guild.id][member.id] = settings
            self.user_apartments[member.id] = (member.guild.id, settings)
        
        # Create the voice channel
        try:
            channel = await member.guild.create_voice_channel(
                name=settings.name or f"{member.name}'s Apartment",
                category=category,
                reason=f"Apartment room for {member}",
                user_limit=settings.user_limit
            )
            
            # Update active channel
            settings.active_channel_id = channel.id
            self.active_channels[channel.id] = settings
            
            # Send welcome message if we haven't already
            if not settings.instruction_message_id:
                message = await self.send_welcome_message(channel, member)
                if message:
                    settings.instruction_message_id = message.id
                    self.instruction_messages[channel.id] = message.id
            
            self.save_data()
            return channel
            
        except Exception as e:
            print(f"Error creating apartment room: {e}")
            return None
    
    async def send_welcome_message(self, channel: discord.VoiceChannel, owner: discord.Member) -> Optional[discord.Message]:
        """Send welcome message to the apartment room owner via DMs."""
        embed = discord.Embed(
            title="🏠 Welcome to your Apartment Room",
            description=(
                "This is your personal space! You can customize it using these commands:\n\n"
                "• `pls voice help` - Show all available commands\n"
                "• `pls voice limit <number>` - Set user limit (0 for unlimited)\n"
                "• `pls voice lock` - Lock/unlock your room\n"
                "• `pls voice kick @user` - Kick a user\n"
                "• `pls voice ban @user` - Ban/unban a user"
            ),
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        embed.set_footer(text=f"Room: {channel.name}")
        
        try:
            return await owner.send(embed=embed)
        except Exception as e:
            print(f"Error sending welcome message to {owner}: {e}")
            return None
    
    async def cleanup_apartment(self, channel_id: int) -> None:
        """Clean up an apartment room when it's empty."""
        settings = self.active_channels.get(channel_id)
        if not settings:
            return
            
        try:
            # Try to delete the instruction message if it exists
            if settings.instruction_message_id:
                try:
                    user = self.bot.get_user(settings.user_id)
                    if user:
                        try:
                            channel = await user.create_dm()
                            message = await channel.fetch_message(settings.instruction_message_id)
                            await message.delete()
                        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                            pass  # Message already deleted or no permission
                except Exception as e:
                    print(f"Error deleting instruction message: {e}")
            
            # Delete the channel
            channel = self.bot.get_channel(channel_id)
            if channel:
                await channel.delete(reason="Apartment room empty")
                
        except Exception as e:
            print(f"Error cleaning up apartment {channel_id}: {e}")
        finally:
            # Clean up active channels and instruction messages
            if channel_id in self.active_channels:
                del self.active_channels[channel_id]
                
            if channel_id in self.instruction_messages:
                del self.instruction_messages[channel_id]
                
            settings.active_channel_id = None  # Mark channel as inactive
            self.save_data()    
            if channel_id in self.instruction_messages:
                del self.instruction_messages[channel_id]
                
            self.save_data()
    
    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        """Handle voice state updates for apartment rooms."""
        # Check if user joined a voice channel
        if not before.channel and after.channel:
            await self.handle_voice_join(member, after.channel)
        
        # Check if user left a voice channel
        if before.channel and before.channel != after.channel:
            # Check if it's an apartment channel and if the channel is now empty
            settings = self.get_apartment_by_channel(before.channel.id)
            if settings and len(before.channel.members) == 0:
                await self.handle_voice_leave(before.channel)
    
    async def handle_voice_join(self, member: discord.Member, channel: discord.VoiceChannel) -> None:
        """Handle a user joining a voice channel."""
        # Ignore if the bot is the one joining
        if member.bot:
            return
            
        guild_config = Config.get_guild_config(member.guild.id)
        if not guild_config:
            return
        
        # Check if user joined the apartment lobby
        if channel.id == guild_config.get('apartment_lobby'):
            # Check if voice category is set up
            voice_category_id = guild_config.get('voice_category')
            if not voice_category_id:
                # Try to DM the user about the error
                try:
                    embed = discord.Embed(
                        title="⚠️ Apartment System Not Configured",
                        description="The apartment system is not fully set up on this server.\n\n"
                                  "Would you like me to notify the server staff about this issue?",
                        color=discord.Color.orange()
                    )
                    
                    # Only show the reaction options if staff roles are configured
                    staff_roles = guild_config.get('staff_roles', [])
                    if staff_roles:
                        msg = await member.send(embed=embed)
                        await msg.add_reaction("✅")  # Checkmark
                        await msg.add_reaction("❌")  # X
                        
                        def check(reaction, user):
                            return user == member and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == msg.id
                        
                        try:
                            reaction, _ = await self.bot.wait_for('reaction_add', timeout=60.0, check=check)
                            if str(reaction.emoji) == "✅":
                                # Find a channel to notify staff
                                notify_channel = None
                                
                                # Try system channel first
                                if member.guild.system_channel:
                                    notify_channel = member.guild.system_channel
                                # Then try toss_logs if set
                                elif 'toss_logs' in guild_config:
                                    notify_channel = member.guild.get_channel(guild_config['toss_logs'])
                                
                                if notify_channel:
                                    staff_mentions = ' '.join([f'<@&{role_id}>' for role_id in staff_roles])
                                    await notify_channel.send(
                                        f"{staff_mentions} - {member.mention} tried to use the apartment system "
                                        "but the voice category is not set up in the config."
                                    )
                                    await member.send("✅ Staff have been notified about this issue.")
                                else:
                                    await member.send("❌ Could not find a suitable channel to notify staff.")
                            
                            # Remove reactions after handling
                            try:
                                await msg.clear_reactions()
                            except:
                                pass
                                
                        except asyncio.TimeoutError:
                            try:
                                await msg.clear_reactions()
                            except:
                                pass
                    else:
                        # No staff roles configured, just inform the user
                        embed.description = "The apartment system is not fully set up on this server.\nPlease contact a server administrator."
                        await member.send(embed=embed)
                        
                except Exception as e:
                    print(f"Error handling apartment lobby join: {e}")
                
                # Disconnect the user from the lobby
                try:
                    await member.move_to(None, reason="Apartment system not configured")
                except:
                    pass
                return
                
            # Get or create user's apartment settings
            settings = self.get_user_apartment(member.id)
            
            # If no settings exist, create a new apartment
            if not settings:
                new_channel = await self.create_apartment_room(member)
                if new_channel:
                    try:
                        await member.move_to(new_channel, reason="Moving to new apartment room")
                    except Exception as e:
                        print(f"Error moving user to new apartment: {e}")
                        try:
                            await member.send("❌ Failed to move you to your apartment room. Please try again.")
                        except:
                            pass
                return
                
            # If we have settings but no active channel, create one
            if not settings.active_channel_id:
                new_channel = await self.create_apartment_room(member)
                if new_channel:
                    try:
                        await member.move_to(new_channel, reason="Recreating apartment room")
                    except Exception as e:
                        print(f"Error moving user to recreated apartment: {e}")
                return
                
            # If we have an active channel, check if it exists and move user there
            try:
                apt_channel = self.bot.get_channel(settings.active_channel_id)
                if apt_channel and isinstance(apt_channel, discord.VoiceChannel):
                    # Check if the channel still exists in the guild
                    if apt_channel in member.guild.voice_channels:
                        await member.move_to(apt_channel, reason="Moving to existing apartment")
                        return
                
                # If we get here, the channel doesn't exist or is invalid
                # Clean up and create a new one
                await self.cleanup_apartment(settings.active_channel_id)
                settings.active_channel_id = None
                new_channel = await self.create_apartment_room(member)
                if new_channel:
                    await member.move_to(new_channel, reason="Recreating deleted apartment room")
            except Exception as e:
                print(f"Error handling apartment join: {e}")
                # If anything fails, try to create a new apartment
                if settings.active_channel_id:
                    await self.cleanup_apartment(settings.active_channel_id)
                    settings.active_channel_id = None
                new_channel = await self.create_apartment_room(member)
                if new_channel:
                    await member.move_to(new_channel, reason="Recovering from error")
    
    async def handle_voice_leave(self, channel: discord.VoiceChannel) -> None:
        """Handle a user leaving an apartment room."""
        settings = self.get_apartment_by_channel(channel.id)
        if not settings:
            return
            
        # Clean up if the room is empty
        if len(channel.members) == 0:
            await self.cleanup_apartment(channel.id)
    
    @commands.hybrid_group(name="voice", invoke_without_command=True)
    async def voice_group(self, ctx: commands.Context) -> None:
        """Apartment room management commands."""
        await ctx.send_help(ctx.command)
    
    @voice_group.command(name="limit")
    @app_commands.describe(limit="Maximum number of users (0 for unlimited)")
    async def voice_limit(self, ctx: commands.Context, limit: int) -> None:
        """Set the user limit for your apartment room."""
        settings = self.get_user_apartment(ctx.author.id)
        if not settings:
            await ctx.send("You don't have an active apartment room.", delete_after=10)
            return
            
        # Validate limit
        if limit < 0 or limit > 99:
            await ctx.send("User limit must be between 0 and 99 (0 for unlimited).", delete_after=10)
            return
            
        try:
            if not settings.active_channel_id:
                await ctx.send("Your apartment room is not currently active.", delete_after=10)
                return
                
            channel = self.bot.get_channel(settings.active_channel_id)
            if not channel or not isinstance(channel, discord.VoiceChannel):
                await ctx.send("Could not find your apartment room.", delete_after=10)
                return
                
            await channel.edit(user_limit=limit if limit > 0 else 0)
            settings.user_limit = limit
            self.save_data()
            
            await ctx.send(f"✅ User limit set to {'unlimited' if limit == 0 else limit}", delete_after=10)
            
        except Exception as e:
            print(f"Error setting user limit: {e}")
            await ctx.send("An error occurred while updating the user limit.", delete_after=10)
    
    @voice_group.command(name="lock")
    async def voice_lock(self, ctx: commands.Context) -> None:
        """Lock your apartment room (sets user limit to 1)."""
        settings = self.get_user_apartment(ctx.author.id)
        if not settings:
            await ctx.send("You don't have an active apartment room.", delete_after=10)
            return
            
        try:
            if not settings.active_channel_id:
                await ctx.send("Your apartment room is not currently active.", delete_after=10)
                return
                
            channel = self.bot.get_channel(settings.active_channel_id)
            if not channel or not isinstance(channel, discord.VoiceChannel):
                await ctx.send("Could not find your apartment room.", delete_after=10)
                return
                
            settings.locked = True
            settings.user_limit = 1
            await channel.edit(user_limit=1)
            await ctx.send("🔒 Your apartment room is now locked (user limit set to 1).", delete_after=10)
            self.save_data()
            
        except Exception as e:
            print(f"Error locking room: {e}")
            await ctx.send("An error occurred while locking your room.", delete_after=10)
    
    @voice_group.command(name="unlock")
    async def voice_unlock(self, ctx: commands.Context) -> None:
        """Unlock your apartment room (sets user limit to 0 for unlimited)."""
        settings = self.get_user_apartment(ctx.author.id)
        if not settings:
            await ctx.send("You don't have an active apartment room.", delete_after=10)
            return
            
        try:
            if not settings.active_channel_id:
                await ctx.send("Your apartment room is not currently active.", delete_after=10)
                return
                
            channel = self.bot.get_channel(settings.active_channel_id)
            if not channel or not isinstance(channel, discord.VoiceChannel):
                await ctx.send("Could not find your apartment room.", delete_after=10)
                return
                
            settings.locked = False
            settings.user_limit = 0  # Set to unlimited
            await channel.edit(user_limit=0)
            await ctx.send("🔓 Your apartment room is now unlocked (user limit set to unlimited).", delete_after=10)
            self.save_data()
            
        except Exception as e:
            print(f"Error unlocking room: {e}")
            await ctx.send("An error occurred while unlocking your room.", delete_after=10)
    
    @voice_group.command(name="kick")
    @app_commands.describe(member="The user to kick from your room")
    async def voice_kick(self, ctx: commands.Context, member: discord.Member) -> None:
        """Kick a user from your apartment room."""
        settings = self.get_user_apartment(ctx.author.id)
        if not settings:
            await ctx.send("You don't have an active apartment room.", delete_after=10)
            return
            
        if member.id == ctx.author.id:
            await ctx.send("You can't kick yourself!", delete_after=10)
            return
            
        try:
            channel = self.bot.get_channel(settings.channel_id)
            if not channel or not isinstance(channel, discord.VoiceChannel):
                await ctx.send("Could not find your apartment room.", delete_after=10)
                return
                
            # Check if the target is in the voice channel
            if member.voice and member.voice.channel and member.voice.channel.id == channel.id:
                await member.move_to(None, reason=f"Kicked by {ctx.author}")
                await ctx.send(f"✅ Kicked {member.mention} from your apartment room.", delete_after=10)
            else:
                await ctx.send("That user is not in your apartment room.", delete_after=10)
                
        except Exception as e:
            print(f"Error kicking user: {e}")
            await ctx.send("An error occurred while trying to kick the user.", delete_after=10)
    
    @voice_group.command(name="ban")
    @app_commands.describe(member="The user to ban from your room")
    async def voice_ban(self, ctx: commands.Context, member: discord.Member) -> None:
        """Ban or unban a user from your apartment room."""
        settings = self.get_user_apartment(ctx.author.id)
        if not settings:
            await ctx.send("You don't have an active apartment room.", delete_after=10)
            return
            
        if member.id == ctx.author.id:
            await ctx.send("You can't ban yourself!", delete_after=10)
            return
            
        try:
            # Toggle ban status
            if member.id in settings.banned_users:
                settings.banned_users.remove(member.id)
                action = "unbanned from"
            else:
                settings.banned_users.add(member.id)
                action = "banned from"
                
            self.save_data()
            
            # Kick the user if they're in the room
            if settings.active_channel_id:
                channel = self.bot.get_channel(settings.active_channel_id)
                if (isinstance(channel, discord.VoiceChannel) and 
                    member.voice and member.voice.channel and 
                    member.voice.channel.id == channel.id):
                    await member.move_to(None, reason=f"{action.capitalize()} by {ctx.author}")
                
            await ctx.send(f"✅ {member.mention} has been {action} your apartment room.", delete_after=10)
            
        except Exception as e:
            print(f"Error banning user: {e}")
            await ctx.send("An error occurred while trying to update the ban list.", delete_after=10)
    
    @voice_group.command(name="name")
    @app_commands.describe(new_name="The new name for your apartment")
    async def voice_name(self, ctx: commands.Context, *, new_name: str) -> None:
        """Rename your apartment room."""
        settings = self.get_user_apartment(ctx.author.id)
        if not settings:
            await ctx.send("You don't have an active apartment room.", delete_after=10)
            return
            
        # Validate name length
        if len(new_name) > 32:
            await ctx.send("Apartment name must be 32 characters or less.", delete_after=10)
            return
            
        try:
            if not settings.active_channel_id:
                await ctx.send("Your apartment room is not currently active.", delete_after=10)
                return
                
            channel = self.bot.get_channel(settings.active_channel_id)
            if not channel or not isinstance(channel, discord.VoiceChannel):
                await ctx.send("Could not find your apartment room.", delete_after=10)
                return
                
            # Update channel name
            await channel.edit(name=f"{new_name}")
            settings.name = new_name
            self.save_data()
            
            await ctx.send(f"✅ Apartment renamed to: {new_name}", delete_after=10)
            
        except Exception as e:
            print(f"Error renaming apartment: {e}")
            await ctx.send("An error occurred while renaming your apartment.", delete_after=10)
    
    @voice_group.command(name="help")
    async def voice_help(self, ctx: commands.Context) -> None:
        """Show help for apartment room commands."""
        # Check if this is in a DM or a server
        if isinstance(ctx.channel, discord.DMChannel):
            # In DMs, we need to find the user's apartment
            settings = self.get_user_apartment(ctx.author.id)
            if not settings:
                await ctx.send("You don't have an active apartment room.")
                return
                
            # Send help message in DMs
            await self._send_help_embed(ctx, settings)
        else:
            # In a server, check if the channel is an apartment room
            if not ctx.guild or not ctx.guild.me.guild_permissions.manage_channels:
                await ctx.send("I need the 'Manage Channels' permission to function properly.", delete_after=10)
                return
                
            # Check if this is an apartment channel
            settings = self.get_apartment_by_channel(ctx.channel.id)
            if not settings:
                # Not in an apartment channel, check if they own one
                settings = self.get_user_apartment(ctx.author.id)
                if not settings:
                    await ctx.send("You don't have an active apartment room.", delete_after=10)
                    return
            
            # Send help message
            await self._send_help_embed(ctx, settings)
    
    async def _send_help_embed(self, ctx: commands.Context, settings: ApartmentSettings) -> None:
        """Send the help embed with apartment room commands."""
        embed = discord.Embed(
            title="🏠 Apartment Room Commands",
            description=(
                "Manage your personal apartment room with these commands:\n\n"
                "• `pls voice help` - Show this help message\n"
                f"• `pls voice limit <number>` - Set user limit (0 for unlimited, current: {settings.user_limit if settings.user_limit > 0 else 'unlimited'})\n"
                "• `pls voice lock` - Lock room (sets user limit to 1)\n"
                "• `pls voice unlock` - Unlock room (sets user limit to 0 for unlimited)\n"
                "• `pls voice name <name>` - Rename your apartment room\n"
                "• `pls voice kick @user` - Kick a user from your room\n"
                "• `pls voice ban @user` - Ban/unban a user from your room"
            ),
            color=discord.Color.blue()
        )
        
        # Add current room status
        status = [
            f"👥 User limit: {settings.user_limit if settings.user_limit > 0 else 'unlimited'}",
            f"🔒 Status: {'Locked' if settings.locked else 'Unlocked'}"
        ]
            
        if settings.banned_users:
            banned_mentions = [f"<@{uid}>" for uid in settings.banned_users]
            status.append(f"🚫 Banned users: {', '.join(banned_mentions) if len(banned_mentions) <= 5 else f'{len(banned_mentions)} users'}")
            
        if status:
            embed.add_field(name="Room Status", value="\n".join(status), inline=False)
        
        # Try to send in the channel, fall back to DMs if no permissions
        try:
            if isinstance(ctx.channel, discord.DMChannel) or ctx.channel.permissions_for(ctx.guild.me).send_messages:
                await ctx.send(embed=embed, delete_after=60 if not isinstance(ctx.channel, discord.DMChannel) else None)
            else:
                await ctx.author.send(embed=embed)
        except Exception as e:
            print(f"Error sending help message: {e}")
    


async def setup(bot):
    await bot.add_cog(ApartmentRooms(bot))
