import discord
from discord.ext import commands

class WelcomeCog(commands.Cog):
    """Sends a welcome message when the bot joins a new server."""
    
    def __init__(self, bot):
        self.bot = bot
        
    @commands.Cog.listener()
    async def on_guild_join(self, guild: discord.Guild):
        """Send a welcome message when the bot joins a new server."""
        # Try to find a suitable channel to send the welcome message
        channel = None
        
        # First try to find a channel named 'general' (case-insensitive)
        for ch in guild.text_channels:
            if ch.name.lower() == 'general' and ch.permissions_for(guild.me).send_messages:
                channel = ch
                break
        
        # If no general channel found, try the system channel
        if not channel and guild.system_channel and guild.system_channel.permissions_for(guild.me).send_messages:
            channel = guild.system_channel
        
        # If still no channel, try to find any text channel we can send to
        if not channel:
            for ch in guild.text_channels:
                if ch.permissions_for(guild.me).send_messages:
                    channel = ch
                    break
        
        # If we found a channel, send the welcome message
        if channel:
            try:
                embed = discord.Embed(
                    title="👋 Thanks for adding me to your server!",
                    description=(
                        "I'm here to help with server management and moderation. "
                        "To get started, you'll need to set up your server's configuration."
                    ),
                    color=discord.Color.blue()
                )
                
                # Add configuration instructions
                embed.add_field(
                    name="📋 Getting Started",
                    value=(
                        "1. Use `pls config get` to see your current configuration\n"
                        "2. Edit the configuration as needed\n"
                        "3. Use `pls config set` with the updated file"
                    ),
                    inline=False
                )
                
                # Add helpful links
                embed.add_field(
                    name="🔗 Need Help?",
                    value=(
                        "• [Documentation](https://lucasongithub.github.io/the-world-machine)"
                    ),
                    inline=False
                )
                
                embed.set_footer(text="Type 'pls help' to see all available commands")
                
                await channel.send(embed=embed)
                
                # Also send a direct message to the server owner if possible
                try:
                    owner = guild.owner
                    if owner and not owner.bot:
                        try:
                            await owner.send(
                                f"Thanks for adding me to **{guild.name}**! "
                                "I've sent a welcome message in the server. "
                                "Please make sure to set up your server's configuration using `pls config`."
                            )
                        except discord.Forbidden:
                            pass  # Couldn't send DM to owner
                except Exception as e:
                    print(f"Error sending DM to server owner: {e}")
                    
            except Exception as e:
                print(f"Error sending welcome message in {guild.name}: {e}")
        else:
            print(f"Could not find a suitable channel to send welcome message in {guild.name}")

async def setup(bot):
    await bot.add_cog(WelcomeCog(bot))
