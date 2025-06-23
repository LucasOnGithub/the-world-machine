import discord
from discord.ext import commands
from discord import app_commands
import yaml
import sqlite3
import os
from typing import Dict, Any, Optional

# Database setup
def get_db_connection():
    conn = sqlite3.connect('guild_configs.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db_connection() as conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS guild_configs (
            guild_id INTEGER PRIMARY KEY,
            config_data TEXT NOT NULL
        )
        ''')
        conn.commit()

# Initialize the database when the module loads
init_db()

class ConfigCog(commands.Cog):
    """Server configuration management commands."""
    
    def __init__(self, bot):
        self.bot = bot
        self.default_config = {
            'staff_roles': [],
            'toss_category': None,
            'toss_logs': None,
            'voice_category': None,
            'apartment_lobby': None,
            'owner_roles': {}
        }
    
    def get_guild_config(self, guild_id: int) -> Dict[str, Any]:
        """Get the config for a guild from the database."""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT config_data FROM guild_configs WHERE guild_id = ?', (guild_id,))
            row = cursor.fetchone()
            
            if row:
                # Return the parsed config
                return yaml.safe_load(row['config_data'])
            else:
                # Return a new default config if none exists
                return self.default_config.copy()
    
    def save_guild_config(self, guild_id: int, config: Dict[str, Any]) -> None:
        """Save the config for a guild to the database."""
        # Ensure all required keys are present
        for key, default_value in self.default_config.items():
            if key not in config:
                config[key] = default_value
        
        with get_db_connection() as conn:
            # Convert config to YAML string
            config_yaml = yaml.dump(config, sort_keys=False)
            
            # Insert or update the config
            conn.execute('''
            INSERT INTO guild_configs (guild_id, config_data)
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                config_data = excluded.config_data
            ''', (guild_id, config_yaml))
            conn.commit()
    
    async def _config_check(self, ctx: commands.Context) -> bool:
        """Check if the command can be run in the current context."""
        if not ctx.guild:
            from discord.ext.commands import NoPrivateMessage
            raise NoPrivateMessage()
            
        if not ctx.author.guild_permissions.administrator:
            from discord.ext.commands import MissingPermissions
            raise MissingPermissions(['administrator'])
            
        return True
    
    @commands.hybrid_group(name="config", invoke_without_command=True)
    @app_commands.describe()
    async def config_group(self, ctx: commands.Context) -> None:
        """Show or manage server configuration."""
        # Run our custom checks
        if not await self._config_check(ctx):
            return
            
        print(f"[DEBUG] config_group command invoked by {ctx.author}")
        # Instead of showing help, show the current config
        await ctx.invoke(self.config_get)
    
    @config_group.command(name="get")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    async def config_get(self, ctx: commands.Context) -> None:
        """Get the current server configuration as a YAML file."""
        # Get the current config
        config = self.get_guild_config(ctx.guild.id)
        
        # Add comments to the YAML
        commented_yaml = """# Server Configuration
# ---------------------
# Staff Roles: List of role IDs that have staff permissions. 
# This will allow users with said staff roles to use staff commands.
# If you want this to be reduced to only one role, make a "Staff Team" role and give it to your staff members.
# This is a requirement.
# Example: [123456789012345678, 234567890123456789]
staff_roles: {staff_roles}

# Toss Category: Category ID where toss channels will be created. 
# Tossing is a moderation feature that allows staff to interrogate users if they have broken the rules.
# Example: 123456789012345678
toss_category: {toss_category}

# Toss Logs: Channel ID where toss logs will be sent
# Tossing is a moderation feature that allows staff to interrogate users if they have broken the rules.
# Example: 123456789012345678
toss_logs: {toss_logs}

# Voice Category: Category ID for voice channels
# This will be used for creating Apartment Rooms, which are voice channels that users can edit to their liking.
# Example: 123456789012345678
voice_category: {voice_category}

# Apartment Lobby: Voice channel ID for apartment lobby
# The Apartment Lobby VC is the voice channel a user joins for their VC to be created.
# It is recommended to put Apartment Lobby under all other voice channels, although this might change in the future.
# Example: 123456789012345678
apartment_lobby: {apartment_lobby}

# Owner Roles: Dictionary mapping owner IDs to co-owner IDs
# Let me simplify this. Owners can't moderate each other, but they can moderate Staff, which can't moderate owners, and members cant use moderation commands.
# This is a requirement.
# Format: {{owner_id: co_owner_id}}
# Example: {{123456789012345678: 234567890123456789}}
owner_roles: {owner_roles}""".format(
            staff_roles=config['staff_roles'],
            toss_category=config['toss_category'],
            toss_logs=config['toss_logs'],
            voice_category=config['voice_category'],
            apartment_lobby=config['apartment_lobby'],
            owner_roles=config['owner_roles']
        )
        
        # Convert to YAML for the actual file
        config_yaml = yaml.dump(config, sort_keys=False)
        
        # Create a temporary file with the commented YAML
        with open("config.yaml", "w") as f:
            f.write(commented_yaml)
        
        # Send the file
        try:
            await ctx.send(
                "Here's your server's configuration. Edit it and use `pls config set` to update.",
                file=discord.File("config.yaml")
            )
        finally:
            # Clean up the temporary file
            if os.path.exists("config.yaml"):
                os.remove("config.yaml")
    
    @config_group.command(name="set")
    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    async def config_set(self, ctx: commands.Context) -> None:
        """Update the server configuration from an attached YAML file."""
        # Check for attachments
        if not ctx.message.attachments:
            await ctx.send("❌ Please attach a YAML configuration file.", ephemeral=True)
            return
        
        # Get the first attachment
        attachment = ctx.message.attachments[0]
        
        # Check file extension
        if not attachment.filename.lower().endswith(('.yaml', '.yml')):
            await ctx.send("❌ Please upload a YAML file (.yaml or .yml).", ephemeral=True)
            return
        
        try:
            # Download and parse the YAML
            file_content = await attachment.read()
            new_config = yaml.safe_load(file_content)
            
            # Validate the config structure
            if not isinstance(new_config, dict):
                raise ValueError("Configuration must be a YAML dictionary")
            
            # Save the config
            self.save_guild_config(ctx.guild.id, new_config)
            
            await ctx.send("✅ Configuration updated successfully!", ephemeral=True)
            
        except yaml.YAMLError as e:
            await ctx.send(f"❌ Error parsing YAML: {str(e)}", ephemeral=True)
        except Exception as e:
            await ctx.send(f"❌ Error updating configuration: {str(e)}", ephemeral=True)

async def setup(bot):
    print("[DEBUG] Loading config_cog...")
    cog = ConfigCog(bot)
    await bot.add_cog(cog)
    print("[DEBUG] config_cog loaded successfully!")
