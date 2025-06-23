import os
import sqlite3
import logging
import asyncio
import aiohttp
import io
import matplotlib.pyplot as plt
import discord
from discord.ext import commands
from discord import app_commands
from typing import Optional, Literal
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ssc_ranking.log')
    ]
)
logger = logging.getLogger('ssc_ranking')

# Ensure ssc directory exists
os.makedirs('ssc', exist_ok=True)

# Database file paths
DB_DIR = 'ssc'
RANKED_MAPS_DB = os.path.join(DB_DIR, 'ranked_maps.db')
PENDING_APPS_DB = os.path.join(DB_DIR, 'pending_applications.db')
APPROVED_USERS_DB = os.path.join(DB_DIR, 'approved_users.db')
USER_PASSES_DB = os.path.join(DB_DIR, 'user_passes.db')

# Initialize database directory and files
def init_db():
    logger.info('Initializing databases...')
    os.makedirs(DB_DIR, exist_ok=True)
    
    # Initialize ranked maps database
    init_ranked_maps_db()
    
    # Initialize pending applications database
    init_pending_apps_db()
    
    # Initialize approved users database
    init_approved_users_db()

def init_pending_apps_db():
    """Initialize the pending applications database."""
    try:
        conn = sqlite3.connect(PENDING_APPS_DB)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS user_applications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discord_id TEXT NOT NULL,
                    discord_username TEXT NOT NULL,
                    beatleader_id TEXT NOT NULL,
                    beatleader_username TEXT NOT NULL,
                    pp REAL NOT NULL,
                    rank INTEGER NOT NULL,
                    country_rank INTEGER NOT NULL,
                    country TEXT NOT NULL,
                    avatar_url TEXT,
                    profile_url TEXT NOT NULL,
                    application_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    reviewed_by TEXT,
                    review_time TIMESTAMP,
                    UNIQUE(discord_id, beatleader_id)
                )''')
                
        conn.commit()
        logger.info("Pending applications database initialized")
        
    except sqlite3.Error as e:
        logger.error(f"Error initializing pending applications database: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def init_approved_users_db():
    """Initialize the approved users database."""
    try:
        conn = sqlite3.connect(APPROVED_USERS_DB)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS approved_users (
                    discord_id TEXT PRIMARY KEY,
                    beatleader_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    pp REAL NOT NULL,
                    rank INTEGER NOT NULL,
                    country_rank INTEGER NOT NULL,
                    country TEXT NOT NULL,
                    avatar_url TEXT,
                    profile_url TEXT NOT NULL,
                    approved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    approved_by TEXT NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )''')
                
        conn.commit()
        logger.info("Approved users database initialized")
        
    except sqlite3.Error as e:
        logger.error(f"Error initializing approved users database: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            
    try:
        conn = sqlite3.connect(USER_PASSES_DB)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_passes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_id TEXT NOT NULL,
                beatleader_id TEXT NOT NULL,
                song_name TEXT NOT NULL,
                characteristic TEXT NOT NULL,
                difficulty TEXT NOT NULL,
                level INTEGER NOT NULL,
                accuracy REAL NOT NULL,
                points INTEGER NOT NULL,
                passed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(discord_id, song_name, characteristic, difficulty)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS disallowed_passes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_id TEXT NOT NULL,
                beatleader_id TEXT NOT NULL,
                song_name TEXT NOT NULL,
                characteristic TEXT NOT NULL,
                difficulty TEXT NOT NULL,
                level INTEGER NOT NULL,
                accuracy REAL NOT NULL,
                modifiers TEXT NOT NULL,
                passed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(discord_id, song_name, characteristic, difficulty, modifiers)
            )
        ''')
        # Create index for faster lookups
        c.execute('CREATE INDEX IF NOT EXISTS idx_user_passes_discord ON user_passes(discord_id)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_user_passes_bl ON user_passes(beatleader_id)')
        conn.commit()
        logger.info("User passes database initialized")
    except Exception as e:
        logger.error(f"Error initializing user passes database: {e}", exc_info=True)
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def init_ranked_maps_db():
    """Initialize the ranked maps database."""
    try:
        conn = sqlite3.connect(RANKED_MAPS_DB)
        c = conn.cursor()
        
        c.execute('''CREATE TABLE IF NOT EXISTS ranked_maps
                    (id TEXT PRIMARY KEY,
                     name TEXT NOT NULL,
                     song_name TEXT NOT NULL,
                     song_author TEXT NOT NULL,
                     level_author TEXT,
                     bpm REAL NOT NULL,
                     duration INTEGER NOT NULL,
                     cover_url TEXT,
                     download_url TEXT,
                     characteristic TEXT NOT NULL,
                     difficulty TEXT NOT NULL,
                     nps REAL NOT NULL,
                     notes INTEGER NOT NULL,
                     njs REAL NOT NULL,
                     category TEXT NOT NULL,
                     level INTEGER NOT NULL,
                     ranked_by INTEGER NOT NULL,
                     ranked_at TEXT NOT NULL,
                     song_hash TEXT)''')
        
        # Add song_hash column if it doesn't exist (for existing databases)
        c.execute("PRAGMA table_info(ranked_maps)")
        columns = [column[1] for column in c.fetchall()]
        if 'song_hash' not in columns:
            c.execute('ALTER TABLE ranked_maps ADD COLUMN song_hash TEXT')
    
        conn.commit()
        logger.info('Database initialized successfully')
    except sqlite3.Error as e:
        logger.error(f'Error initializing database: {e}')
        raise
    finally:
        if conn:
            conn.close()

# Initialize the database
init_db()

class PaginatedView(discord.ui.View):
    """A view that provides pagination for embeds."""
    
    def __init__(self, embeds: list[discord.Embed], timeout: float = 180.0):
        super().__init__(timeout=timeout)
        self.embeds = embeds
        self.current_page = 0
        self.update_buttons()
    
    def update_buttons(self):
        """Update the state of navigation buttons based on current page."""
        self.first_page.disabled = self.current_page == 0
        self.previous_page.disabled = self.current_page == 0
        self.next_page.disabled = self.current_page == len(self.embeds) - 1
        self.last_page.disabled = self.current_page == len(self.embeds) - 1
        self.page_counter.label = f"{self.current_page + 1}/{len(self.embeds)}"
    
    @discord.ui.button(emoji="⏮", style=discord.ButtonStyle.grey)
    async def first_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to the first page."""
        if self.current_page != 0:
            self.current_page = 0
            self.update_buttons()
            await interaction.response.edit_message(embed=self.embeds[0], view=self)
    
    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.primary)
    async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to the previous page."""
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)
    
    @discord.ui.button(style=discord.ButtonStyle.grey, disabled=True)
    async def page_counter(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Display current page number (non-interactive)."""
        pass
    
    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to the next page."""
        if self.current_page < len(self.embeds) - 1:
            self.current_page += 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)
    
    @discord.ui.button(emoji="⏭", style=discord.ButtonStyle.grey)
    async def last_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to the last page."""
        if self.current_page != len(self.embeds) - 1:
            self.current_page = len(self.embeds) - 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.embeds[-1], view=self)
    
    @discord.ui.button(emoji="❌", style=discord.ButtonStyle.danger)
    async def delete_message(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Delete the message."""
        await interaction.message.delete()
        self.stop()
    
    async def on_timeout(self):
        """Disable all buttons when the view times out."""
        for item in self.children:
            item.disabled = True
        try:
            await self.message.edit(view=self)
        except discord.NotFound:
            pass


class SSCTools(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.ranking_team_role = "Ranking Team"  # Role that can use rank commands
        self.categories = ["tech", "jumps", "streams", "shitpost", "vibro"]
        self.characteristics = ["Standard", "Lawless", "OneSaber"]
        self.difficulties = ["Easy", "Normal", "Hard", "Expert", "Expert+"]
        self.review_channel_id = 1384780527751397436  # Channel for reviewing applications
        self.pending_applications = {}  # message_id -> application_data
        self.allowed_guild_id = 1371217348660560023  # SSC server ID
        self.owner_id = 1125692090123309056  # Your user ID
        
        # Load pending applications when the cog is loaded
        self.bot.loop.create_task(self.load_pending_applications())
        
    async def check_guild_permission(self, interaction: discord.Interaction) -> bool:
        """Check if the command is being used in the allowed server or by the owner."""
        if interaction.user.id == self.owner_id:
            return True
            
        if interaction.guild_id != self.allowed_guild_id:
            await interaction.response.send_message(
                "❌ This command can only be used in the SSC server.",
                ephemeral=True
            )
            return False
            
        return True

    async def get_map_data(self, bsr_code: str) -> dict:
        """Fetch map data from BeatSaver API."""
        url = f"https://api.beatsaver.com/maps/id/{bsr_code}"
        logger.info(f'Fetching map data for BSR: {bsr_code}')
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        logger.debug(f'Successfully fetched map data for BSR: {bsr_code}')
                        return await response.json()
                    else:
                        error_msg = f"Failed to fetch map data: {response.status}"
                        logger.warning(f'{error_msg} for BSR: {bsr_code}')
                        return {"error": error_msg}
        except Exception as e:
            error_msg = f"An error occurred: {str(e)}"
            logger.error(f'{error_msg} while fetching BSR: {bsr_code}', exc_info=True)
            return {"error": error_msg}

    def save_ranked_map(self, map_data: dict) -> bool:
        """Save ranked map to the SQLite database."""
        conn = None
        try:
            logger.info(f"Saving map to database: {map_data.get('name')} (ID: {map_data.get('id')})")
            conn = sqlite3.connect(RANKED_MAPS_DB)
            c = conn.cursor()
            
            # Check if map already exists
            c.execute('SELECT id, name FROM ranked_maps WHERE id = ?', (map_data['id'],))
            if existing_map := c.fetchone():
                logger.warning(f"Map already exists in database: {existing_map[1]} (ID: {existing_map[0]})")
                return False  # Map already exists
            
            # Insert new map
            c.execute('''
                INSERT INTO ranked_maps (
                    id, name, song_name, song_author, level_author, bpm, duration,
                    cover_url, download_url, characteristic, difficulty, nps, notes,
                    njs, category, level, ranked_by, ranked_at, song_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                map_data['id'],
                map_data['name'],
                map_data['songName'],
                map_data['songAuthorName'],
                map_data.get('levelAuthorName', 'Unknown'),
                map_data['bpm'],
                map_data['duration'],
                map_data.get('coverURL'),
                map_data.get('downloadURL'),
                map_data['characteristic'],
                map_data['difficulty'],
                map_data.get('nps', 0),
                map_data.get('notes', 0),
                map_data.get('njs', 0),
                map_data['category'],
                map_data['level'],
                map_data['ranked_by'],
                map_data.get('ranked_at', datetime.utcnow().isoformat()),
                map_data.get('song_hash')
            ))
            
            conn.commit()
            logger.info(f"Successfully saved map: {map_data.get('name')} (ID: {map_data.get('id')})")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Database error while saving map {map_data.get('id')}: {e}", exc_info=True)
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logger.error(f"Error closing database connection: {e}", exc_info=True)

    @app_commands.command(name="viewallmaps", description="View all ranked maps with pagination")
    @app_commands.describe(show_levels="Show map levels distribution")
    async def view_all_maps(self, interaction: discord.Interaction, show_levels: bool = False):
        """View all ranked maps with pagination (4 maps per page)."""
        await interaction.response.defer()
        
        if show_levels:
            try:
                conn = sqlite3.connect(RANKED_MAPS_DB)
                c = conn.cursor()
                
                # Get level distribution
                c.execute("""
                    SELECT 
                        CASE 
                            WHEN level BETWEEN 1 AND 32 THEN level
                            ELSE 100
                        END as level_group,
                        COUNT(*) as count
                    FROM ranked_maps
                    GROUP BY level_group
                    ORDER BY level_group
                """)
                
                level_data = {str(level): count for level, count in c.fetchall()}
                
                if not level_data:
                    await interaction.followup.send("❌ No map data available.")
                    return
                
                # Generate map counts for each level range
                total_maps = 0
                
                # Function to generate lines for a range of levels
                def generate_level_lines(start, end):
                    nonlocal total_maps
                    lines = []
                    for level in range(start, end + 1):
                        count = level_data.get(str(level), 0)
                        total_maps += count
                        lines.append(f"Lvl {level:>2}: {count:>3}")
                    return lines
                
                # Generate lines for each level range
                lines_1_10 = generate_level_lines(1, 10)
                lines_11_20 = generate_level_lines(11, 20)
                lines_21_32 = generate_level_lines(21, 32)
                
                # Add level 100 (unranked)
                count_100 = level_data.get('100', 0)
                total_maps += count_100
                
                # Combine all lines with proper spacing
                max_lines = max(len(lines_1_10), len(lines_11_20), len(lines_21_32))
                combined_lines = []
                
                # Add header
                combined_lines.append("Levels 1-10".ljust(20) + "Levels 11-20".ljust(20) + "Levels 21-32")
                combined_lines.append("-" * 60)
                
                # Combine the columns
                for i in range(max_lines):
                    col1 = lines_1_10[i] if i < len(lines_1_10) else ""
                    col2 = lines_11_20[i] if i < len(lines_11_20) else ""
                    col3 = lines_21_32[i] if i < len(lines_21_32) else ""
                    combined_lines.append(f"{col1.ljust(20)}{col2.ljust(20)}{col3}")
                
                # Add level 100 at the bottom
                combined_lines.append("")
                combined_lines.append(f"Lvl 100: {count_100:>3}")
                
                # Create the embed with all levels in description
                description = "```\n" + "\n".join(combined_lines) + "\n```"
                embed = discord.Embed(
                    title="📊 Map Levels Distribution",
                    description=description,
                    color=discord.Color.blue()
                )
                
                embed.set_footer(text=f"Total: {total_maps} maps")
                await interaction.followup.send(embed=embed)
                return
                
            except Exception as e:
                logger.error(f"Error generating levels display: {e}", exc_info=True)
                await interaction.followup.send("❌ Failed to generate levels display.")
                return
        
        try:
            conn = sqlite3.connect(RANKED_MAPS_DB)
            c = conn.cursor()
            
            # Get total number of maps
            c.execute("SELECT COUNT(*) FROM ranked_maps")
            total_maps = c.fetchone()[0]
            
            if total_maps == 0:
                await interaction.followup.send("❌ No ranked maps found in the database.")
                return
                
            # Get all maps ordered by ID
            c.execute("""
                SELECT id, song_name, song_author, characteristic, difficulty, level, 
                       category, ranked_by, cover_url, level_author
                FROM ranked_maps 
                ORDER BY id
            """)
            
            all_maps = c.fetchall()
            
            # Split maps into pages of 4
            maps_per_page = 4
            pages = [all_maps[i:i + maps_per_page] for i in range(0, len(all_maps), maps_per_page)]
            
            # Create embeds for the first page
            embeds = []
            for i, (map_id, song_name, song_author, char, diff, level, category, 
                    ranked_by, cover_url, level_author) in enumerate(pages[0], 1):
                title = f"{song_author} - {song_name}" if song_author else song_name
                embed = discord.Embed(
                    title=title,
                    description=f"Mapped by: {level_author}" if level_author else None,
                    color=discord.Color.blue()
                )
                
                if map_id.startswith('!'):
                    embed.url = f"https://beatsaver.com/maps/{map_id[1:]}"
                
                embed.add_field(name="ID", value=f"`{map_id}`", inline=True)
                embed.add_field(name="Difficulty", value=f"{char} {diff}", inline=True)
                embed.add_field(name="Level", value=str(level), inline=True)
                embed.add_field(name="Category", value=category.capitalize(), inline=True)
                
                if ranked_by:
                    embed.add_field(name="Ranked By", value=f"<@{ranked_by}>", inline=True)
                
                if cover_url:
                    embed.set_thumbnail(url=cover_url)
                
                embed.set_footer(text=f"Map {i} of {len(pages[0])} • Page 1 of {len(pages)}")
                embeds.append(embed)
            
            # Create view with pagination
            view = discord.ui.View(timeout=180.0)
            current_page = 0
            
            async def update_page(interaction: discord.Interaction, page: int):
                nonlocal current_page, embeds
                current_page = page
                
                # Clear existing embeds
                embeds = []
                
                # Create new embeds for this page
                for i, (map_id, song_name, song_author, char, diff, level, category, 
                        ranked_by, cover_url, level_author) in enumerate(pages[page], 1):
                    title = f"{song_author} - {song_name}" if song_author else song_name
                    embed = discord.Embed(
                        title=title,
                        description=f"Mapped by: {level_author}" if level_author else None,
                        color=discord.Color.blue()
                    )
                    
                    if map_id.startswith('!'):
                        embed.url = f"https://beatsaver.com/maps/{map_id[1:]}"
                    
                    embed.add_field(name="ID", value=f"`{map_id}`", inline=True)
                    embed.add_field(name="Difficulty", value=f"{char} {diff}", inline=True)
                    embed.add_field(name="Level", value=str(level), inline=True)
                    embed.add_field(name="Category", value=category.capitalize(), inline=True)
                    
                    if ranked_by:
                        embed.add_field(name="Ranked By", value=f"<@{ranked_by}>", inline=True)
                    
                    if cover_url:
                        embed.set_thumbnail(url=cover_url)
                    
                    embed.set_footer(text=f"Map {i} of {len(pages[page])} • Page {page + 1} of {len(pages)}")
                    embeds.append(embed)
                
                # Update message with new embeds
                await interaction.response.edit_message(embeds=embeds, view=view)
            
            # Add navigation buttons
            if len(pages) > 1:
                prev_button = discord.ui.Button(style=discord.ButtonStyle.primary, emoji="⬅️")
                next_button = discord.ui.Button(style=discord.ButtonStyle.primary, emoji="➡️")
                
                async def prev_callback(interaction: discord.Interaction):
                    nonlocal current_page
                    if current_page > 0:
                        await update_page(interaction, current_page - 1)
                
                async def next_callback(interaction: discord.Interaction):
                    nonlocal current_page
                    if current_page < len(pages) - 1:
                        await update_page(interaction, current_page + 1)
                
                prev_button.callback = prev_callback
                next_button.callback = next_callback
                
                view.add_item(prev_button)
                view.add_item(next_button)
            
            await interaction.followup.send(embeds=embeds, view=view)
            
        except sqlite3.Error as e:
            logger.error(f"Database error in view_all_maps: {e}", exc_info=True)
            await interaction.followup.send("❌ An error occurred while fetching maps from the database.")
        except Exception as e:
            logger.error(f"Error in view_all_maps: {e}", exc_info=True)
            await interaction.followup.send("❌ An unexpected error occurred.")
        finally:
            if 'conn' in locals():
                conn.close()

    @app_commands.command(name="getinfo", description="Get information about a ranked map by ID or name")
    @app_commands.describe(
        query="The map ID or name to search for",
        limit="Maximum number of results to return (1-10, default: 5)"
    )
    async def get_map_info(
        self,
        interaction: discord.Interaction,
        query: str,
        limit: int = 5
    ):
        """
        Get information about a ranked map by its ID or search by name.
        
        Parameters
        ----------
        query: The map ID or name to search for
        limit: Maximum number of results to return (1-10, default: 5)
        """
        await interaction.response.defer(ephemeral=False)
        
        # Validate limit
        limit = max(1, min(10, limit))  # Clamp between 1 and 10
        
        try:
            conn = sqlite3.connect(RANKED_MAPS_DB)
            c = conn.cursor()
            
            # Check if query is a short alphanumeric ID (1-5 chars) or BSR code
            if (1 <= len(query) <= 5 and query.isalnum()) or \
               (query.startswith('!') and 2 <= len(query) <= 6 and query[1:].isalnum()):
                # If it's a raw ID (no ! prefix), add the prefix
                if not query.startswith('!'):
                    search_id = f"!{query.upper()}"  # Add ! prefix and uppercase for consistency
                    # Search for both with and without prefix
                    c.execute("""
                        SELECT id, song_name, song_author, characteristic, difficulty, level, category, 
                               ranked_by, id, cover_url, level_author
                        FROM ranked_maps 
                        WHERE id = ? OR id = ?
                        ORDER BY id = ? DESC
                        LIMIT ?
                    """, (search_id, query, search_id, limit))
                else:
                    # It already has a ! prefix, search as is
                    search_id = query.upper()
                    c.execute("""
                        SELECT id, song_name, song_author, characteristic, difficulty, level, category, 
                               ranked_by, id, cover_url, level_author
                        FROM ranked_maps 
                        WHERE id = ?
                        LIMIT ?
                    """, (search_id, limit))
            else:
                # Search by song name (case-insensitive partial match)
                search_term = f"%{query}%"
                c.execute("""
                    SELECT id, song_name, song_author, characteristic, difficulty, level, category, 
                           ranked_by, id, cover_url, level_author
                    FROM ranked_maps 
                    WHERE LOWER(song_name) LIKE LOWER(?) 
                    ORDER BY 
                        CASE 
                            WHEN LOWER(song_name) = LOWER(?) THEN 0
                            WHEN LOWER(song_name) LIKE LOWER(?) THEN 1
                            ELSE 2
                        END,
                        song_name
                    LIMIT ?
                """, (search_term, query, f"{query}%", limit))
            
            results = c.fetchall()
            
            if not results:
                await interaction.followup.send(
                    f"❌ No ranked maps found matching: `{query}`. "
                    "Try a different search term or check the spelling."
                )
                return
            
            # Create embeds for each result (up to the limit)
            embeds = []
            for idx, (map_id, song_name, song_author, char, diff, level, category, 
                     ranked_by, bsr_code, cover_url, level_author) in enumerate(results, 1):
                
                # Create embed with map info
                title = f"{song_author} - {song_name}" if song_author else song_name
                description = f"Mapped by: {level_author}" if level_author else None
                embed = discord.Embed(
                    title=title,
                    description=description,
                    color=discord.Color.blue(),
                    url=f"https://beatsaver.com/maps/{bsr_code}" if bsr_code else None
                )
                
                # Add fields with map details
                embed.add_field(name="Difficulty", value=f"{char} {diff}", inline=True)
                embed.add_field(name="Level", value=f"{level}", inline=True)
                embed.add_field(name="Category", value=category.capitalize(), inline=True)
                
                if ranked_by:
                    embed.add_field(name="Ranked By", value=f"<@{ranked_by}>", inline=True)
                if bsr_code:
                    embed.add_field(name="BSR", value=f"`{bsr_code}`", inline=True)

                
                # Add footer with result count
                embed.set_footer(text=f"Result {idx} of {len(results)}")
                
                # Add cover image if available
                if cover_url:
                    embed.set_thumbnail(url=cover_url)
                
                # Try to get cover image from BeatSaver if available
                if bsr_code:
                    try:
                        async with aiohttp.ClientSession() as session:
                            async with session.get(f"https://api.beatsaver.com/maps/id/{bsr_code}") as resp:
                                if resp.status == 200:
                                    map_data = await resp.json()
                                    if 'coverURL' in map_data:
                                        embed.set_thumbnail(url=map_data['coverURL'])
                    except Exception as e:
                        logger.warning(f"Couldn't fetch cover for {bsr_code}: {e}")
                
                embeds.append(embed)
            
            # Send the embeds with pagination if more than one result
            if len(embeds) == 1:
                await interaction.followup.send(embed=embeds[0])
            else:
                # If multiple results, send the first one with navigation buttons
                view = PaginatedView(embeds)
                await interaction.followup.send(embed=embeds[0], view=view)
            
        except sqlite3.Error as e:
            logger.error(f"Database error in get_map_info: {e}", exc_info=True)
            await interaction.followup.send("❌ An error occurred while searching the database. Please try again later.")
        except Exception as e:
            logger.error(f"Error in get_map_info: {e}", exc_info=True)
            await interaction.followup.send("❌ An unexpected error occurred. Please try again later.")
        finally:
            conn.close()

    # Role IDs for each level (1-32)
    LEVEL_ROLE_IDS = {
        1: 1373413894101536769, 2: 1373413939676713020, 3: 1373413953627230298,
        4: 1373413967744991242, 5: 1373413984958550036, 6: 1373413998405484604,
        7: 1373414011823063081, 8: 1373414025085325403, 9: 1373414041585848380,
        10: 1373414057293648053, 11: 1373414071684169748, 12: 1373414090432577556,
        13: 1373414106379583688, 14: 1373414120539295835, 15: 1373414140714160230,
        16: 1373414156228624527, 17: 1373414174951997561, 18: 1373414190835961986,
        19: 1373414203578388612, 20: 1373414217637564590, 21: 1373414239913377853,
        22: 1373414252240437418, 23: 1373414269084766310, 24: 1373414283605708880,
        25: 1373414307664101397, 26: 1373414322369331331, 27: 1373414339578691754,
        28: 1373414356762759351, 29: 1373414376354218076, 30: 1373414395815657544,
        31: 1373414413687587079, 32: 1373414438547362002
    }
    RANKING_TEAM_ROLE_ID = 1373413539196305500  # Ranking Team role ID

    @app_commands.command(name="scan", description="Scan your BeatLeader scores and update passed maps")
    async def scan_scores(self, interaction: discord.Interaction):
        """Scan a user's BeatLeader scores and check against ranked maps."""
        await interaction.response.defer(ephemeral=False)  # Changed to False to make results visible to everyone

        discord_id = str(interaction.user.id)
        logger.info(f"Starting scan for user {discord_id}")

        try:
            # Get BeatLeader ID from approved users DB
            try:
                conn = sqlite3.connect(APPROVED_USERS_DB)
                c = conn.cursor()
                c.execute("SELECT beatleader_id, username FROM approved_users WHERE discord_id = ?", (discord_id,))
                row = c.fetchone()
            except sqlite3.Error as e:
                logger.error(f"Database error fetching user: {e}")
                await interaction.followup.send("❌ Database error. Please try again later.")
                return
            finally:
                conn.close()

            if not row:
                await interaction.followup.send("❌ You must be linked to use this command.")
                return

            beatleader_id, discord_username = row
            logger.info(f"Found BeatLeader ID {beatleader_id} for user {discord_username}")

            # Fetch up to 10 pages of recent scores (1000 scores max)
            all_scores = []
            page = 1
            max_pages = 1000
            
            try:
                while page <= max_pages:
                    url = f"https://api.beatleader.com/player/{beatleader_id}/scores?sortBy=date&order=desc&page={page}&count=100"
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url) as resp:
                            if resp.status != 200:
                                if page == 1:  # Only fail completely if first page fails
                                    await interaction.followup.send("❌ Failed to fetch scores from BeatLeader. Please try again later.")
                                    return
                                break
                            
                            data = await resp.json()
                            page_scores = data.get("data", [])
                            all_scores.extend(page_scores)
                            
                            if not page_scores:  # Stop if no more scores
                                break
                                
                            page += 1
                            await asyncio.sleep(0.5)  # Rate limiting
                
                if not all_scores:
                    await interaction.followup.send("❌ No scores found for this player.")
                    return
                    
            except Exception as e:
                logger.error(f"Error fetching scores: {e}", exc_info=True)
                await interaction.followup.send("❌ Error fetching your scores. Please try again later.")
                return

            # Get all ranked maps once
            try:
                conn = sqlite3.connect(RANKED_MAPS_DB)
                c = conn.cursor()
                c.execute("""
                    SELECT LOWER(song_name) as song_name_lower, characteristic, difficulty, level, song_name, song_hash 
                    FROM ranked_maps
                """)
                ranked_maps = c.fetchall()
            except sqlite3.Error as e:
                logger.error(f"Database error fetching ranked maps: {e}")
                await interaction.followup.send("❌ Database error. Please try again later.")
                return
            finally:
                conn.close()

            if not ranked_maps:
                await interaction.followup.send("❌ No ranked maps found in the database.")
                return

            # Process scores and save to database
            passed_levels = set()
            total_points = 0
            detailed_passes = []
            processed_songs = set()  # To avoid duplicate songs
            disallowed_modifier_passes = []  # Track passes with disallowed modifiers
            
            # Get existing passes from database
            existing_passes = {}
            try:
                conn = sqlite3.connect(USER_PASSES_DB)
                c = conn.cursor()
                c.execute(
                    'SELECT song_name, characteristic, difficulty, accuracy FROM user_passes WHERE discord_id = ?', 
                    (discord_id,)
                )
                existing_passes = {
                    f"{row[0].lower()}:{row[1]}:{row[2].replace('Expert+', 'ExpertPlus')}": row[3] 
                    for row in c.fetchall()
                }
            except sqlite3.Error as e:
                logger.error(f"Error fetching existing passes: {e}")
                await interaction.followup.send("❌ Error checking existing passes. Please try again.")
                return
            finally:
                conn.close()
            
            new_passes = []
            updated_passes = []
            
            for entry in all_scores:
                leaderboard = entry.get("leaderboard", {})
                song = leaderboard.get("song", {})
                diff = leaderboard.get("difficulty", {})
                
                acc = entry.get("accuracy", 0)
                if acc <= 0:  # Skip failed or invalid scores
                    continue
                    
                song_name = song.get("name", "").strip()
                song_name_lower = song_name.lower()
                difficulty = diff.get("difficultyName", "").replace("ExpertPlus", "Expert+")
                characteristic = diff.get("modeName", "")
                
                # Get the song hash from the score if available
                score_hash = song.get("hash", "").lower()
                
                # Create a unique key using hash if available, otherwise use song name
                song_key = f"{score_hash or song_name_lower}:{characteristic}:{difficulty}"
                if song_key in processed_songs:
                    continue
                    
                # Check for disallowed modifiers
                modifiers = [m for m in entry.get('modifiers', '').split(',') if m]
                disallowed_modifiers = {'NO', 'NB', 'NF', 'SS', 'NA', 'OP'}
                used_disallowed_mods = [m for m in modifiers if m in disallowed_modifiers]
                has_disallowed_modifier = len(used_disallowed_mods) > 0
                
                # Find matching ranked map
                logger.info(f"Processing score - Song: '{song_name_lower}', Char: '{characteristic}', Diff: '{difficulty}', Hash: '{score_hash}'")
                
                # Find matching ranked map
                match_found = False
                for r_song_lower, r_char, r_diff, level, r_song_original, r_hash in ranked_maps:
                    # Skip if no match on characteristic or difficulty
                    if characteristic != r_char or difficulty != r_diff:
                        continue
                        
                    # Check for hash match first (most reliable)
                    if score_hash and r_hash and score_hash == r_hash.lower():
                        logger.info(f"Hash match found - Song: '{r_song_original}', Hash: {r_hash}")
                        match_found = True
                        break
                        
                    # Fall back to name matching if no hash available
                    if not score_hash and r_song_lower == song_name_lower:
                        logger.info(f"Name match found - Song: '{r_song_original}'") 
                        match_found = True
                        break
                
                if not match_found:
                    logger.info(f"No matching ranked map found for: {song_name_lower} ({characteristic} {difficulty})")
                    continue
                    
                logger.info(f"MATCH FOUND! - Song: '{r_song_original}', Char: '{r_char}', Diff: '{r_diff}', Level: {level}, Hash: {r_hash}")
                
                # Skip if this is an unranked map (level 100)
                if level == 100:
                    # Check if this is a new pass for an unranked map
                    db_song_key = f"{song_name_lower}:{characteristic}:{difficulty.replace('Expert+', 'ExpertPlus')}"
                    existing_acc = existing_passes.get(db_song_key, 0)
                    
                    if acc * 100 > existing_acc and not has_disallowed_modifier:
                        # Notify ranking team about the unranked pass
                        ranking_team_role = interaction.guild.get_role(self.RANKING_TEAM_ROLE_ID)
                        if ranking_team_role:
                            await interaction.followup.send(
                                f"🚨 {ranking_team_role.mention} - Unranked map passed!\n"
                                f"**Map:** {r_song_original} ({characteristic} {difficulty})\n"
                                f"**Player:** {interaction.user.mention}\n"
                                f"**Accuracy:** {acc*100:.2f}%\n"
                                f"Please update this map's level using `/updatemap` if it should be ranked.",
                                ephemeral=False
                            )
                        
                        # Add to database to track that we've notified about this pass
                        pass_data = (
                            discord_id,
                            beatleader_id,
                            r_song_original,
                            characteristic,
                            difficulty,
                            level,  # This will be 100 for unranked
                            acc * 100,
                            0  # 0 points for unranked maps
                        )
                        
                        if existing_acc == 0:
                            new_passes.append(pass_data)
                        else:
                            updated_passes.append(pass_data)
                        continue
                    
# For ranked maps (level 1-32)
                    if has_disallowed_modifier:
                        # Save disallowed pass to database
                        try:
                            db_song_key = f"{song_name_lower}:{characteristic}:{difficulty.replace('Expert+', 'ExpertPlus')}"
                            existing_acc = 0
                            
                            # Check if we have a better score for this map+modifiers already
                            conn = sqlite3.connect(USER_PASSES_DB)
                            c = conn.cursor()
                            c.execute("""
                                SELECT accuracy FROM disallowed_passes 
                                WHERE discord_id = ? 
                                AND song_name = ? 
                                AND characteristic = ? 
                                AND difficulty = ?
                                AND modifiers = ?
                            """, (discord_id, r_song_original, characteristic, difficulty, ','.join(sorted(used_disallowed_mods))))
                            
                            existing = c.fetchone()
                            if existing:
                                existing_acc = existing[0]
                            
                            # Only save if this is a better score
                            if acc * 100 > existing_acc:
                                c.execute("""
                                    INSERT OR REPLACE INTO disallowed_passes 
                                    (discord_id, beatleader_id, song_name, characteristic, 
                                     difficulty, level, accuracy, modifiers)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    discord_id,
                                    beatleader_id,
                                    r_song_original,
                                    characteristic,
                                    difficulty,
                                    level,
                                    acc * 100,
                                    ','.join(sorted(used_disallowed_mods))
                                ))
                                conn.commit()
                                
                                # Add to our list for the notification
                                disallowed_modifier_passes.append((
                                    r_song_original,
                                    level,
                                    difficulty,
                                    used_disallowed_mods,
                                    acc * 100
                                ))
                                
                                logger.info(f"Saved disallowed pass for {r_song_original} with mods: {used_disallowed_mods}")
                            
                        except Exception as e:
                            logger.error(f"Error saving disallowed pass: {e}", exc_info=True)
                        finally:
                            conn.close()
                        
                        continue
                            
                        points = round(level * acc * 21.3)
                        total_points += points
                        passed_levels.add(level)
                        processed_songs.add(song_key)
                        
                        # Check if this is a new pass or an improvement
                        db_song_key = f"{song_name_lower}:{characteristic}:{difficulty.replace('Expert+', 'ExpertPlus')}"
                        existing_acc = existing_passes.get(db_song_key, 0)
                        
                        if acc * 100 > existing_acc:
                            detailed_passes.append((r_song_original, level, acc * 100, points, acc > existing_acc/100))
                            
                            # Prepare for database update
                            pass_data = (
                                discord_id,
                                beatleader_id,
                                r_song_original,  # Use the original song name from ranked maps
                                characteristic,
                                difficulty,
                                level,
                                acc * 100,  # Store as percentage
                                points
                            )
                            
                            if existing_acc == 0:
                                new_passes.append(pass_data)
                            else:
                                updated_passes.append(pass_data)
                        
                        break

            # Save new and updated passes to database
            try:
                conn = sqlite3.connect(USER_PASSES_DB)
                c = conn.cursor()
                
                # Insert new passes
                if new_passes:
                    c.executemany('''
                        INSERT INTO user_passes 
                        (discord_id, beatleader_id, song_name, characteristic, difficulty, level, accuracy, points)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', new_passes)
                
                # Update existing passes with better accuracy
                if updated_passes:
                    c.executemany('''
                        INSERT INTO user_passes 
                        (discord_id, beatleader_id, song_name, characteristic, difficulty, level, accuracy, points)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(discord_id, song_name, characteristic, difficulty) 
                        DO UPDATE SET 
                            accuracy = excluded.accuracy,
                            points = excluded.points,
                            passed_at = CURRENT_TIMESTAMP
                        WHERE excluded.accuracy > user_passes.accuracy
                    ''', updated_passes)
                
                conn.commit()
                logger.info(f"Saved {len(new_passes)} new passes and updated {len(updated_passes)} passes for user {discord_id}")
                
            except sqlite3.Error as e:
                logger.error(f"Error saving user passes: {e}", exc_info=True)
                # Continue execution even if saving fails
            finally:
                conn.close()
            
            # Sort by level descending, then by accuracy
            detailed_passes.sort(key=lambda x: (-x[1], -x[2]))
            
            # Get total pass count from database
            try:
                conn = sqlite3.connect(USER_PASSES_DB)
                c = conn.cursor()
                c.execute('SELECT COUNT(*) FROM user_passes WHERE discord_id = ?', (discord_id,))
                total_passes = c.fetchone()[0]
            except sqlite3.Error as e:
                logger.error(f"Error getting total pass count: {e}")
                total_passes = len(detailed_passes)
            finally:
                conn.close()

            # Get user's previous highest level
            previous_level = 0
            try:
                member = interaction.guild.get_member(int(discord_id))
                if member:
                    # Find the highest level role the user currently has
                    for level, role_id in sorted(self.LEVEL_ROLE_IDS.items(), reverse=True):
                        role = interaction.guild.get_role(role_id)
                        if role and role in member.roles:
                            previous_level = level
                            break
            except Exception as e:
                logger.error(f"Error checking previous roles: {e}", exc_info=True)
            
            # Determine new level (highest level passed, no consecutive requirement)
            current_level = max(passed_levels) if passed_levels else 0
            
            # Update user roles based on their level
            level_up = False
            try:
                if member:
                    # Remove all level roles first
                    for role_id in self.LEVEL_ROLE_IDS.values():
                        role = interaction.guild.get_role(role_id)
                        if role and role in member.roles:
                            await member.remove_roles(role)
                    
                    # Add the highest level role the user qualifies for
                    if current_level > 0:
                        role_id = self.LEVEL_ROLE_IDS[current_level]
                        role = interaction.guild.get_role(role_id)
                        if role:
                            await member.add_roles(role)
                            logger.info(f"Added level {current_level} role to {discord_username}")
                            # Check if this is a level up
                            if current_level > previous_level:
                                level_up = True
            except Exception as e:
                logger.error(f"Error updating roles: {e}", exc_info=True)

            # Create embeds
            embeds = []
            main_embed = discord.Embed(
                title=f"🔍 Scan Results for {discord_username}",
                color=discord.Color.green()
            )
            embeds.append(main_embed)
            
            # Create a separate embed for disallowed modifier passes if any
            if disallowed_modifier_passes:
                try:
                    # Group passes by song to show all mods together
                    passes_by_song = {}
                    for song, level, diff, mods, acc in disallowed_modifier_passes:
                        key = (song, level, diff)
                        if key not in passes_by_song:
                            passes_by_song[key] = {'mods': set(), 'acc': acc}
                        passes_by_song[key]['mods'].update(mods)
                        # Keep the highest accuracy for this song
                        if acc > passes_by_song[key]['acc']:
                            passes_by_song[key]['acc'] = acc
                    
                    # Sort by accuracy descending
                    sorted_passes = sorted(
                        [(song, level, diff, data['mods'], data['acc']) 
                         for (song, level, diff), data in passes_by_song.items()],
                        key=lambda x: x[4],  # Sort by accuracy
                        reverse=True
                    )
                    
                    # Create the embed
                    disallowed_embed = discord.Embed(
                        title="⚠️ Passes with Disallowed Modifiers",
                        description=(
                            "The following maps were passed with prohibited modifiers and did not count:\n"
                            "*(NO, NB, NF, SS, NA, OP modifiers are not allowed for ranked play)*"
                        ),
                        color=discord.Color.orange()
                    )
                    
                    # Add passes to the embed
                    pass_list = []
                    for song, level, diff, mods, acc in sorted_passes:
                        mods_str = ', '.join(sorted(mods))
                        pass_list.append(
                            f"`{acc:5.2f}%` `{diff}` {song[:30]}{'...' if len(song) > 30 else ''} ({mods_str})"
                        )
                    
                    # Split into chunks to avoid hitting field value length limit
                    chunk_size = 10
                    for i in range(0, len(pass_list), chunk_size):
                        chunk = pass_list[i:i + chunk_size]
                        disallowed_embed.add_field(
                            name="Passes" if i == 0 else "\u200b",
                            value="\n".join(chunk),
                            inline=False
                        )
                    
                    embeds.append(disallowed_embed)
                    
                except Exception as e:
                    logger.error(f"Error creating disallowed passes embed: {e}", exc_info=True)
            
            # Add stats about new/updated passes
            stats = [
                f"**Total Points:** {total_points:,}",
                f"**Current Level:** {current_level}",
                f"**Total Maps Passed:** {total_passes}"
            ]
            
            if level_up:
                stats.append(f"🎉 **Level Up!** Reached Level {current_level}!")
            if new_passes:
                stats.append(f"**New Passes:** {len(new_passes)}")
            if updated_passes:
                stats.append(f"**Improved Scores:** {len(updated_passes)}")
            
            main_embed.add_field(
                name="Summary",
                value="\n".join(stats),
                inline=False
            )
            
            # Add note about disallowed modifiers if any were found
            if disallowed_modifier_passes:
                main_embed.add_field(
                    name="⚠️ Note",
                    value=(
                        f"{len(disallowed_modifier_passes)} map(s) were passed with disallowed modifiers "
                        "and didn't count. See below for details."
                    ),
                    inline=False
                )

            # Add top passes
            if detailed_passes:
                top_passes = detailed_passes[:10]  # Show top 10
                shown = "\n".join([
                    f"`Level {level:2d}` {name[:30]}{'...' if len(name) > 30 else ''} | "
                    f"{acc:.1f}% → {pts:4d} pts"
                    f"{' 🆕' if is_new else ''}"
                    for name, level, acc, pts, is_new in top_passes
                ])
                
                main_embed.add_field(
                    name=f"Top {len(top_passes)} Best Passes" if len(detailed_passes) > 10 else "Best Passes",
                    value=shown,
                    inline=False
                )
                
                if len(detailed_passes) > 10:
                    main_embed.set_footer(text=f"Showing 10 of {len(detailed_passes)} passed maps")

            # Send all embeds
            await interaction.followup.send(embeds=embeds)
            logger.info(f"Successfully completed scan for user {discord_username}")
            
        except Exception as e:
            logger.error(f"Unexpected error in scan_scores: {e}", exc_info=True)
            try:
                await interaction.followup.send("❌ An unexpected error occurred. Please try again later.")
            except:
                pass

    @app_commands.command(name="rankmap", description="Rank a Beat Saber map")
    @app_commands.describe(
        bsr_code="The BeatSaver ID or URL of the map",
        category="The category of the map",
        level="The difficulty level (1-32 for ranked, 100 for unranked)",
        characteristic="The map characteristic",
        difficulty="The difficulty name",
        additional_info="Optional additional information about the map ranking"
    )
    @app_commands.choices(
        category=[app_commands.Choice(name=c, value=c) for c in ["tech", "jumps", "streams", "shitpost", "vibro"]],
        characteristic=[app_commands.Choice(name=c, value=c) for c in ["Standard", "Lawless", "OneSaber"]],
        difficulty=[app_commands.Choice(name=d, value=d) for d in ["Easy", "Normal", "Hard", "Expert", "Expert+"]]
    )
    @app_commands.checks.has_role(RANKING_TEAM_ROLE_ID)
    async def rankmap(
        self,
        interaction: discord.Interaction,
        bsr_code: str,
        category: Literal["tech", "jumps", "streams", "shitpost", "vibro"],
        level: int,
        characteristic: Literal["Standard", "Lawless", "OneSaber"],
        difficulty: Literal["Easy", "Normal", "Hard", "Expert", "Expert+"],
        additional_info: Optional[str] = None
    ):
        """Rank a Beat Saber map.
        
        Parameters
        ----------
        additional_info: Optional additional information about the map ranking
        """
        if not await self.check_guild_permission(interaction):
            return
            
        logger.info(f"Rankmap command invoked by {interaction.user} (ID: {interaction.user.id}) with BSR: {bsr_code}")
        logger.debug(f"Command params - Category: {category}, Level: {level}, Characteristic: {characteristic}, Difficulty: {difficulty}")
        
        # Validate level (1-32 for ranked, or exactly 100 for unranked)
        if level != 100 and not (1 <= level <= 32):
            error_msg = f"Invalid level: {level}. Must be between 1-32 for ranked maps, or exactly 100 for unranked maps."
            logger.warning(error_msg)
            await interaction.response.send_message(error_msg, ephemeral=True)
            return

        # Fetch map data
        logger.debug("Fetching map data from BeatSaver API...")
        await interaction.response.defer()  # Remove ephemeral=True to make response visible to everyone
        map_data = await self.get_map_data(bsr_code)
        
        if "error" in map_data:
            error_msg = f"Error fetching map data: {map_data['error']}"
            logger.error(error_msg)
            await interaction.followup.send(f"Error: {map_data['error']}")
            return

        # Find the specific difficulty version
        version = None
        for v in map_data['versions']:
            # Store the song hash from the first version (should be the same for all versions)
            if 'hash' in v and 'song_hash' not in map_data:
                map_data['song_hash'] = v['hash']
            for d in v['diffs']:
                # Normalize difficulty names (handle Expert+ vs ExpertPlus)
                diff_name = d['difficulty'].replace('Plus', '+').lower()
                if (d['characteristic'].lower() == characteristic.lower() and 
                    diff_name == difficulty.lower()):
                    version = {
                        **d,
                        'coverURL': v['coverURL'],
                        'downloadURL': v['downloadURL']
                    }
                    logger.debug(f"Found matching version: {d['characteristic']} {d['difficulty']}")
                    break
            if version:
                break

        if not version:
            error_msg = f"Could not find {characteristic} {difficulty} version of map {bsr_code}"
            logger.warning(error_msg)
            await interaction.followup.send(
                f"Could not find {characteristic} {difficulty} version of this map."
            )
            return

        ranked_map = {
            'id': map_data['id'],
            'name': map_data['name'],
            'songName': map_data['metadata']['songName'],
            'songAuthorName': map_data['metadata']['songAuthorName'],
            'levelAuthorName': map_data['metadata'].get('levelAuthorName', 'Unknown'),
            'bpm': map_data['metadata']['bpm'],
            'duration': map_data['metadata']['duration'],
            'coverURL': version['coverURL'],
            'downloadURL': version['downloadURL'],
            'characteristic': characteristic,
            'difficulty': difficulty,
            'nps': version.get('nps', 0),
            'notes': version.get('notes', 0),
            'njs': version.get('njs', 0),
            'category': category,
            'level': level,
            'ranked_by': interaction.user.id,
            'ranked_at': datetime.utcnow().isoformat()
        }

        # Save to database
        logger.debug("Attempting to save map to database...")
        if self.save_ranked_map(ranked_map):
            logger.info(f"Successfully ranked map: {ranked_map['name']} (ID: {ranked_map['id']})")
            
            # Send notification to ranked-maps channel
            try:
                ranked_channel = interaction.guild.get_channel(1385101828416602213)  # ranked-maps channel ID
                if ranked_channel:
                    # Create public embed
                    public_embed = discord.Embed(
                        title=f"🎉 New Ranked Map: {ranked_map['name']}",
                        color=discord.Color.blue()
                    )
                    
                    # Add map cover as thumbnail if available
                    if 'coverURL' in version:
                        public_embed.set_thumbnail(url=version['coverURL'])
                    
                    # Add main fields
                    public_embed.add_field(name="Difficulty", value=difficulty, inline=True)
                    public_embed.add_field(name="Level", value=str(level), inline=True)
                    public_embed.add_field(name="Category", value=category.capitalize(), inline=True)
                    
                    # Add additional info if provided
                    if additional_info:
                        public_embed.add_field(name="Notes", value=additional_info, inline=False)
                    
                    # Add map link
                    map_url = f"https://beatsaver.com/maps/{bsr_code}"
                    public_embed.add_field(name="Map Link", value=map_url, inline=False)
                    
                    # Add footer with who ranked it (username and ID)
                    public_embed.set_footer(text=f"Ranked by {interaction.user.name} ({interaction.user.id})")
                    
                    # Send the embed
                    await ranked_channel.send(embed=public_embed)
                    
            except Exception as e:
                logger.error(f"Failed to send notification to ranked-maps channel: {e}", exc_info=True)
            
            # Create embed
            embed = discord.Embed(
                title=f"✅ Map Ranked Successfully",
                description=f"**{map_data['name']}** has been ranked!",
                color=discord.Color.green()
            )
            
            # Add map details
            minutes, seconds = divmod(ranked_map['duration'], 60)
            embed.add_field(name="Song", value=ranked_map['songName'], inline=True)
            embed.add_field(name="Artist", value=ranked_map['songAuthorName'], inline=True)
            embed.add_field(name="Mapper", value=ranked_map['levelAuthorName'], inline=True)
            embed.add_field(name="Duration", value=f"{int(minutes)}:{int(seconds):02d}", inline=True)
            embed.add_field(name="BPM", value=str(ranked_map['bpm']), inline=True)
            embed.add_field(name="NPS", value=f"{ranked_map['nps']:.2f}", inline=True)
            embed.add_field(name="Notes", value=str(ranked_map['notes']), inline=True)
            embed.add_field(name="NJS", value=str(ranked_map['njs']), inline=True)
            embed.add_field(name="Category", value=ranked_map['category'].title(), inline=True)
            embed.add_field(name="Level", value=f"Level {ranked_map['level']}", inline=True)
            embed.add_field(name="Characteristic", value=ranked_map['characteristic'], inline=True)
            embed.add_field(name="Difficulty", value=ranked_map['difficulty'], inline=True)
            
            # Set thumbnail
            if ranked_map['coverURL']:
                embed.set_thumbnail(url=ranked_map['coverURL'])
            
            # Add footer with BSR code
            embed.set_footer(text=f"BSR: {bsr_code} • Ranked by {interaction.user.display_name}")
            
            logger.debug("Sending success response to Discord...")
            await interaction.followup.send(embed=embed)
            logger.info("Success response sent to Discord")
        else:
            error_msg = f"Failed to save map data for BSR: {bsr_code}"
            logger.error(error_msg)
            await interaction.followup.send("Failed to save map data. Please try again.")

    @rankmap.error
    async def rankmap_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        """Handle errors for the rankmap command."""
        error_msg = str(error)
        if isinstance(error, app_commands.MissingRole):
            error_msg = f"Unauthorized access attempt by {interaction.user} (ID: {interaction.user.id}): {error}"
            logger.warning(error_msg)
            await interaction.response.send_message(
                "❌ You don't have permission to use this command. Only Ranking Team members can rank maps.",
                ephemeral=True
            )
        else:
            error_msg = f"Error in rankmap command: {error}"
            logger.error(error_msg, exc_info=True)
            await interaction.response.send_message(
                f"An error occurred: {str(error)}",
                ephemeral=True
            )
            
    async def get_map_by_bsr(self, bsr_code: str, max_retries: int = 3) -> Optional[dict]:
        """Get a ranked map by its BSR code with retry logic.
        
        Args:
            bsr_code: The BeatSaver ID of the map to retrieve
            max_retries: Maximum number of retry attempts on database lock
            
        Returns:
            dict: Map data if found, None otherwise
        """
        attempt = 0
        while attempt < max_retries:
            conn = None
            try:
                conn = sqlite3.connect(RANKED_MAPS_DB, timeout=10.0)  # 10 second timeout
                conn.execute('PRAGMA journal_mode=WAL')  # Better concurrency
                c = conn.cursor()
                c.execute('BEGIN IMMEDIATE')  # Start a transaction immediately
                c.execute('SELECT * FROM ranked_maps WHERE id = ?', (bsr_code,))
                row = c.fetchone()
                conn.commit()  # Commit the read transaction
                
                if not row:
                    return None
                    
                # Convert row to dict
                columns = [desc[0] for desc in c.description]
                return dict(zip(columns, row))
                
            except sqlite3.OperationalError as e:
                if conn:
                    conn.rollback()  # Rollback any failed transaction
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    attempt += 1
                    wait_time = 0.1 * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Database locked, retrying in {wait_time:.2f}s... (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                logger.error(f"Error getting map by BSR: {e}", exc_info=True)
                return None
            except sqlite3.Error as e:
                if conn:
                    conn.rollback()
                logger.error(f"Database error getting map by BSR: {e}", exc_info=True)
                return None
            finally:
                if conn:
                    conn.close()
        return None
    
    async def update_ranked_map(self, bsr_code: str, updates: dict, max_retries: int = 3) -> bool:
        """Update a ranked map's information with retry logic.
        
        Args:
            bsr_code: The BeatSaver ID of the map to update
            updates: Dictionary of field: value pairs to update
            max_retries: Maximum number of retry attempts on database lock
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        attempt = 0
        while attempt < max_retries:
            conn = None
            try:
                conn = sqlite3.connect(RANKED_MAPS_DB, timeout=10.0)  # 10 second timeout
                conn.execute('PRAGMA journal_mode=WAL')  # Better concurrency
                c = conn.cursor()
                
                # Start an immediate transaction
                c.execute('BEGIN IMMEDIATE')
                
                # Build the update query dynamically based on provided fields
                set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
                values = list(updates.values())
                values.append(bsr_code)  # For the WHERE clause
                
                query = f"""
                    UPDATE ranked_maps 
                    SET {set_clause}
                    WHERE id = ?
                """
                
                c.execute(query, values)
                conn.commit()  # Commit the transaction
                return c.rowcount > 0
                
            except sqlite3.OperationalError as e:
                if conn:
                    conn.rollback()  # Rollback on error
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    attempt += 1
                    wait_time = 0.1 * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Database locked, retrying in {wait_time:.2f}s... (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                logger.error(f"Error updating ranked map: {e}", exc_info=True)
                return False
            except sqlite3.Error as e:
                if conn:
                    conn.rollback()
                logger.error(f"Database error updating ranked map: {e}", exc_info=True)
                return False
            finally:
                if conn:
                    conn.close()
        return False
    
    @app_commands.command(name="updatemap", description="Update a ranked map's information")
    @app_commands.checks.has_role(RANKING_TEAM_ROLE_ID)
    async def update_map(
        self,
        interaction: discord.Interaction,
        bsr_code: str,
        category: Optional[Literal["tech", "jumps", "streams", "shitpost", "vibro"]] = None,
        level: Optional[int] = None,
        characteristic: Optional[Literal["Standard", "Lawless", "OneSaber"]] = None,
        difficulty: Optional[Literal["Easy", "Normal", "Hard", "Expert", "Expert+"]] = None,
        additional_info: Optional[str] = None,
        song_hash: Optional[str] = None
    ):
        """Update a ranked map's information.
        
        Parameters
        ----------
        bsr_code: The BeatSaver ID of the map to update
        category: New category for the map
        level: New level (1-32)
        characteristic: New characteristic (Standard/Lawless/OneSaber)
        difficulty: New difficulty
        additional_info: New additional information
        
        Note: Only one field can be updated at a time.
        """
        if not await self.check_guild_permission(interaction):
            return
            
        await interaction.response.defer()
        
        # Count how many fields are being updated
        update_fields = [f for f in [category, level, characteristic, difficulty, additional_info, song_hash] if f is not None]
        
        # Check if exactly one field is being updated
        if len(update_fields) != 1:
            await interaction.followup.send(
                "❌ Please specify exactly one field to update. "
                "You can only update one field at a time."
            )
            return
            
        # Check if level is valid (1-32) if provided
        if level is not None and not (1 <= level <= 32):
            await interaction.followup.send("❌ Level must be between 1 and 32.")
            return
            
        # Get the current map data
        current_map = await self.get_map_by_bsr(bsr_code)
        if not current_map:
            await interaction.followup.send(f"❌ Could not find a ranked map with BSR: {bsr_code}")
            return
            
        # Prepare updates
        updates = {}
        updated_fields = []
        
        if category is not None:
            if category == current_map['category']:
                await interaction.followup.send(f"❌ The category is already set to '{category}'")
                return
            updates['category'] = category
            updated_fields.append(f"**Category**: {current_map['category']} → {category}")
            
        elif level is not None:
            if level == current_map['level']:
                await interaction.followup.send(f"❌ The level is already set to {level}")
                return
            updates['level'] = level
            updated_fields.append(f"**Level**: {current_map['level']} → {level}")
            
        elif characteristic is not None:
            if characteristic == current_map['characteristic']:
                await interaction.followup.send(f"❌ The characteristic is already set to '{characteristic}'")
                return
            updates['characteristic'] = characteristic
            updated_fields.append(f"**Characteristic**: {current_map['characteristic']} → {characteristic}")
            
        elif difficulty is not None:
            if difficulty == current_map['difficulty']:
                await interaction.followup.send(f"❌ The difficulty is already set to '{difficulty}'")
                return
            updates['difficulty'] = difficulty
            updated_fields.append(f"**Difficulty**: {current_map['difficulty']} → {difficulty}")
            
        elif additional_info is not None:
            if additional_info == current_map.get('additional_info'):
                await interaction.followup.send("❌ The additional info is already set to the provided value")
                return
            updates['additional_info'] = additional_info
            updated_fields.append("**Additional Info**: Updated")
            
        elif song_hash is not None:
            if song_hash == current_map.get('song_hash'):
                await interaction.followup.send("❌ The song hash is already set to the provided value")
                return
            updates['song_hash'] = song_hash
            updated_fields.append("**Song Hash**: Updated")
        
        # If no actual changes (all provided values are the same as current)
        if not updates:
            await interaction.followup.send("❌ No changes were made. The provided values are the same as the current ones.")
            return
            
        # Update the map
        if await self.update_ranked_map(bsr_code, updates):
            # Send update to ranked-maps channel
            try:
                ranked_channel = interaction.guild.get_channel(1385101828416602213)  # ranked-maps channel ID
                if ranked_channel:
                    # Create update embed
                    update_embed = discord.Embed(
                        title=f"🔄 Map Updated: {current_map['name']}",
                        color=discord.Color.orange()
                    )
                    
                    # Add map cover if available
                    if current_map.get('coverURL'):
                        update_embed.set_thumbnail(url=current_map['coverURL'])
                    
                    # Add update details
                    update_embed.description = "\n".join(updated_fields)
                    
                    # Add map link
                    map_url = f"https://beatsaver.com/maps/{bsr_code}"
                    update_embed.add_field(name="Map Link", value=map_url, inline=False)
                    
                    # Add footer with who updated it
                    update_embed.set_footer(text=f"Updated by {interaction.user.name} ({interaction.user.id})")
                    
                    await ranked_channel.send(embed=update_embed)
                    
            except Exception as e:
                logger.error(f"Failed to send update notification: {e}", exc_info=True)
            
            await interaction.followup.send("✅ Map updated successfully!")
        else:
            await interaction.followup.send("❌ Failed to update the map. Please try again.")

    def remove_ranked_map(self, bsr_code: str) -> tuple:
        """Remove a ranked map from the database."""
        conn = None
        try:
            conn = sqlite3.connect(RANKED_MAPS_DB)
            c = conn.cursor()
            
            # First get the map data for the notification
            c.execute('SELECT name, coverURL, category, level, characteristic, difficulty, additional_info FROM ranked_maps WHERE id = ?', (bsr_code,))
            map_data = c.fetchone()
            
            if not map_data:
                return False, None, None, None, None, None, None, None
                
            # Delete the map
            c.execute('DELETE FROM ranked_maps WHERE id = ?', (bsr_code,))
            conn.commit()
            
            return True, map_data[0], map_data[1], map_data[2], map_data[3], map_data[4], map_data[5], map_data[6]
            
        except sqlite3.Error as e:
            logger.error(f"Error removing ranked map: {e}", exc_info=True)
            return False, None, None, None, None, None, None, None
        finally:
            if conn:
                conn.close()
    
    @app_commands.command(name="removemap", description="Remove a ranked map")
    @app_commands.checks.has_role(RANKING_TEAM_ROLE_ID)
    async def remove_map(
        self,
        interaction: discord.Interaction,
        bsr_code: str,
        reason: Optional[str] = None
    ):
        """Remove a ranked map from the database.
        
        Parameters
        ----------
        bsr_code: The BeatSaver ID of the map to remove
        reason: Optional reason for removal
        """
        if not await self.check_guild_permission(interaction):
            return
            
        await interaction.response.defer(ephemeral=True)
        
        # Remove the map from the database
        success, map_name, cover_url, category, level, char, diff, add_info = self.remove_ranked_map(bsr_code)
        
        if not success or not map_name:
            await interaction.followup.send(
                f"❌ Could not find or remove the map with BSR: {bsr_code}",
                ephemeral=True
            )
            return
            
        # Send notification to ranked-maps channel
        try:
            ranked_channel = interaction.guild.get_channel(1385101828416602213)  # ranked-maps channel ID
            if ranked_channel:
                # Create removal embed
                remove_embed = discord.Embed(
                    title=f"🗑️ Map Removed: {map_name}",
                    description=f"This map has been removed from the ranked pool.",
                    color=discord.Color.red()
                )
                
                # Add map details
                details = [
                    f"**Category**: {category}",
                    f"**Level**: {level}",
                    f"**Characteristic**: {char}",
                    f"**Difficulty**: {diff}"
                ]
                if add_info:
                    details.append(f"**Additional Info**: {add_info}")
                
                remove_embed.add_field(
                    name="Map Details",
                    value="\n".join(details),
                    inline=False
                )
                
                # Add map cover if available
                if cover_url:
                    remove_embed.set_thumbnail(url=cover_url)
                
                # Add BSR code and reason if provided
                remove_embed.add_field(name="BSR", value=bsr_code, inline=True)
                if reason:
                    remove_embed.add_field(name="Reason", value=reason, inline=True)
                
                # Add map link
                map_url = f"https://beatsaver.com/maps/{bsr_code}"
                remove_embed.add_field(name="Map Link", value=map_url, inline=False)
                
                # Add footer with who removed it
                remove_embed.set_footer(text=f"Removed by {interaction.user.name} ({interaction.user.id})")
                
                await ranked_channel.send(embed=remove_embed)
                
        except Exception as e:
            logger.error(f"Failed to send removal notification: {e}", exc_info=True)
        
        await interaction.followup.send(
            f"✅ Successfully removed map: {map_name} (BSR: {bsr_code})",
            ephemeral=True
        )
    
    @remove_map.error
    async def remove_map_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        """Handle errors for the remove_map command."""
        if isinstance(error, app_commands.MissingRole):
            await interaction.response.send_message(
                "❌ You don't have permission to use this command. Only Ranking Team members can remove maps.",
                ephemeral=True
            )
        else:
            logger.error(f"Error in remove_map: {error}", exc_info=True)
            await interaction.response.send_message(
                f"❌ An error occurred: {str(error)}",
                ephemeral=True
            )
            
    async def fetch_beatleader_profile(self, profile_identifier: str):
        """
        Fetch BeatLeader profile data from a URL, numeric ID, or player alias.
        
        Args:
            profile_identifier: Can be a BeatLeader profile URL, numeric player ID, or player alias
            
        Returns:
            dict: Player profile data if found, None otherwise
        """
        try:
            # Clean and normalize the input
            profile_identifier = profile_identifier.strip()
            
            # If it's a URL, extract the username or ID
            if 'beatleader.com/u/' in profile_identifier:
                # Extract username from profile URL (e.g., https://beatleader.com/u/lucascomposes)
                username = profile_identifier.split('/u/')[-1].split('/')[0].split('?')[0]
                if username.isdigit():
                    # It's a numeric ID in URL format
                    api_url = f"https://api.beatleader.com/player/{username}"
                else:
                    # It's a username in URL format
                    search_url = f"https://api.beatleader.com/players?search={username}"
                    async with aiohttp.ClientSession() as session:
                        async with session.get(search_url) as resp:
                            if resp.status != 200:
                                logger.error(f"Error searching for user {username}: HTTP {resp.status}")
                                return None
                            data = await resp.json()
                            if not data or not data.get('data') or not data['data']:
                                logger.error(f"No user found with username: {username}")
                                return None
                            player_id = data['data'][0]['id']
                            api_url = f"https://api.beatleader.com/player/{player_id}"
            elif '/player/' in profile_identifier:
                # Direct API URL with player ID
                player_id = profile_identifier.split('/player/')[-1].split('/')[0].split('?')[0]
                api_url = f"https://api.beatleader.com/player/{player_id}"
                logger.debug(f"Extracted player ID from URL: {player_id}")
            else:
                # It's either a numeric ID or a username
                if profile_identifier.isdigit():
                    # Direct numeric ID
                    api_url = f"https://api.beatleader.com/player/{profile_identifier}"
                    logger.debug(f"Using direct player ID: {profile_identifier}")
                else:
                    # Username search
                    search_url = f"https://api.beatleader.com/players?search={profile_identifier}"
                    logger.debug(f"Searching for username: {profile_identifier}")
                    async with aiohttp.ClientSession() as session:
                        async with session.get(search_url) as resp:
                            if resp.status != 200:
                                logger.error(f"User search failed: {profile_identifier} - HTTP {resp.status}")
                                return None
                            data = await resp.json()
                            if not data or not data.get('data') or not data['data']:
                                logger.debug(f"No user found: {profile_identifier}")
                                return None
                            player_id = data['data'][0]['id']
                            api_url = f"https://api.beatleader.com/player/{player_id}"
            
            # Fetch the full profile
            logger.debug(f"Fetching profile from: {api_url}")
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(api_url) as resp:
                        if resp.status != 200:
                            logger.error(f"Profile fetch failed - Status: {resp.status} - URL: {api_url}")
                            return None
                        logger.debug(f"Successfully fetched profile from {api_url}")
                        return await resp.json()
                    
            except aiohttp.ClientError as e:
                logger.error(f"Network error fetching profile: {str(e).splitlines()[0]}")
                return None
            except Exception as e:
                logger.error(f"Unexpected error: {str(e).splitlines()[0]}")
                return None
                
        except Exception as e:
            logger.error(f"Error in fetch_beatleader_profile: {str(e).splitlines()[0]}")
            return None
    
    def save_application(self, discord_id: str, discord_username: str, profile_data: dict) -> bool:
        """Save a new application to the pending applications database."""
        conn = None
        try:
            conn = sqlite3.connect(PENDING_APPS_DB)
            c = conn.cursor()
            
            # First check if user already has a pending application
            c.execute('SELECT 1 FROM user_applications WHERE discord_id = ? AND status = ?', 
                     (str(discord_id), 'pending'))
            if c.fetchone():
                logger.info(f"User {discord_id} already has a pending application")
                return False
            
            c.execute('''
                INSERT INTO user_applications 
                (discord_id, discord_username, beatleader_id, beatleader_username, pp, rank, country_rank, 
                 country, avatar_url, profile_url, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            ''', (
                str(discord_id),
                discord_username,
                profile_data['id'],
                profile_data['name'],
                profile_data['pp'],
                profile_data['rank'],
                profile_data['countryRank'],
                profile_data['country'],
                profile_data.get('avatar'),
                f"https://beatleader.com/u/{profile_data['id']}"
            ))
            
            conn.commit()
            logger.info(f"Saved application for user {discord_id} (BeatLeader: {profile_data['id']})")
            return True
            
        except sqlite3.Error as e:
            logger.error(f"Error saving application to database: {e}", exc_info=True)
            return False
        finally:
            if conn:
                conn.close()
    
    async def load_pending_applications(self):
        """Load pending applications from the database."""
        await self.bot.wait_until_ready()
        
        conn = None
        try:
            conn = sqlite3.connect(PENDING_APPS_DB)
            c = conn.cursor()
            
            # Get all pending applications
            c.execute('''
                SELECT discord_id, discord_username, beatleader_id, beatleader_username, 
                       pp, rank, country_rank, country, avatar_url, profile_url
                FROM user_applications 
                WHERE status = 'pending'
            ''')
            
            pending_apps = c.fetchall()
            
            if not pending_apps:
                logger.info("No pending applications found in the database.")
                return
                
            # Get the review channel
            guild = self.bot.get_guild(1384780527751397436)  # Replace with your guild ID if needed
            if not guild:
                logger.error("Could not find guild to load pending applications")
                return
                
            review_channel = guild.get_channel(self.review_channel_id)
            if not review_channel:
                logger.error("Could not find review channel to load pending applications")
                return
                
            logger.info(f"Loading {len(pending_apps)} pending applications...")
            
            for app in pending_apps:
                try:
                    # Create embed for the application
                    embed = discord.Embed(
                        title=f"Pending Application: {app[3]}",
                        description=f"**PP:** {app[4]:.2f} (#{app[5]} Global, #{app[6]} {app[7]})\n                                  **Status:** Pending Review\n"
                                  f"**Submitted:** <t:{int(datetime.now().timestamp())}:R>",
                        color=discord.Color.orange()
                    )
                    
                    if app[8]:  # avatar_url
                        embed.set_thumbnail(url=app[8])
                        
                    embed.add_field(name="Profile", value=f"[View on BeatLeader]({app[9]})", inline=True)
                    embed.add_field(name="Discord User", value=f"<@{app[0]}> (`{app[1]}`)", inline=True)
                    
                    # Send message and add reactions
                    message = await review_channel.send(embed=embed)
                    await message.add_reaction("✅")  # Approve
                    await message.add_reaction("❌")  # Deny
                    
                    # Store in memory
                    self.pending_applications[message.id] = {
                        'discord_id': app[0],
                        'discord_username': app[1],
                        'beatleader_id': app[2],
                        'beatleader_username': app[3],
                        'profile_data': {
                            'id': app[2],
                            'name': app[3],
                            'pp': app[4],
                            'rank': app[5],
                            'countryRank': app[6],
                            'country': app[7],
                            'avatar': app[8]
                        }
                    }
                    
                    logger.info(f"Loaded pending application for {app[1]} (Message ID: {message.id})")
                    
                except Exception as e:
                    logger.error(f"Error loading pending application: {e}", exc_info=True)
                    continue
                    
            logger.info("Finished loading pending applications")
            
        except Exception as e:
            logger.error(f"Error loading pending applications: {e}", exc_info=True)
        finally:
            if conn:
                conn.close()
    
    @app_commands.command(name="forcelink", description="[Ranking Team] Manually link a user to a BeatLeader profile (skips verification)")
    @app_commands.describe(
        user="Discord user to link (mention or ID)",
        profile_identifier="BeatLeader profile URL, numeric ID, or username"
    )
    @app_commands.checks.has_role('Ranking Team')
    async def force_link_profile(
        self, 
        interaction: discord.Interaction, 
        user: discord.User,
        profile_identifier: str
    ):
        """
        [Ranking Team Only] Manually link a Discord user to a BeatLeader profile.
        This bypasses the normal verification process.
        
        Parameters:
        user: The Discord user to link (mention or ID)
        profile_identifier: BeatLeader profile URL, numeric ID, or username
        """
        await interaction.response.defer(ephemeral=True)
        
        # Check if target user already has a BeatLeader profile linked
        try:
            conn = sqlite3.connect(APPROVED_USERS_DB)
            c = conn.cursor()
            c.execute("SELECT beatleader_id, username FROM approved_users WHERE discord_id = ?", (str(user.id),))
            existing_link = c.fetchone()
            
            if existing_link:
                await interaction.followup.send(
                    f"❌ {user.mention} is already linked to BeatLeader profile: "
                    f"{existing_link[1]} (ID: {existing_link[0]})\n"
                    "Use `/unlink` first if you want to change their linked profile.",
                    ephemeral=False
                )
                return
                
        except sqlite3.Error as e:
            logger.error(f"Database error checking existing link: {e}")
            await interaction.followup.send("❌ Database error. Please try again later.", ephemeral=False)
            return
        finally:
            conn.close()
            
        # Fetch the BeatLeader profile
        profile_data = await self.fetch_beatleader_profile(profile_identifier)
        if not profile_data:
            await interaction.followup.send("❌ Could not find the specified BeatLeader profile.", ephemeral=False)
            return
            
        # Save the link (bypassing normal application process)
        try:
            conn = sqlite3.connect(APPROVED_USERS_DB)
            c = conn.cursor()
            
            c.execute('''
                INSERT INTO approved_users 
                (discord_id, beatleader_id, username, pp, rank, country_rank, country, avatar_url, profile_url, approved_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    beatleader_id = excluded.beatleader_id,
                    username = excluded.username,
                    pp = excluded.pp,
                    rank = excluded.rank,
                    country_rank = excluded.country_rank,
                    country = excluded.country,
                    avatar_url = excluded.avatar_url,
                    profile_url = excluded.profile_url,
                    approved_by = excluded.approved_by,
                    last_updated = CURRENT_TIMESTAMP
            ''', (
                str(user.id),
                str(profile_data['id']),
                profile_data['name'],
                profile_data.get('pp', 0),
                profile_data.get('rank', 0),
                profile_data.get('countryRank', 0),
                profile_data.get('country', 'XX'),
                profile_data.get('avatar', ''),
                f"https://beatleader.xyz/u/{profile_data['id']}",
                str(interaction.user.id)
            ))
            
            conn.commit()
            
            # Send success message
            embed = discord.Embed(
                title="✅ Profile Linked Successfully",
                description=f"{user.mention} has been linked to BeatLeader profile:",
                color=discord.Color.green()
            )
            embed.add_field(name="BeatLeader Username", value=profile_data['name'], inline=True)
            embed.add_field(name="PP", value=f"{profile_data.get('pp', 0):.2f}", inline=True)
            embed.add_field(name="Global Rank", value=f"#{profile_data.get('rank', 'N/A')}", inline=True)
            embed.add_field(name="Country Rank", value=f"#{profile_data.get('countryRank', 'N/A')}", inline=True)
            embed.add_field(name="Linked By", value=interaction.user.mention, inline=True)
            
            if 'avatar' in profile_data and profile_data['avatar']:
                embed.set_thumbnail(url=profile_data['avatar'])
                
            await interaction.followup.send(embed=embed, ephemeral=False)
            
            # Send DM to the linked user
            try:
                dm_embed = discord.Embed(
                    title="🔗 BeatLeader Profile Linked",
                    description=f"Your account has been linked to the BeatLeader profile **{profile_data['name']}** by a ranking team member.",
                    color=discord.Color.blue()
                )
                dm_embed.add_field(name="Profile", value=f"[View on BeatLeader](https://beatleader.xyz/u/{profile_data['id']})", inline=False)
                if 'avatar' in profile_data and profile_data['avatar']:
                    dm_embed.set_thumbnail(url=profile_data['avatar'])
                await user.send(embed=dm_embed)
            except discord.Forbidden:
                pass  # User has DMs disabled or blocked the bot
                
        except sqlite3.Error as e:
            logger.error(f"Database error saving forced link: {e}")
            await interaction.followup.send("❌ Error saving the link to the database. Please try again.", ephemeral=False)
        except Exception as e:
            logger.error(f"Unexpected error in forcelink: {e}", exc_info=True)
            await interaction.followup.send(f"❌ An unexpected error occurred: {str(e)}", ephemeral=False)
        finally:
            if 'conn' in locals():
                conn.close()

    @app_commands.command(name="link", description="Link your BeatLeader profile to apply for SSC")
    @app_commands.describe(profile_identifier="Your BeatLeader profile URL, numeric ID, or username")
    async def link_profile(self, interaction: discord.Interaction, profile_identifier: str):
        """
        Link your BeatLeader profile to apply for SSC.
        
        You can provide either:
        - Your full BeatLeader profile URL (https://beatleader.xyz/u/...)
        - Your numeric BeatLeader ID
        - Your BeatLeader username (case sensitive)
        """
        if not await self.check_guild_permission(interaction):
            return
            
        await interaction.response.defer(ephemeral=True)
        
        # Check if user already has an approved profile
        try:
            conn = sqlite3.connect(APPROVED_USERS_DB)
            c = conn.cursor()
            c.execute('SELECT 1 FROM approved_users WHERE discord_id = ?', (str(interaction.user.id),))
            if c.fetchone():
                await interaction.followup.send("✅ Your BeatLeader profile is already linked and approved!", ephemeral=True)
                return
        except sqlite3.Error as e:
            logger.error(f"Error checking approved users: {e}", exc_info=True)
            await interaction.followup.send("❌ An error occurred while checking your profile status.", ephemeral=True)
            return
        finally:
            if 'conn' in locals():
                conn.close()
        
        # Check for existing pending application
        try:
            conn = sqlite3.connect(PENDING_APPS_DB)
            c = conn.cursor()
            c.execute('''
                SELECT 1 FROM user_applications 
                WHERE discord_id = ? AND status = 'pending'
            ''', (str(interaction.user.id),))
            if c.fetchone():
                await interaction.followup.send(
                    "⏳ You already have a pending application. Please wait for it to be reviewed.",
                    ephemeral=True
                )
                return
        except sqlite3.Error as e:
            logger.error(f"Error checking pending applications: {e}", exc_info=True)
            await interaction.followup.send("❌ An error occurred while checking your application status.", ephemeral=True)
            return
        finally:
            if 'conn' in locals():
                conn.close()
        
        # Fetch profile data
        try:
            profile_data = await self.fetch_beatleader_profile(profile_identifier)
            if not profile_data:
                await interaction.followup.send(
                    "❌ Could not find a BeatLeader profile with that identifier. Please check and try again.\n\n"
                    "You can use:\n"
                    "- Your full BeatLeader profile URL (https://www.beatleader.com/u/...)\n"
                    "- Your numeric BeatLeader ID\n"
                    "- Your exact BeatLeader username (case sensitive)",
                    ephemeral=True
                )
                return
                
            # Save application
            if not self.save_application(interaction.user.id, str(interaction.user), profile_data):
                await interaction.followup.send(
                    "❌ You already have a pending application or an error occurred. Please try again later.",
                    ephemeral=True
                )
                return
                
            # Send to review channel
            review_channel = interaction.guild.get_channel(self.review_channel_id)
            if not review_channel:
                raise Exception("Review channel not found")
                    
            # Create application embed
            embed = discord.Embed(
                title=f"New Application: {profile_data['name']}",
                description=(
                    f"**PP:** {profile_data['pp']:.2f} "
                    f"(#{profile_data['rank']} Global, "
                    f"#{profile_data['countryRank']} {profile_data.get('country', '??')})\n                    **Scores:** {profile_data.get('scoreStats', {}).get('totalRankedScore', 0):,} ranked | "
                    f"{profile_data.get('scoreStats', {}).get('totalUnrankedScore', 0):,} unranked\n"
                    f"**Play Count:** {profile_data.get('scoreStats', {}).get('totalPlayCount', 0):,} "
                    f"({profile_data.get('scoreStats', {}).get('rankedPlayCount', 0):,} ranked)"
                ),
                color=discord.Color.blue()
            )
            
            if profile_data.get('avatar'):
                embed.set_thumbnail(url=profile_data['avatar'])
                    
            embed.add_field(
                name="Profile", 
                value=f"[View on BeatLeader](https://beatleader.com/u/{profile_data['id']})", 
                inline=True
            )
            embed.add_field(
                name="Discord User", 
                value=f"{interaction.user.mention} (`{interaction.user}`)", 
                inline=True
            )
            
            # Add top plays if available
            if 'scoreStats' in profile_data:
                stats = profile_data['scoreStats']
                top_plays = [
                    f"★ {stats.get('topPp', 0):.2f}pp (Top PP)",
                    f"★ {stats.get('topAccPP', 0):.2f}pp (Top Acc)",
                    f"★ {stats.get('topPassPP', 0):.2f}pp (Top Pass)",
                    f"★ {stats.get('topTechPP', 0):.2f}pp (Top Tech)",
                ]
                embed.add_field(
                    name="Top Plays", 
                    value="\n".join(top_plays), 
                    inline=False
                )
            
            embed.set_footer(text=f"User ID: {interaction.user.id} | Application ID: {profile_data['id']}")
            
            # Send message and add reactions
            message = await review_channel.send(
                "New application to review!", 
                embed=embed
            )
            await message.add_reaction("✅")  # Approve
            await message.add_reaction("❌")  # Deny
            
            # Store message ID for reaction handling
            self.pending_applications[message.id] = {
                'discord_id': str(interaction.user.id),
                'discord_username': str(interaction.user),
                'beatleader_id': profile_data['id'],
                'beatleader_username': profile_data['name'],
                'profile_data': profile_data
            }
            
            await interaction.followup.send(
                "✅ Your application has been submitted to the ranking team! You'll be notified when it's reviewed.",
                ephemeral=False  # Make the response visible to everyone in the channel
            )
            
        except Exception as e:
            logger.error(f"Error in link_profile: {e}", exc_info=True)
            try:
                await interaction.followup.send(
                    "❌ An error occurred while processing your application. Please try again later.",
                    ephemeral=True
                )
            except:
                pass  # If we can't send the followup, there's nothing we can do
    
    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload):
        """Handle reactions on application messages."""
        # Ignore reactions from bots or not in review channel
        if payload.member.bot or payload.channel_id != self.review_channel_id:
            return
            
        # Check if it's a reaction to an application message
        if payload.message_id not in self.pending_applications:
            return
            
        # Check if the user has the ranking team role
        guild = self.bot.get_guild(payload.guild_id)
        member = guild.get_member(payload.user_id)
        if not any(role.id == self.RANKING_TEAM_ROLE_ID for role in member.roles):
            return
            
        application = self.pending_applications[payload.message_id]
        channel = guild.get_channel(payload.channel_id)
        message = await channel.fetch_message(payload.message_id)
        
        try:
            if str(payload.emoji) == "✅":
                # approve application
                conn = sqlite3.connect(PENDING_APPS_DB)
                c = conn.cursor()
                
                # update application status
                c.execute('''
                    UPDATE user_applications 
                    SET status = 'approved', reviewed_by = ?, review_time = CURRENT_TIMESTAMP
                    WHERE discord_id = ? AND beatleader_id = ?
                ''', (str(payload.user_id), application['discord_id'], application['beatleader_id']))
                conn.commit()
                conn.close()
                
                # add to approved users
                conn = sqlite3.connect(APPROVED_USERS_DB)
                c = conn.cursor()
                c.execute('''
                    INSERT OR REPLACE INTO approved_users 
                    (discord_id, beatleader_id, username, pp, rank, country_rank, 
                     country, avatar_url, profile_url, approved_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    application['discord_id'],
                    application['beatleader_id'],
                    application['beatleader_username'],
                    application['profile_data']['pp'],
                    application['profile_data']['rank'],
                    application['profile_data']['countryRank'],
                    application['profile_data'].get('country', '??'),
                    application['profile_data'].get('avatar'),
                    f"https://beatleader.com/u/{application['beatleader_id']}",
                    str(payload.user_id)
                ))
                
                conn.commit()
                
                # Send DM to user
                try:
                    user = await self.bot.fetch_user(int(application['discord_id']))
                    embed = discord.Embed(
                        title="✅ Application Approved!",
                        description=f"Your BeatLeader profile has been approved by the SSC ranking team!",
                        color=discord.Color.green()
                    )
                    embed.add_field(name="BeatLeader Profile", value=f"[View Profile](https://beatleader.com/u/{application['beatleader_id']})", inline=False)
                    embed.set_thumbnail(url=application['profile_data'].get('avatar', ''))
                    await user.send(embed=embed)
                except Exception as e:
                    logger.error(f"Error sending approval DM: {e}", exc_info=True)
                
                # Update message
                embed = message.embeds[0]
                embed.color = discord.Color.green()
                embed.set_footer(text=f"✅ Approved by {member.display_name} • {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                await message.edit(embed=embed)
                await message.clear_reactions()
                
                # Remove from pending applications
                del self.pending_applications[payload.message_id]
                
            elif str(payload.emoji) == "❌":
                # Deny application
                conn = sqlite3.connect(PENDING_APPS_DB)
                c = conn.cursor()
                
                # Update application status
                c.execute('''
                    UPDATE user_applications 
                    SET status = 'denied', reviewed_by = ?, review_time = CURRENT_TIMESTAMP
                    WHERE discord_id = ? AND beatleader_id = ?
                ''', (str(payload.user_id), application['discord_id'], application['beatleader_id']))
                
                conn.commit()
                conn.close()
                # Send DM to user
                try:
                    user = await self.bot.fetch_user(int(application['discord_id']))
                    embed = discord.Embed(
                        title="❌ Application Denied",
                        description=f"Your BeatLeader profile was not approved by the SSC ranking team.",
                        color=discord.Color.red()
                    )
                    embed.add_field(name="Reason", value="Please contact a ranking team member for more information.", inline=False)
                    await user.send(embed=embed)
                except Exception as e:
                    logger.error(f"Error sending denial DM: {e}", exc_info=True)
                
                # Update message
                embed = message.embeds[0]
                embed.color = discord.Color.red()
                embed.set_footer(text=f"❌ Denied by {member.display_name} • {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                await message.edit(embed=embed)
                await message.clear_reactions()
                
                # Remove from pending applications
                del self.pending_applications[payload.message_id]
                
            # Remove the reaction
            await message.remove_reaction(payload.emoji, member)
            
        except Exception as e:
            logger.error(f"Error processing reaction: {e}", exc_info=True)
            await message.remove_reaction(payload.emoji, member)

async def setup(bot):
    try:
        await bot.add_cog(SSCTools(bot))
        logger.info("SSCTools cog loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load SSCTools cog: {e}", exc_info=True)
        raise