from typing import Dict, List, Optional
import random

class ErrorMessages:
    """A helper class for managing error messages and responses."""
    
    # Emoji shorthands
    shorthands = {
        'normal': '<:en:1379300688852549663>',
        'angry': '<:en_surprised:1379300752656171029>',
        'unamused': '<:en_what:1379300810269130793>',
        'cry': '<:en_cry:1379307090346115172>',
        'dizzy': '<:en_bsod:1379308614660919336>',
        'sleep': '<:en_yawn:1379307375227568190>',
        'drunk': '<:en_yawn:1379307375227568190>',
        'eat': '<:en_83c:1379307816388526110>',
        'fear': '<:en_shock:1379307622099980318>',
        'smug': '<:en_83c:1379307816388526110>',
        'huh': '<:en_speak:1379301712845602889>',
        'love': '<:en_pancakes:1379308143573598298>'
    }
    
    # Error messages
    errors = {
        # Protection level errors
        'protection_denied': [
            "{cry} {reason}",
            "{unamused} {reason}",
            "{sleep} {reason}"
        ],
        'confirm_moderate_staff': [
            "{fear} Warning: You are about to moderate a staff member. Are you sure? (yes/no)",
            "{huh} This user is a staff member. Type 'yes' to confirm:",
            "{unamused} Confirm moderation of staff member (yes/no):"
        ],
        'not_implemented': [
            "{normal} The {feature} feature is not implemented yet.",
            "{sleep} I can't do that yet. {feature} is coming soon!"
        ],
        'channel_not_found': [
            "{huh} I couldn't find the specified channel.",
            "{unamused} Channel not found. Please check the configuration."
        ],
        'no_downloads': [
            "{huh} There are no downloads available right now.",
            "{sleep} No downloads found in the channel."
        ],
        'toss_logs_not_configured': [
            "{angry} Toss logs channel is not configured for this server.",
            "{huh} Please set up a toss logs channel first."
        ],
        'toss_category_not_configured': [
            "{angry} Toss category is not configured for this server.",
            "{huh} Please set up a toss category first."
        ],
        'already_tossed': [
            "{unamused} {user} is already tossed!",
            "{sleep} {user} is already in the tossed state."
        ],
        'not_tossed': [
            "{huh} {user} is not tossed!",
            "{sleep} I can't find {user} in the tossed users list."
        ],
        'not_toss_channel': [
            "{angry} This is not a toss channel!",
            "{huh} You can only use this command in a toss channel."
        ],
        'staff_protected': [
            "{angry} You can't do that to a staff member!",
            "{unamused} Staff members are protected from this action."
        ],
        'max_channels': [
            "{angry} Maximum number of toss channels reached!",
            "{sleep} Please close some toss channels before creating new ones."
        ],
        'generic': [
            "{cry} I'm sorry! Something broke! I let Lucas know about it!",
            "{dizzy} Something broke while I was doing that... I let Lucas know..."
        ],
        'servers_only': [
            "{huh} Hey, you have to do this in a server... I don't know what server you want..."
        ],
        'dms_only': [
            "{normal} You can only do this in DMs, you know."
        ],
        'quotes': [
            "{dizzy} I didn't get that, your quotes are off...",
            "{angry} Hey, check your quotes! I can't work with this input..."
        ],
        'missing_role': [
            "{angry} No, I'm not listening to you! You don't have the role you need for that.",
            "{sleep} Nope. Didn't hear that. I can only hear people with a role saying that."
        ],
        'missing_perms': [
            "{huh} I can't do that, I don't have the permissions for that.",
            "{fear} I tried, but it didn't work! It told me I needed permissions...",
            "{unamused} How am I supposed to do that without permissions..."
        ],
        'cooldown': [
            "{dizzy} Hey, slow down, I can't go that fast... Give me {time} seconds.",
            "{angry} Stop trying to overwork me, I'm on a cooldown... Try again in {time} seconds.",
            "{smug} Sure, I'll get to that... in {time} seconds."
        ],
        'no_attachment': [
            "{huh} You didn't give me an attachment to work with...",
            "{angry} It's not funny, I need an attachment to work with.",
            "{unamused} W-With what attachment?"
        ],
        'unauthorized': [
            "{normal} Lalala, I can't hear you. You're unauthorized.",
            "{sleep} ...Huh? Who are you? You can't ask me to do that.",
            "{unamused} Why am I going to do that for you...? I don't know you."
        ],
        'dm_failed': [
            "{fear} I can't do that, you won't let me DM you! Check your DM settings...",
            "{unamused} Hey, I can't DM you... check your settings first."
        ],
        'user_not_found': [
            "{huh} I don't know who that user is...",
            "{normal} Discord API says that's not a user.",
            "{unamused} Hey, that isn't a user. It doesn't exist."
        ],
        'member_not_found': [
            "{normal} That's not a member on this server.",
            "{huh} You either gave me something wrong, or that member isn't on this server...",
            "{normal} I looked, that's not a member here."
        ],
        'missing_required_argument': [
            "{huh} You're missing a required argument for that command.",
            "{unamused} I need more information to do that. Check the command usage.",
            "{normal} You forgot to include a required argument. Try again?"
        ]
    }
    
    # Warning messages
    warnings = {
        'target_self': [
            "{angry} For the sake of everyone else, {author}, don't do this to yourself!",
            "{unamused} {author}, you're a test subject, but this isn't safe...",
            "{normal} {author}, I'm not doing that."
        ],
        'target_bot': [
            "{smug} I'm sorry {author}, I'm afraid I can't do that.",
            "{unamused} {author}, I am not a test subject like you.",
            "{angry} Please leave me alone, {author}!"
        ],
        'timeout': [
            "{normal} Your command timed out.",
            "{sleep} I got bored waiting, so I timed out your command.",
            "{angry} I can't wait for your response forever! Command timed out."
        ]
    }
    
    # Info messages
    info = {
        'not_implemented': [
            "{normal} The {feature} feature is not implemented yet.",
            "{sleep} I can't do that yet. {feature} is coming soon!"
        ],
        'no_downloads': [
            "{huh} There are no downloads available right now.",
            "{sleep} No downloads found in the channel."
        ],
        'user_tossed': [
            "{normal} {user} has been tossed! {channel}",
            "{sleep} {user} is now in {channel}."
        ],
        'quit': [
            "{cry} Not again! Why must you do this again!",
            "{sleep} I'm already asleep, this changes nothing...",
            "{huh} What? Drop the lightbulb? Okay...",
            "{eat} Yay, lunch break!",
            "{drunk} What, just because I had one drink? Boo...",
            "{smug} You're not safe, you know...",
            "{normal} Bathroom break!"
        ],
        'no_errors': [
            "{eat} You don't have any errors yet.",
            "{sleep} I haven't seen any errors happen yet..."
        ],
        'hello': [
            "{normal} Helloooooo.",
            "{eat} Hello. What's up?",
            "{sleep} ...Wha?"
        ],
        'embed': [
            "{normal} If you can't see this, check your message link settings.",
            "{eat} Here's your embed. Check your settings if you can't see it."
        ]
    }
    
    @classmethod
    def _format(cls, message: str, **kwargs) -> str:
        """Format a message with shorthands and additional context."""
        # First replace shorthands
        for key, value in cls.shorthands.items():
            message = message.replace(f"{{{key}}}", value)
        
        # Then replace any additional context
        for key, value in kwargs.items():
            message = message.replace(f"{{{key}}}", str(value))
            
        return message
    
    @classmethod
    def get_error(cls, error_type: str, **kwargs) -> str:
        """Get a random error message of the specified type."""
        if error_type not in cls.errors:
            return cls._format("{cry} An unknown error occurred.")
            
        message = random.choice(cls.errors[error_type])
        return cls._format(message, **kwargs)
    
    @classmethod
    def get_warning(cls, warning_type: str, **kwargs) -> str:
        """Get a random warning message of the specified type."""
        if warning_type not in cls.warnings:
            return cls._format("{huh} An unknown warning occurred.")
            
        message = random.choice(cls.warnings[warning_type])
        return cls._format(message, **kwargs)
    
    @classmethod
    def get_info(cls, info_type: str, **kwargs) -> str:
        """Get a random info message of the specified type."""
        if info_type not in cls.info:
            return cls._format("{normal} Information not available.")
            
        message = random.choice(cls.info[info_type])
        return cls._format(message, **kwargs)
