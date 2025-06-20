"""
Name generator for Blaze daemons.
Generates a random 3-word hyphenated name for easy identification.
"""

import random

# Word lists for creating daemon names
ADJECTIVES = [
    "swift", "blazing", "rapid", "quick", "fast", "nimble", "agile", "speedy",
    "fiery", "burning", "glowing", "radiant", "bright", "shining", "luminous",
    "cosmic", "stellar", "lunar", "solar", "astral", "celestial", "galactic",
    "mighty", "powerful", "strong", "robust", "sturdy", "solid", "durable",
    "silent", "quiet", "subtle", "gentle", "calm", "serene", "peaceful",
    "clever", "smart", "wise", "sage", "sharp", "brilliant", "intelligent",
    "lively", "vibrant", "dynamic", "energetic", "active", "spirited", "vivid",
    "hidden", "secret", "veiled", "masked", "covert", "stealth", "shadowy",
    "ancient", "elder", "primal", "eternal", "timeless", "endless", "ageless",
]

NOUNS = [
    "flame", "spark", "ember", "blaze", "fire", "inferno", "furnace", "torch",
    "comet", "star", "planet", "moon", "sun", "asteroid", "meteor", "nova",
    "hawk", "eagle", "falcon", "owl", "raven", "phoenix", "dragon", "griffin",
    "tiger", "lion", "panther", "wolf", "bear", "shark", "whale", "dolphin",
    "mountain", "peak", "canyon", "valley", "river", "ocean", "lake", "forest",
    "crystal", "gem", "ruby", "emerald", "sapphire", "diamond", "pearl", "opal",
    "nexus", "node", "core", "hub", "matrix", "vertex", "beacon", "signal",
    "titan", "giant", "colossus", "behemoth", "goliath", "atlas", "hercules",
    "cipher", "enigma", "puzzle", "riddle", "mystery", "secret", "labyrinth",
]

ELEMENTS = [
    "wind", "air", "breeze", "gale", "tempest", "storm", "hurricane", "cyclone",
    "earth", "stone", "rock", "boulder", "mountain", "cliff", "peak", "crag",
    "water", "river", "stream", "ocean", "sea", "lake", "cascade", "waterfall",
    "fire", "flame", "blaze", "inferno", "ember", "spark", "furnace", "forge",
    "metal", "iron", "steel", "bronze", "silver", "gold", "platinum", "titanium",
    "crystal", "gem", "jewel", "diamond", "ruby", "sapphire", "emerald", "opal",
    "light", "beam", "ray", "glow", "gleam", "flash", "radiance", "luminance",
    "shadow", "shade", "darkness", "dusk", "twilight", "midnight", "eclipse",
]

def generate_daemon_name() -> str:
    """
    Generate a random 3-word hyphenated name for a daemon instance.
    
    Returns:
        A string like "swift-dragon-inferno"
    """
    adjective = random.choice(ADJECTIVES)
    noun = random.choice(NOUNS)
    element = random.choice(ELEMENTS)
    
    return f"{adjective}-{noun}-{element}"