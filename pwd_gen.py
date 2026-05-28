#!/usr/bin/env python3
"""
Secure Password & Passphrase Generator
Generates random passwords and memorable Diceware-style passphrases.
"""

import secrets
import string


def generate_password(length: int = 16) -> str:
    """Generate a secure random password."""
    alphabet = string.ascii_letters + string.digits + string.punctuation
    while True:
        pwd = ''.join(secrets.choice(alphabet) for _ in range(length))
        if (any(c.islower() for c in pwd)
            and any(c.isupper() for c in pwd)
            and any(c.isdigit() for c in pwd)
            and any(c in string.punctuation for c in pwd)):
            return pwd


def generate_passphrase(words: int = 5) -> str:
    """Generate a Diceware-style passphrase."""
    wordlist = [
        "apple", "bridge", "candle", "dragon", "eagle", "forest", "garden",
        "harbor", "island", "jungle", "knight", "lemon", "marble", "needle",
        "ocean", "palace", "quartz", "rabbit", "shadow", "tunnel", "violet",
        "window", "yellow", "zebra", "anchor", "breeze", "crystal", "diamond",
        "emerald", "falcon", "galaxy", "hammer", "igloo", "jacket", "lantern",
        "meadow", "nebula", "orbit", "pearl", "quest", "rocket", "storm",
        "tiger", "unicorn", "val
