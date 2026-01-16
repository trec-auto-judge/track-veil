"""Generate pronounceable pseudonyms for anonymization.

Teams get 3-letter CVC (consonant-vowel-consonant) names like "Bax", "Cog", "Fez".
Runs get 2-digit numbers like "07", "42", "93".

Names are pre-shuffled so assignment order doesn't leak information.
"""

import random
from typing import List, Optional


CONSONANTS = list("bcdfghjklmnprstvwxz")  # 19 consonants (no q)
VOWELS = list("aeiou")  # 5 vowels


def generate_cvc_pool(seed: Optional[int] = None) -> List[str]:
    """Generate all CVC combinations and shuffle them.

    Returns ~1805 pronounceable 3-letter names like "Bax", "Cog", "Dip".
    """
    names = []
    for c1 in CONSONANTS:
        for v in VOWELS:
            for c2 in CONSONANTS:
                names.append(f"{c1.upper()}{v}{c2}")

    rng = random.Random(seed)
    rng.shuffle(names)
    return names


def generate_digit_pool(digits: int = 2, seed: Optional[int] = None) -> List[str]:
    """Generate zero-padded digit strings and shuffle them.

    digits=2 gives "01" through "99" (excludes "00").
    """
    max_val = 10 ** digits - 1
    numbers = [str(i).zfill(digits) for i in range(1, max_val + 1)]

    rng = random.Random(seed)
    rng.shuffle(numbers)
    return numbers


class PseudonymPool:
    """Manages pools of pseudonyms for teams and runs.

    Usage:
        pool = PseudonymPool(seed=42)
        team_anon = pool.get_team_pseudonym()  # e.g., "Fez"
        run_anon = pool.get_run_pseudonym()    # e.g., "07"
    """

    def __init__(self, seed: Optional[int] = None):
        self._seed = seed
        self._team_pool = generate_cvc_pool(seed)
        self._run_pool = generate_digit_pool(digits=2, seed=seed)
        self._team_index = 0
        self._run_index = 0

    def get_team_pseudonym(self) -> str:
        """Get next available team pseudonym."""
        if self._team_index >= len(self._team_pool):
            raise RuntimeError(
                f"Exhausted team pseudonym pool ({len(self._team_pool)} names). "
                "Too many unique teams."
            )
        name = self._team_pool[self._team_index]
        self._team_index += 1
        return name

    def get_run_pseudonym(self) -> str:
        """Get next available run pseudonym."""
        if self._run_index >= len(self._run_pool):
            raise RuntimeError(
                f"Exhausted run pseudonym pool ({len(self._run_pool)} numbers). "
                "Too many unique runs."
            )
        number = self._run_pool[self._run_index]
        self._run_index += 1
        return number

    @property
    def teams_remaining(self) -> int:
        return len(self._team_pool) - self._team_index

    @property
    def runs_remaining(self) -> int:
        return len(self._run_pool) - self._run_index

    def set_indices(self, team_index: int, run_index: int):
        """Restore pool state from saved mapping store."""
        self._team_index = team_index
        self._run_index = run_index
