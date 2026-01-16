"""Generate pseudonyms for anonymization.

Teams get "T" + 3-digit number like "T042", "T196", "T007".
Runs get short plantimal names like "ant", "oak", "bear".

Names are pre-shuffled so assignment order doesn't leak information.
"""

import random
from typing import List, Optional


CONSONANTS = list("bcdfghjklmnprstvwxz")  # 19 consonants (no q)
VOWELS = list("aeiou")  # 5 vowels


def generate_cvc_names() -> List[str]:
    """Generate all CVC (consonant-vowel-consonant) combinations.

    Returns ~1805 pronounceable 3-letter names like "bax", "cog", "dip".
    """
    names = []
    for c1 in CONSONANTS:
        for v in VOWELS:
            for c2 in CONSONANTS:
                names.append(f"{c1}{v}{c2}")
    return names


# PLANTIMALS = [
#   "ant","bee","bat","cat","dog","cow","pig","hen","ram","ewe","yak","ape","emu","owl","rat","fox","elk","cod","eel","gar","asp","bug","fly","gnu","frog","toad","carp","tuna","seal","boar","deer","mole","hare","crab","clam","slug","tick","mite","wasp","moth",
#   "oak","elm","yew","fir","rye","pea","tea","soy","fig","nut","ash","ivy","yam",
#   "bear","wolf","goat","lamb","calf","fawn","colt","crow","gull","swan","duck","loon","tern","lark","wren","hawk","kite","ibis","vole","shad","bass","pike","sole","perch","flea","newt","oxen","dodo","mink","puma","lynx",
#   "pine","reed","moss","fern","kelp","lily","iris","rose","sage","mint","aloe","leek","kale","beet","corn","rice","oats","flax","hemp","date","pear","plum","kiwi","lime","okra","bean","dill","nori","ulva","acer","rosa",
#   "horse","sheep","camel","zebra","tiger","lion","hyena","otter","beaver","sloth","panda","koala","lemur","skunk","stoat","snake","gecko","skink","coral","shrimp","squid","whale","shark","guppy","trout","bison","moose","llama","alpaca",
#   "grass","shrub","cedar","birch","maple","olive","mango","peach","apple","guava","melon","berry","onion","garlic","chili","radish","turnip","basil","thyme","cumin","anise","poppy","lotus","algae","fungi","lichen"
# ]
PLANTIMALS =[
  "ant","ape","auk","bat","bee","bug","cat","cod","cow","cub","dog","doe","eel","elk","emu","ewe","fox","fly","gnu","hen","hog","owl","ox","pig","pup","ram","rat","ray","yak",
  "ash","elm","fir","ivy","oak","pea","pod","rye","sap","soy","tea","yam","yew",
  "bass","bear","beet","bird","boar","calf","clam","colt","corn","crow","crab","deer","dill","duck","fern","fig","frog","goat","gull","hare","hawk","herb","ibis","kite","lamb","lark","leek","lime","lion","loon","lynx","mink","mite","mole","moth","newt","nut","okra","oats","pear","perch","pike","pine","plum","puma","reed","rice","rose","sage","seal","slug","sole","swan","tern","tick","toad","tuna","vole","wasp","weed","wren",
  "adder","algae","alpaca","anise","aphid","apple","aspen","badger","bamboo","banana","beaver","berry","bison","borage","buffet","butter","cactus","camel","canary","carrot","catnip","celery","cheese","cherry","chives","clammy","clover","coconut","copper","corals","cotton","coyote","cricket","crocod","cumin","daikon","donkey","dragon","eagle","earwig","edamame","egrets","elmwood","falcon","ferret","figwort","finch","fungus","galago","garlic","gerbil","ginger","ginkgo","goose","gopher","grapes","grassh","ground","guava","hamster","hazeln","hermit","hibisc","hollyh","hornet","horses","hyenas","iguana","jackal","jaguar","juniper","katydid","kelper","kettle","knotwe","larch","lemons","lentil","lichen","lobster","locust","maggot","mallow","mammal","mantis","marlin","meadow","melons","minnow","mollus","monkey","mosquito","mulberry","mushrm","mustel","narwhl","nectar","newton","nutmeg","octopi","olives","onions","orchid","otters","papaya","parrot","parsley","peanut","pepper","petrel","phalar","pigeon","pineap","pistil","plover","pollen","poppy","porcup","potato","prawns","prunes","pumpkn","rabbit","radish","raccoo","raptor","reptil","robin","salmon","sapling","scallp","scarab","seaweed","sesame","shallt","shrimp","skinks","snails","sparrow","spinac","sponge","spruce","squash","squid","starfi","stoats","stork","sunfis","swine","tapirs","thrush","thymer","tomato","tulips","turnip","turtle","urchin","walnut","weasel","weevl","whales","willow","wolves","woodpe","yarrow","zebras"
]



def generate_team_pool(seed: Optional[int] = None) -> List[str]:
    """Generate team pseudonyms as "T" + 3-digit number.

    Returns 999 team names like "T001", "T042", "T196".
    """
    names = [f"T{i:03d}" for i in range(1, 1000)]

    rng = random.Random(seed)
    rng.shuffle(names)
    return names


def generate_plantimal_pool(seed: Optional[int] = None) -> List[str]:
    """Generate run pseudonyms from PLANTIMALS + CVC fallback.

    Returns shuffled list: first PLANTIMALS, then CVC names as fallback.
    Both lists are shuffled independently, then concatenated.
    """
    rng = random.Random(seed)

    # Shuffle plantimals
    plantimals = PLANTIMALS.copy()
    rng.shuffle(plantimals)

    # Generate and shuffle CVC fallback names (exclude any that are in PLANTIMALS)
    plantimal_set = set(PLANTIMALS)
    cvc_names = [name for name in generate_cvc_names() if name not in plantimal_set]
    rng.shuffle(cvc_names)

    # PLANTIMALS first, then CVC fallback
    return plantimals + cvc_names


class PseudonymPool:
    """Manages pools of pseudonyms for teams and runs.

    Usage:
        pool = PseudonymPool(seed=42)
        team_anon = pool.get_team_pseudonym()  # e.g., "T042"
        run_anon = pool.get_run_pseudonym()    # e.g., "bear"
    """

    def __init__(self, seed: Optional[int] = None):
        self._seed = seed
        self._team_pool = generate_team_pool(seed)
        self._run_pool = generate_plantimal_pool(seed)
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
