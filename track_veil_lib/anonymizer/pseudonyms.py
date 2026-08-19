"""Generate pseudonyms for anonymization.

Teams get "T" + 3-digit number like "T042", "T196", "T007".
2025: Runs get short plantimal names like "ant", "oak", "bear" - fallback cvc.
2026: Runs get short baby names like "edgar", "linda" - fallback cvvc

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

def generate_cvvc_names() -> List[str]:
    """Generate all CVVC (consonant-vowel-consonant) combinations.

    Returns ~1805 pronounceable 4-letter names like "baox", "coog", "diap".
    """
    names = []
    for c1 in CONSONANTS:
        for v1 in VOWELS:
            for v2 in VOWELS:
                for c2 in CONSONANTS:
                    names.append(f"{c1}{v1}{v2}{c2}")
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

BABY_NAMES = [
    "aaron", "abby", "abel", "abram", "adam", "adair", "adele", "adela",
    "adina", "adrian", "aidan", "aimee", "alan", "alana", "alba", "alden",
    "alec", "alex", "alice", "alina", "alisa", "allan", "alma", "alton",
    "alvin", "alyssa", "amara", "amber", "amos", "amy", "ana", "andi",
    "andre", "andy", "anika", "anita", "anna", "annie", "april", "arden",
    "ari", "aria", "ariel", "arlen", "arlo", "aron", "arthur", "asa",
    "ava", "avery",

    "bailey", "barry", "beau", "bella", "ben", "bena", "benny", "beth",
    "betty", "bill", "blair", "blake", "boris", "boyd", "brad", "brady",
    "bree", "brent", "brett", "briar", "brian", "brooke", "bruce", "bruno",
    "bryce",

    "caleb", "calla", "calvin", "cara", "carl", "carla", "carlo", "carmen",
    "carol", "cary", "casey", "cesar", "chloe", "chris", "cindy",
    "clara", "clark", "cleo", "cliff", "clyde", "colin", "conor", "cora",
    "corey", "craig",

    "dale", "damon", "dana", "dane", "darcy", "daria", "dario",
    "darren", "david", "dawn", "dean", "debra", "deena", "derek", "devin",
    "devon", "diana", "diego", "dina", "dora", "drew", "dylan",

    "eden", "edgar", "edith", "edwin", "eli", "elia", "elias", "elise",
    "eliza", "ella", "ellen", "ellie", "elmer", "elsa", "emery", "emily",
    "emma", "enid", "eric", "erich", "erin", "esme", "estelle", "ethan",
    "etta", "eva", "evan", "eve", "ezra",

    "faith", "farah", "faye", "felix", "finn", "fiona", "floyd",
    "flynn", "fran", "frank", "fred", "freya",

    "gabe", "gail", "gary", "gavin", "gemma", "gene", "geoff", "gia",
    "gilda", "gina", "glen", "grace", "grant", "greta", "gwen",

    "hal", "hall", "hana", "hank", "hans", "harry", "harvey",
    "heidi", "helen", "henry", "hilda", "hope", "hugo",

    "ian", "ida", "ilan", "ilse", "ines", "irene", "irma",
    "isaac", "ivan",

    "jace", "jack", "jada", "jade", "jake", "jamie", "jan", "jana",
    "jane", "janis", "jared", "jason", "jay", "jean", "jeff", "jena",
    "jenna", "jerry", "jesse", "jill", "joan", "jody", "joe", "joel",
    "joey", "john", "jolie", "jon", "jonas", "joni", "jorge", "jose",
    "joy", "joyce", "juan", "julia", "julie", "june", "justin",

    "kane", "kara", "karl", "kasey", "kate", "katie", "kay",
    "keith", "kelly", "ken", "kent", "kian", "kim", "kira", "kirk",
    "knox", "kurt", "kyle",

    "lacey", "lana", "lance", "lane", "lara", "lars", "laura", "leah",
    "lena", "lenny", "leo", "leon", "liam", "liana", "lidia", "lila",
    "lina", "linda", "lisa", "livia", "liz", "lloyd", "logan",
    "lola", "loren", "lori",  "louis", "luca", "lucy",
    "luke", "luna",

    "mabel", "mae", "maia", "mandy", "mara", "marc", "marco", "marcy",
    "maria", "marie", "mario", "mark", "marla", "marta", "mary", "mason",
    "max", "maya", "megan", "meryl", "mia", "mica", "micah", "mike",
    "mila", "miles", "milo", "mimi", "mina", "mira", "moira", "molly",

    "nadia", "nate", "neil", "nell", "nessa", "nia", "niko", "nina",
    "noah", "noel", "nola", "nora",

    "odin", "olga", "omar", "oona", "opal", "orin", "oscar", "otto",
    "owen",

    "pablo", "paige", "pam", "paris", "pat", "paul", "paula",
    "pedro", "penny", "perry", "peter", "petra", "pia", "piers", "piper",
    "polly",

    "quinn",

    "raul", "reba", "reid", "rene", "rhea", "rhys", "rick",
    "rita", "riva", "ron", "rory", "rosa", "ross", "roy", "ruby", "rudy",
    "ruth", "ryan",

    "sally", "sam", "sami", "sana", "sandy", "sara", "sarah", "sasha",
    "saul", "scott", "sean", "selma", "seth", "shane", "shawn", "sid",
    "simon", "sofia", "sonia", "soren", "stan", "stella", "steve",
    "susan", "sven",

    "talia", "tami", "tania", "tara", "ted", "tess", "tessa", "theo",
    "tim", "tina", "toby", "todd", "tom", "toni", "tony",
    "trent", "troy", "trudy",

    "uma", "una",

    "vance", "veda", "vera", "vic", "vicki", "vince", "viola",

    "wade", "walt", "wanda", "wayne", "wendy", "will",

    "xavi", "xena",

    "yana", "yara", "yoko", "yuri",

    "zane", "zara", "zelda", "zoe", "zora",
]


def generate_team_pool(seed: Optional[int] = None) -> List[str]:
    """Generate team pseudonyms as "T" + 3-digit number.

    Returns 999 team names like "T001", "T042", "T196".
    """
    names = [f"T{i:03d}" for i in range(1, 1000)]

    rng = random.Random(seed)
    rng.shuffle(names)
    return names


def generate_name_pool(names: List[str]=PLANTIMALS, seed: Optional[int] = None) -> List[str]:
    """Generate run pseudonyms from PLANTIMALS + CVC fallback.

    Returns shuffled list: first names, then CVC names as fallback.
    Both lists are shuffled independently, then concatenated.
    """
    rng = random.Random(seed)

    # Shuffle names
    names_ = names.copy()
    rng.shuffle(names_)

    # Generate and shuffle CVC fallback names (exclude any that are in names)
    name_set = set(names)
    cvvc_names = [name for name in generate_cvvc_names() if name not in name_set]
    rng.shuffle(cvvc_names)

    # names first, then CVVC fallback
    return names_ + cvvc_names

def generate_plantimal_pool(seed: Optional[int] = None) -> List[str]:
    return generate_name_pool(names=PLANTIMALS, seed=seed)


def generate_baby_pool(seed: Optional[int] = None) -> List[str]:
    return generate_name_pool(names=BABY_NAMES, seed=seed)
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
        self._run_pool = generate_baby_pool(seed)
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
