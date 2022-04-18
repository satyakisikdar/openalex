import pandas as pd
from pathlib import Path
import sys
import gc
from tqdm.auto import tqdm
import re
from string import punctuation
from rich import print

tqdm.pandas()

sys.path.extend(['./', '../', '../../', '../../../'])
from src.utils import Paths, Indices, IDMap
from src.objects import Work, Author, Venue

if __name__ == '__main__':
    paths = Paths()
    indices = Indices(paths=paths)
    id_map = IDMap(paths=paths)
    vrg = 3003512226  # VRG paper
    avrg = 3207443205
    gn = 2095293504
    work = Work(work_id=vrg, paths=paths)
    work.populate_info(indices=indices)
    work.populate_authors(indices=indices)
    work.populate_venue(indices=indices, id_map=id_map)
    work.populate_concepts(indices=indices, id_map=id_map)
    work.populate_references(indices=indices)
    print(work)

