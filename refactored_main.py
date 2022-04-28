import sys

from rich import print
from tqdm.auto import tqdm

tqdm.pandas()

sys.path.extend(['./', '../', '../../', '../../../'])
from src.utils import Paths, ParquetIndices, IDMap
from src.objects import Work

if __name__ == '__main__':
    paths = Paths()
    indices = ParquetIndices(paths=paths)
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

