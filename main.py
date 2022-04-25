import sys

sys.path.extend(['../', './'])
from tqdm.auto import tqdm

from src.entities import Works
from src.utils import Paths, Indices, IDMap
from src.index import WorkIndexer


def parse(num_workers):
    # TODO: double check files for each entity in all the directories - recompute the stuff that's needed
    paths = Paths()
    things = Works(paths=paths)
    # things = Authors(paths=paths)
    print(things)
    things.validate_tables(delete=False, start=200)
    things.process(num_workers=num_workers)
    # things.compute_missing_tables()
    return


def index_refs(num_workers):
    paths = Paths()
    indices = Indices(paths=paths)
    indices = Indices(paths=paths)
    id_map = IDMap(paths=paths)

    work_ids = indices['works'].index
    
    # ref_index = RefIndexer(paths=paths, indices=indices)
    # for work_id in tqdm(work_ids):
    # ref_index.process_entry(work_id=work_id)
    # return

    work_indexer = WorkIndexer(paths=paths, indices=indices, id_map=id_map)
    for work_id in tqdm(work_ids):
        work_indexer.process_entry(work_id=work_id)
    return
    #
    # with parallel_backend('threading', n_jobs=num_workers):
    #     Parallel()(
    #         delayed(ref_index.process_entry)(work_id) for work_id in tqdm(work_ids)
    #     )


def main():
    # parse(num_workers=6)
    index_refs(num_workers=10)
    return


if __name__ == '__main__':
    main()
