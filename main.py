import sys

sys.path.extend(['../', './'])
from tqdm.auto import tqdm

from src.entities import Works
from src.utils import Paths, Indices, IDMap, combined_dump_work_refs
from src.index import WorkIndexer, ConceptIndexer, RefIndexer
from joblib import Parallel, delayed, parallel_backend


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
    id_map = IDMap(paths=paths)
    concept_indexer = ConceptIndexer(paths=paths, indices=indices)
    work_indexer = WorkIndexer(paths=paths, indices=indices, id_map=id_map)
    ref_indexer = RefIndexer(paths=paths, indices=indices)

    complex_network = 34947359
    concept = concept_indexer.parse_bytes(offset=concept_indexer.offsets[complex_network]['offset'])
    print(f'{concept=}')

    work_ids = [w for w, _ in concept.tagged_works]
    print(f'{len(work_ids)=:,}')

    work_indexer.update_index_from_dump()  # update the data and offset files
    ref_indexer.update_index_from_dump()  # update the data and offset files

    with parallel_backend('threading', n_jobs=num_workers):
        Parallel()(
            delayed(combined_dump_work_refs)(work_id, work_indexer, ref_indexer) for work_id in tqdm(work_ids)
        )
    work_indexer.update_index_from_dump()  # update the data and offset files
    ref_indexer.update_index_from_dump()  # update the data and offset files
    return


#     for work_id in tqdm(work_ids):
#         try:
#             w = Work(work_id=work_id, paths=paths)
#             w.load(ref_indexer=ref_indexer, work_indexer=work_indexer)

#         except Exception as e:
#             print(f'{work_id=} {e=}')
#     return
#


def main():
    # parse(num_workers=6)
    index_refs(num_workers=15)
    return


if __name__ == '__main__':
    main()
