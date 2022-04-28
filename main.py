import sys

sys.path.extend(['../', './'])
from tqdm.auto import tqdm

from src.entities import Works
from src.utils import Paths, ParquetIndices, IDMap, combined_dump_work_refs
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


def index_works_parallel(work_ids, num_workers, work_indexer, ref_indexer):
    """
    index work ids in parallel
    """
    work_indexer.update_index_from_dump()  # update the data and offset files
    ref_indexer.update_index_from_dump()  # update the data and offset files

    if len(work_ids) < 20 or num_workers == 1:  # dont bother parallelizing
        for work_id in tqdm(work_ids):
            combined_dump_work_refs(work_id, work_indexer, ref_indexer)
    else:
        with parallel_backend('threading', n_jobs=num_workers):
            Parallel()(
                delayed(combined_dump_work_refs)(work_id, work_indexer, ref_indexer) for work_id in tqdm(work_ids,
                                                                                                         colour='green',
                                                                                                         desc='Refs')
            )
    work_indexer.update_index_from_dump()  # update the data and offset files
    ref_indexer.update_index_from_dump()  # update the data and offset files
    return


def index_concept_tagged_works(num_workers, concept_id):
    paths = Paths()
    indices = ParquetIndices(paths=paths)
    indices.initialize()

    id_map = IDMap(paths=paths)
    concept_indexer = ConceptIndexer(paths=paths, indices=indices)
    work_indexer = WorkIndexer(paths=paths, indices=indices, id_map=id_map)
    ref_indexer = RefIndexer(paths=paths, indices=indices)

    concept = concept_indexer.parse_bytes(offset=concept_indexer.offsets[concept_id]['offset'])
    print(f'{concept=}')

    work_ids = [w for w, _ in concept.tagged_works]  # if w not in ref_indexer or w not in work_indexer]
    print(f'{len(work_ids)=:,} to index')

    index_works_parallel(work_ids=work_ids, num_workers=num_workers, work_indexer=work_indexer, ref_indexer=ref_indexer)

    print(f'Now processing references and citations')
    for work_id in tqdm(work_ids, colour='red', desc='Work ID'):
        index_references_and_citations(num_workers=num_workers, work_id=work_id,
                                       work_indexer=work_indexer, ref_indexer=ref_indexer)
    return


def index_references_and_citations(num_workers, work_id, work_indexer, ref_indexer):
    """
    Index all references and citations for a work id in parallel
    """
    if work_id not in ref_indexer:
        combined_dump_work_refs(work_id=work_id, work_indexer=work_indexer, ref_indexer=ref_indexer)
        work_indexer.update_index_from_dump()  # update the data and offset files
        ref_indexer.update_index_from_dump()  # update the data and offset files

        work_id, refs, cites = ref_indexer[work_id]

    work_id, refs, cites = ref_indexer[work_id]
    work_ids = refs | cites
    work_ids = [w for w in work_ids if w not in ref_indexer or w not in work_indexer]

    if len(work_ids) > 0:
        print(f'{work_id=} {len(work_ids)=:,}')
        index_works_parallel(work_ids=work_ids, num_workers=num_workers, work_indexer=work_indexer,
                             ref_indexer=ref_indexer)
    return


def main():
    # parse(num_workers=6)
    index_concept_tagged_works(num_workers=18, concept_id=34947359)
    return


if __name__ == '__main__':
    main()
