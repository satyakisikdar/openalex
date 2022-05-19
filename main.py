import gzip
import sys

import orjson as json

from src.objects import Concept

sys.path.extend(['../', './'])
from tqdm.auto import tqdm

from src.entities import Works
from src.utils import Paths, ParquetIndices, IDMap, combined_dump_work_refs, process_and_dump_references, \
    read_manifest
from src.index import WorkIndexer, RefIndexer
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



def process_jsonl_file(jsonl_file, work_indexer, id_map):
    ## create work objects directly
    with gzip.open(jsonl_file) as fp:
        lines = fp.readlines()

    work_indexer.update_index_from_dump()

    for i, line in tqdm(enumerate(lines), total=len(lines), leave=False):
        data = json.loads(line)
        work = process_json(data, work_indexer=work_indexer, id_map=id_map)
        if work is None:
            continue
        bites = work_indexer.convert_to_bytes(work)
        work_indexer.write_index(bites=bites, id_=work.work_id)

    work_indexer.update_index_from_dump()
    return


def parse_works_v2():
    paths = Paths()
    indices = ParquetIndices(paths=paths)
    id_map = IDMap(paths=paths)
    work_indexer = WorkIndexer(paths=paths, indices=indices, id_map=id_map)
    works_manifest = read_manifest(kind='works', paths=paths)

    for entry in tqdm(works_manifest.entries):
        jsonl_file = entry.filename
        process_jsonl_file(jsonl_file, work_indexer=work_indexer, id_map=id_map)
    return


def index_works_parallel(work_ids, num_workers, work_indexer, ref_indexer):
    """
    index work ids in parallel
    """
    work_indexer.update_index_from_dump()  # update the data and offset files
    ref_indexer.update_index_from_dump()  # update the data and offset files

    work_ids = list(work_ids)

    if len(work_ids) <= 25 or num_workers == 1:  # dont bother parallelizing
        for work_id in tqdm(work_ids):
            combined_dump_work_refs(work_id, work_indexer, ref_indexer)
    else:
        with parallel_backend(backend='threading', n_jobs=num_workers):
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
    work_indexer = WorkIndexer(paths=paths, indices=indices, id_map=id_map)
    ref_indexer = RefIndexer(paths=paths, indices=indices)
    concept = Concept(concept_id=concept_id)
    concept.populate_tagged_works(indices=indices, paths=paths)
    # concept = concept_indexer.parse_bytes(offset=concept_indexer.offsets[concept_id]['offset'])

    print(f'{concept=}')

    work_ids = [w for w, _ in concept.tagged_works]  # if w not in ref_indexer or w not in work_indexer]
    print(f'{len(work_ids)=:,} to index')

    # index_works_parallel(work_ids=work_ids, num_workers=num_workers, work_indexer=work_indexer, ref_indexer=ref_indexer)
    i = 0
    print(f'Now processing references and citations')
    for work_id in tqdm(work_ids, colour='red', desc='Work ID'):
        combined_dump_work_refs(work_id=work_id, ref_indexer=ref_indexer, work_indexer=work_indexer)

        i += 1
        if i > 0 and i % 50 == 0:
            work_indexer.update_index_from_dump()
        # index_references_and_citations(num_workers=num_workers, work_id=work_id,
        #                                work_indexer=work_indexer, ref_indexer=ref_indexer)
    return


def index_references_and_citations(num_workers, work_id, work_indexer, ref_indexer):
    """
    Index all references and citations for a work id in parallel
    """
    if work_id not in ref_indexer:
        process_and_dump_references(work_id=work_id, ref_indexer=ref_indexer)
        ref_indexer.update_index_from_dump()  # update the data and offset files
        work_id, refs, cites = ref_indexer[work_id]

    work_id, refs, cites = ref_indexer[work_id]
    work_ids = refs | cites
    work_ids = [w for w in work_ids if w not in work_indexer]  # or w not in ref_indexer]

    if len(work_ids) > 0:
        print(f'{work_id=} {len(work_ids)=:,}')
        index_works_parallel(work_ids=work_ids, num_workers=num_workers, work_indexer=work_indexer,
                             ref_indexer=ref_indexer)
    return


def main():
    # parse(num_workers=6)
    # feshbach 39190425, neutrino oscillation: 107966497, soliton: 87651913
    index_concept_tagged_works(num_workers=25, concept_id=39190425)
    index_concept_tagged_works(num_workers=25, concept_id=107966497)
    index_concept_tagged_works(num_workers=25, concept_id=87651913)
    # parse_works_v2()
    return


if __name__ == '__main__':
    main()
