import gzip
import sys

import orjson as json

from src.objects import Work, Venue, Institution, Author, Concept

sys.path.extend(['../', './'])
from tqdm.auto import tqdm

from src.entities import Works
from src.utils import Paths, ParquetIndices, IDMap, combined_dump_work_refs, process_and_dump_references, \
    convert_openalex_id_to_int, clean_string, reconstruct_abstract, read_manifest
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


def process_json(work_json, work_indexer, id_map):
    work_cols = ['id', 'doi', 'title', 'publication_year', 'publication_date', 'type',
                 'cited_by_count', 'is_retracted', 'is_paratext', 'abstract_inverted_index', 'updated_date']

    if (abstract := work_json.get('abstract_inverted_index')) is not None:
        work_json['abstract_inverted_index'] = json.dumps(abstract)

    work_row = {col: work_json.get(col, '') for col in work_cols}

    if work_row['is_retracted'] or work_row['is_paratext']:
        return None

    work_id = convert_openalex_id_to_int(work_row['id'])
    if work_id in work_indexer:
        return None

    work = Work(work_id=work_id)

    work.publication_date = work_row['publication_date']
    work.publication_year = work_row['publication_year']
    work.title = clean_string(work_row['title'])
    work.doi = work_row['doi']
    work.type = work_row['type']
    work.citations = work_row['cited_by_count']
    work.abstract = clean_string(reconstruct_abstract(work_row['abstract_inverted_index']))

    # host venues
    if host_venue := (work_json.get('host_venue') or {}):
        if host_venue_id := host_venue.get('id'):
            venue_id = convert_openalex_id_to_int(host_venue_id)
            venue_name = clean_string(id_map.venue_id2name[venue_id])
            venue = Venue(venue_id=venue_id, name=venue_name)
        else:
            venue = None
    else:
        venue = None
    work.venue = venue
    work.authors = []
    # authorships
    # sort them by order
    if authorships := work_json.get('authorships'):
        for authorship in authorships:
            if author_id := authorship.get('author', {}).get('id'):
                author_id = convert_openalex_id_to_int(author_id)
                author_name = clean_string(authorship['author']['display_name'])

                institutions = authorship.get('institutions')
                institution_ids = [convert_openalex_id_to_int(i.get('id')) for i in institutions]
                institution_ids = [i for i in institution_ids if i]

                if len(institution_ids) > 0:
                    institutions = [Institution(institution_id=inst_id, name=clean_string(id_map.inst_id2name[inst_id]))
                                    for inst_id in institution_ids]
                else:
                    institutions = [None]
                author = Author(author_id=author_id, name=author_name, position=authorship.get('author_position', ''))
                author.insts = institutions
                work.authors.append(author)

    # concepts
    work.concepts = []
    for concept in work_json.get('concepts'):
        if concept_id := concept.get('id'):
            concept_id = convert_openalex_id_to_int(concept_id)
            work.concepts.append(
                Concept(concept_id=concept_id,
                        name=id_map.concept_id2name[concept_id],
                        score=concept.get('score'),
                        level=id_map.concept_id2level[concept_id])
            )

    #         # referenced_works
    #         for referenced_work in work_json.get('referenced_works'):
    #             if referenced_work:
    #                 ref_rows.append({
    #                     'work_id': work_id,
    #                     'referenced_work_id': referenced_work
    #                 })

    #         # related_works
    #         for related_work in work_json.get('related_works'):
    #             if related_work:
    #                 rel_rows.append({
    #                     'work_id': work_id,
    #                     'related_work_id': related_work
    #                 })
    return work


def process_jsonl_file(jsonl_file, work_indexer, id_map):
    ## create work objects directly
    with gzip.open(jsonl_file) as fp:
        lines = fp.readlines()

    work_indexer.update_index_from_dump()

    for i, line in tqdm(enumerate(lines), total=len(lines)):
        data = json.loads(line)
        work = process_json(data, work_indexer=work_indexer, id_map=id_map)
        if work is None:
            continue
        bites = work_indexer.convert_to_bytes(work)
        work_indexer.write_index(bites=bites, id_=work.work_id)

    work_indexer.update_index_from_dump()
    del lines


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

    if len(work_ids) < 25 or num_workers == 1:  # dont bother parallelizing
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
    concept_indexer = ConceptIndexer(paths=paths, indices=indices)
    work_indexer = WorkIndexer(paths=paths, indices=indices, id_map=id_map)
    ref_indexer = RefIndexer(paths=paths, indices=indices)

    concept = concept_indexer.parse_bytes(offset=concept_indexer.offsets[concept_id]['offset'])
    print(f'{concept=}')

    work_ids = [w for w, _ in concept.tagged_works]  # if w not in ref_indexer or w not in work_indexer]
    print(f'{len(work_ids)=:,} to index')

    # index_works_parallel(work_ids=work_ids, num_workers=num_workers, work_indexer=work_indexer, ref_indexer=ref_indexer)

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
        process_and_dump_references(work_id=work_id, ref_indexer=ref_indexer)
        ref_indexer.update_index_from_dump()  # update the data and offset files
        work_id, refs, cites = ref_indexer[work_id]

    work_id, refs, cites = ref_indexer[work_id]
    work_ids = refs | cites
    work_ids = [w for w in work_ids if w not in work_indexer or w not in ref_indexer]

    if len(work_ids) > 0:
        print(f'{work_id=} {len(work_ids)=:,}')
        index_works_parallel(work_ids=work_ids, num_workers=num_workers, work_indexer=work_indexer,
                             ref_indexer=ref_indexer)
    return


def main():
    # parse(num_workers=6)
    # index_concept_tagged_works(num_workers=25, concept_id=34947359)
    parse_works_v2()
    return


if __name__ == '__main__':
    main()
