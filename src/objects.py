"""
Containers for different entities
"""

# TODO: add progress bars to all the stuff, optimize for credit allocation - cocited graphs
# TODO: re-create author institution timeline

from dataclasses import dataclass, field
from typing import Optional, List

import orjson
import orjson as json
import requests
from tqdm.auto import tqdm

from src.utils import ParquetIndices, get_rows, Paths, get_partition_no, convert_openalex_id_to_int, \
    clean_string, reconstruct_abstract_new


def process_json(work_json, work_indexer, id_map):
    work_cols = ['id', 'doi', 'title', 'publication_year', 'publication_date', 'type',
                 'cited_by_count', 'is_retracted', 'is_paratext', 'abstract_inverted_index', 'updated_date']

    work_row = {col: work_json.get(col, '') for col in work_cols}

    if work_row['is_retracted'] or work_row['is_paratext']:
        return None

    work_id = convert_openalex_id_to_int(work_row['id'])
    if work_id in work_indexer:
        return None
    #     return work_indexer[work_id]

    work = Work(work_id=work_id)

    work.publication_date = work_row['publication_date']
    work.publication_year = work_row['publication_year']
    work.title = clean_string(work_row['title'])
    work.doi = work_row['doi']
    work.type = work_row['type']
    work.cited_by_count = work_row['cited_by_count']
    work.updated_date = work_row['updated_date']

    try:
        if (abstract := work_json.get('abstract_inverted_index')) is not None:
            work_row['abstract_inverted_index'] = json.dumps(abstract)

        work.abstract = reconstruct_abstract_new(work_row['abstract_inverted_index'])
    except orjson.JSONDecodeError as e:
        print('JSON decode error abstract')
        work.abstract = ''

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
                Concept(concept_id=int(concept_id),
                        name=id_map.concept_id2name[concept_id],
                        score=float(concept.get('score')),
                        level=id_map.concept_id2level[concept_id])
            )

    # referenced_works
    for referenced_work in work_json.get('referenced_works'):
        if referenced_work:
            work.references.add(convert_openalex_id_to_int(referenced_work))

    # related_works
    for related_work in work_json.get('related_works'):
        if related_work:
            work.related_works.add(convert_openalex_id_to_int(related_work))
    return work


@dataclass
class Institution:
    institution_id: int
    name: str
    url: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        self.url = f'https://openalex.org/I{self.institution_id}'
        return


@dataclass
class Author:
    author_id: int
    name: Optional[str] = field(default=None)
    position: Optional[str] = None
    insts: List[Institution] = field(default_factory=lambda: [])
    url: Optional[str] = field(default=None, repr=False)
    work_ids: Optional[list] = field(default_factory=lambda: [], repr=False)

    def __post_init__(self):
        self.url = f'https://openalex.org/A{self.author_id}'
        return

    def populate_info(self, indices: ParquetIndices, paths: Paths):
        """
        Populates info like Name
        """
        kind = 'authors'
        part_no = get_partition_no(id_=self.author_id, kind=kind, ix_df=indices[kind])
        self.name = get_rows(id_=self.author_id, kind='authors', part_no=part_no, id_col='author_id',
                             paths=paths).author_name.values[0]
        return

    def parse_all_author_works(self, id_map, work_indexer):
        """
        Get all work ids for an author
        """
        email = 'as@nd.edu'

        with requests.Session() as session:
            url = f'https://api.openalex.org/works?filter=author.id:A{self.author_id}'
            params = {'mailto': email, 'per-page': '200'}
            session.headers.update(params)
            response = session.get(url, headers=session.headers, params=params)
            assert response.status_code == 200, f'Response code: {response.status_code} {url=}'
            data = response.json()

            works_count = data['meta']['count']
            num_pages = works_count // data['meta']['per_page'] + 1
            # print(f'{self.author_id=} {self.name=} {works_count=:,} {num_pages=:,}')
            work_jsons = data['results']

            if num_pages > 1:
                for page in range(2, num_pages + 1):
                    new_url = url + f'&page={page}'
                    response = session.get(new_url, headers=session.headers, params=params)
                    assert response.status_code == 200, f'Response code: {response.status_code} {url=}'
                    data = response.json()
                    work_jsons.extend(data['results'])
                    # for work_json in tqdm(data['results']):
                    #     process_json(id_map=id_map, work_indexer=work_indexer, work_json=work_json)

                    # work_ids.extend(
                    #     [int(res['id'].replace('https://openalex.org/W', '')) for res in data['results']]
                    # )

            for work_json in tqdm(work_jsons, leave=False):
                work_id = convert_openalex_id_to_int(work_json['id'])
                if work_id in work_indexer:
                    self.work_ids.append(work_id)
                else:
                    work = process_json(id_map=id_map, work_indexer=work_indexer, work_json=work_json)
                    if work is None:
                        continue
                    try:
                        bites = work_indexer.convert_to_bytes(work)
                        work_indexer.dump_bytes(work_id=work.work_id, bites=bites)
                    except Exception as e:
                        print(f'Exception {e=} for {work.work_id=}')

        return


@dataclass
class Concept:
    concept_id: int
    score: Optional[float] = None
    name: Optional[str] = None
    level: Optional[int] = None
    url: Optional[str] = field(default=None, repr=False)
    tagged_works: Optional[list] = field(default_factory=lambda: [], repr=False)
    works_count: Optional[int] = None
    related_concepts: Optional[list] = field(default_factory=lambda: [], repr=False)
    ancestors: Optional[list] = field(default_factory=lambda: [], repr=False)

    def parse_works(self, work_indexer, id_map):
        email = 'as@nd.edu'

        with requests.Session() as session:
            url = f'https://api.openalex.org/works?filter=concepts.id:C{self.concept_id}'
            params = {'mailto': email, 'per-page': '200'}
            session.headers.update(params)
            response = session.get(url, headers=session.headers, params=params)
            assert response.status_code == 200, f'Response code: {response.status_code} {url=}'
            data = response.json()

            works_count = data['meta']['count']
            num_pages = works_count // data['meta']['per_page'] + 1
            # print(f'{self.author_id=} {self.name=} {works_count=:,} {num_pages=:,}')
            work_jsons = data['results']

            if num_pages > 1:
                for page in range(2, num_pages + 1):
                    new_url = url + f'&page={page}'
                    response = session.get(new_url, headers=session.headers, params=params)
                    assert response.status_code == 200, f'Response code: {response.status_code} {url=}'
                    data = response.json()
                    work_jsons.extend(data['results'])

            for work_json in tqdm(work_jsons, leave=False):
                work_id = convert_openalex_id_to_int(work_json['id'])
                if work_id in work_indexer:
                    self.work_ids.append(work_id)
                else:
                    work = process_json(id_map=id_map, work_indexer=work_indexer, work_json=work_json)
                    if work is None:
                        continue
                    try:
                        bites = work_indexer.convert_to_bytes(work)
                        work_indexer.dump_bytes(work_id=work.work_id, bites=bites)
                    except Exception as e:
                        print(f'Exception {e=} for {work.work_id=}')

        return

    def populate_related_and_ancestor_concepts(self):
        session = requests.Session()
        url = f'https://api.openalex.org/C{self.concept_id}'
        params = {'mailto': 'ssikdar@iu.edu'}
        session.headers.update(params)
        response = session.get(url, headers=session.headers, params=params)
        assert response.status_code == 200, f'Response code: {response.status_code} {url=}'
        data = response.json()
        session.close()

        print(f'{len(data["related_concepts"])} related concepts')
        for related_concept_data in tqdm(data['related_concepts']):
            id_ = convert_openalex_id_to_int(related_concept_data['id'])
            c = Concept(concept_id=id_, score=related_concept_data['score'], level=related_concept_data['level'],
                        name=related_concept_data['display_name'])
            self.related_concepts.append(c)

        print(f'{len(data["ancestors"])} ancestors')
        for anc_concept_data in tqdm(data['ancestors']):
            id_ = convert_openalex_id_to_int(anc_concept_data['id'])
            c = Concept(concept_id=id_, level=anc_concept_data['level'],
                        name=anc_concept_data['display_name'])
            self.ancestors.append(c)
        return

    def __post_init__(self):
        self.url = f'https://openalex.org/C{self.concept_id}'
        if self.name is None:  # or self.works_count is None or self.related_concepts is None:
            # print('Making API call')
            session = requests.Session()

            url = f'https://api.openalex.org/C{self.concept_id}'
            params = {'mailto': 'ssikdar@iu.edu'}
            session.headers.update(params)
            response = session.get(url, headers=session.headers, params=params)
            assert response.status_code == 200, f'Response code: {response.status_code} {url=}'
            data = response.json()
            self.name = data['display_name']
            self.level = data['level']
            self.works_count = data['works_count']
            session.close()
        return

    def populate_tagged_works(self, indices: ParquetIndices, paths: Paths):
        """
        Return the set of work ids tagged with the concept
        """
        if len(self.tagged_works) > 0:
            print('Already tagged')
            return

        print(f'Getting works tagged with concept: {self.name!r}')
        kind = 'concepts_works'

        part_no = get_partition_no(id_=self.concept_id, kind=kind, ix_df=indices[kind])

        if part_no is None:
            print('Part no not found')
            return

        concepts_work_rows = get_rows(id_=self.concept_id, id_col='concept_id', kind=kind, paths=paths, part_no=part_no)
        # rows have work ids and score
        concepts_work_rows = concepts_work_rows.sort_values(by='score', ascending=False)  # sort by score
        for row in concepts_work_rows.itertuples():
            self.tagged_works.append((row.work_id, row.score))

        self.works_count = len(self.tagged_works)  # update works count
        return

    def get_tagged_works(self, concept_indexer) -> set:
        """
        use the concept indexer to get the set of works tagged with the concept
        If not indexed, add it to the concept index
        """
        work_ids = set()
        if self.concept_id in concept_indexer:
            pass
        raise NotImplementedError()

        return work_ids


@dataclass
class Venue:
    venue_id: int
    name: str
    url: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        self.url = f'https://openalex.org/V{self.venue_id}'
        return


@dataclass
class Work:
    work_id: int
    paths: Paths = field(repr=False, default=None)
    partitions_dict: dict = field(default_factory=lambda: {}, repr=False)  # partition indices

    url: Optional[str] = field(default=None, repr=False)
    type: Optional[str] = None
    doi: Optional[str] = None
    title: Optional[str] = None
    publication_year: Optional[int] = None
    publication_date: Optional[str] = None
    updated_date: Optional[str] = None
    venue: Optional[Venue] = field(default=None, repr=False)
    abstract: Optional[str] = field(default=None, repr=False)
    abstract_inverted_index: Optional[str] = field(default=None, repr=False)
    authors: List[Author] = field(default_factory=lambda: [], repr=False)
    concepts: List[Concept] = field(default_factory=lambda: [], repr=False)
    cited_by_count: int = None  # number of cited_by_count
    references: set = field(default_factory=lambda: set(), repr=False)  # set of references works
    citing_works: set = field(default=None, repr=False)  # set of citing works
    cocited_works: set = field(default=None, repr=False)  # set of co-cited works
    related_works: set = field(default_factory=lambda: set(), repr=False)  # set of related works
