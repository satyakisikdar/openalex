"""
Containers for different entities
"""

# TODO: add progress bars to all the stuff, optimize for credit allocation - cocited graphs
# TODO: re-create author institution timeline

from dataclasses import dataclass, field
from typing import Optional, List

import pandas as pd
import requests

# import src.index as indexer
from src.utils import Indices, get_rows, Paths, IDMap, get_partition_no


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

    def __post_init__(self):
        self.url = f'https://openalex.org/A{self.author_id}'
        return

    def populate_info(self, indices: Indices, paths: Paths):
        """
        Populates info like Name
        """
        kind = 'authors'
        part_no = get_partition_no(id_=self.author_id, kind=kind, ix_df=indices[kind])
        self.name = get_rows(id_=self.author_id, kind='authors', part_no=part_no, id_col='author_id',
                             paths=paths).author_name.values[0]
        return

    def get_all_author_works(self):
        """
        Get all work ids for an author
        TODO:

        make a works class and use methods to populate info
        """
        session = requests.Session()

        url = f'https://api.openalex.org/works?filter=author.id:A{self.author_id}'
        params = {'mailto': 'ssikdar@iu.edu', 'per-page': str(200)}
        session.headers.update(params)
        response = session.get(url, headers=session.headers, params=params)
        assert response.status_code == 200, f'Response code: {response.status_code} {url=}'
        data = response.json()
        works_count = data['meta']['count']
        num_pages = works_count // data['meta']['per_page'] + 1
        print(f'{self.author_id=} {self.name=} {works_count=:,} {num_pages=:,}')

        work_ids = [int(res['id'].replace('https://openalex.org/W', '')) for res in data['results']]
        if num_pages > 1:
            for page in range(2, num_pages + 1):
                new_url = url + f'&page={page}'
                response = session.get(new_url, headers=session.headers, params=params)
                assert response.status_code == 200, f'Response code: {response.status_code} {url=}'
                data = response.json()
                work_ids.extend(
                    [int(res['id'].replace('https://openalex.org/W', '')) for res in data['results']]
                )

        return work_ids


@dataclass
class Concept:
    concept_id: int
    score: Optional[float] = None
    name: Optional[str] = None
    level: Optional[int] = None
    url: Optional[str] = field(default=None, repr=False)
    tagged_works: Optional[list] = field(default_factory=lambda: [], repr=False)
    works_count: Optional[int] = None

    def __post_init__(self):
        self.url = f'https://openalex.org/C{self.concept_id}'
        if self.name is None:  # make an API call to fill out the details
            session = requests.Session()

            url = f'https://api.openalex.org/C{self.concept_id}'
            params = {'mailto': 'ssikdar@iu.edu'}
            session.headers.update(params)
            response = session.get(url, headers=session.headers, params=params)
            assert response.status_code == 200, f'Response code: {response.status_code} {url=}'
            data = response.json()
            self.name = data['display_name']
            self.level = data['level']
        return

    def populate_tagged_works(self, indices: Indices, paths: Paths):
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
            return

        concepts_work_rows = get_rows(id_=self.concept_id, id_col='concept_id', kind=kind, paths=paths, part_no=part_no)
        # rows have work ids and score
        concepts_work_rows = concepts_work_rows.sort_values(by='score', ascending=False)  # sort by score
        for row in concepts_work_rows.itertuples():
            self.tagged_works.append((row.work_id, row.score))

        self.works_count = len(self.tagged_works)  # update works count
        return


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
    part_no: Optional[int] = None
    type: Optional[str] = None
    doi: Optional[str] = None
    title: Optional[str] = None
    publication_year: Optional[int] = None
    publication_date: Optional[str] = None
    venue: Optional[Venue] = field(default=None, repr=False)
    abstract: Optional[str] = field(default=None, repr=False)
    abstract_inverted_index: Optional[str] = field(default=None, repr=False)
    authors: List[Author] = field(default_factory=lambda: [], repr=False)
    concepts: List[Concept] = field(default_factory=lambda: [], repr=False)
    citations: int = None  # number of citations
    references: set = field(default=None, repr=False)  # set of reference works
    citing_works: set = field(default=None, repr=False)  # set of citing works
    cocited_works: set = field(default=None, repr=False)  # set of co-cited works

    def __post_init__(self):
        self.url = f'https://openalex.org/W{self.work_id}'
        if self.citing_works is not None:
            self.citations = len(self.citing_works)
        ## TODO: replace populate_info with an API call?
        return

    def get_partition_info(self, kind: str, indices: Indices):

        if kind not in self.partitions_dict:
            part_no = get_partition_no(id_=self.work_id, kind=kind, ix_df=indices[kind])
            if part_no is None:  # not found
                # print(f'{self.work_id=} {kind!r} entry not found!')
                self.partitions_dict[kind] = None  # prevents future lookups
            else:
                self.partitions_dict[kind] = part_no
        return self.partitions_dict[kind]

    def load(self, ref_indexer=None, work_indexer=None, compute: bool = True):
        """
        Try to load the object from the indices
        If compute is True, compute and update the index
        """
        if work_indexer is not None:
            # work_indexer.read_offsets()
            work_offset = work_indexer.offsets.get(self.work_id, {}).get('offset')
            if work_offset is None and compute:
                work_indexer.process_entry(work_id=self.work_id)
                work_offset = work_indexer.offsets[self.work_id]['offset']
                # print(f'{self.work_id=} {work_offset=}')
            if work_offset is not None:
                # load the work info
                w = work_indexer.parse_bytes(offset=work_offset)
                for att in dir(w):
                    if att.startswith('_'):
                        continue
                    setattr(self, att, getattr(w, att))

        # try to load the references and citations
        if ref_indexer is not None:
            # ref_indexer.read_offsets()
            ref_offset = ref_indexer.offsets.get(self.work_id, {}).get('offset')

            if ref_offset is None and compute:
                ref_indexer.process_entry(work_id=self.work_id)
                ref_offset = ref_indexer.offsets[self.work_id]['offset']
                # print(f'{self.work_id=} {ref_offset=}')
            if ref_offset is not None:
                # load the ref index
                work_id, refs, cites = ref_indexer.parse_bytes(offset=ref_offset)
                assert work_id == self.work_id, f'Work ids in index does not match'

                self.references = refs
                self.citations = len(cites)
                self.citing_works = cites

        return

    def populate_info(self, indices: Indices):
        """
        Populate basic info
        """
        # print('Populating info')
        kind = 'works'
        part_no = self.get_partition_info(kind=kind, indices=indices)

        if part_no is None:
            return

        info = get_rows(id_=self.work_id, kind=kind, paths=self.paths, part_no=part_no).to_dict()
        for key, val in info.items():
            setattr(self, key, val[self.work_id])
        return

    def populate_venue(self, indices: Indices, id_map: IDMap):
        """
        Populate host venue
        """
        # print('Populating venue')
        kind = 'works_host_venues'
        part_no = self.get_partition_info(kind=kind, indices=indices)

        if part_no is None:
            return

        venue_row = get_rows(id_=self.work_id, kind=kind, paths=self.paths, part_no=part_no)

        venue_id = venue_row.venue_id.values[0]
        self.venue = Venue(venue_id=venue_id, name=id_map.venue_id2name[venue_id])
        return

    def construct_abstract(self):
        """
        Construct abstract if possible
        :return:
        """
        pass

    def populate_authors(self, indices: Indices):
        """
        Add list of authors
        """
        # print('Getting authorship data')
        kind = 'works_authorships'

        part_no = self.get_partition_info(kind=kind, indices=indices)

        if part_no is None:
            return

        authors_info = get_rows(id_=self.work_id, kind=kind, paths=self.paths, part_no=part_no)

        if authors_info is None:
            return

        authors_info.loc[:, 'author_position'] = pd.Categorical(authors_info.author_position,
                                                                categories=['first', 'middle', 'last'],
                                                                ordered=True)
        authors_info = authors_info.sort_values(by='author_position')

        # check for multiple affils
        authors_dict = {}

        for row in authors_info.itertuples():
            if pd.isna(row.institution_id):
                inst = None
            else:
                inst = Institution(institution_id=int(row.institution_id), name=row.institution_name)

            if row.author_id in authors_dict:  # repeated author, multi affils
                author = authors_dict[row.author_id]
                author.insts.append(inst)
            else:
                author = Author(author_id=row.author_id, position=row.author_position)
                author.populate_info(indices=indices, paths=self.paths)
                author.insts.append(inst)
                authors_dict[row.author_id] = author
        self.authors = list(authors_dict.values())
        return

    def populate_concepts(self, indices: Indices, id_map: IDMap):
        """
        Add list of concepts
        """
        # print('Getting Concepts info')
        kind = 'works_concepts'

        part_no = self.get_partition_info(kind=kind, indices=indices)
        # print(f'{part_no=}, {type(part_no)=}')
        if part_no is None:
            return

        concepts_rows = get_rows(id_=self.work_id, kind=kind, paths=self.paths, part_no=part_no)
        if concepts_rows is None:
            return

        concepts = []
        for row in concepts_rows.itertuples():
            concept = Concept(concept_id=row.concept_id, name=id_map.concept_id2name[row.concept_id],
                              score=row.score, level=id_map.concept_id2level[row.concept_id])
            concepts.append(concept)
        concepts = sorted(concepts, key=lambda c: c.score, reverse=True)
        self.concepts = concepts
        return

    def populate_citations(self, indices: Indices):
        """
        Add reference works
        """
        kind = 'works_citing_works'

        part_no = self.get_partition_info(kind=kind, indices=indices)
        if part_no is None:
            self.citations = 0
            self.citing_works = set()
            return

        cites_rows = get_rows(id_=self.work_id, kind=kind, id_col='referenced_work_id', part_no=part_no,
                              paths=self.paths)

        self.citing_works = set(cites_rows.work_id)
        self.citations = len(self.citing_works)
        return

    def populate_references(self, indices: Indices):
        """
        add references
        """
        kind = 'works_referenced_works'
        part_no = self.get_partition_info(kind=kind, indices=indices)
        if part_no is None:
            self.references = set()
            return
        refs_rows = get_rows(id_=self.work_id, kind=kind, part_no=part_no, paths=self.paths)

        self.references = set(refs_rows.referenced_work_id)
        return

    def populate_cocitations(self, indices: Indices, work_indexer, ref_indexer):
        """
        Return the co-cited works -- D = set of papers citing the paper,
        cocited papers = set of papers cited by papers in D

        currently too slow -- optimize references call -- build a graph?
        """
        return  # cocited_papers
