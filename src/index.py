"""
For all the indexing stuff
Do it for concepts, authorships, works

Works: workid, type, title, venue id, date, year, citations, references, num_authors
Works Authors: workid, num authors, author1, num_inst1, [inst1, inst2], author2, inst1, ...]
Works Concepts: workid, num concepts, [concept1, score1, concept2, score2, ... ]
Concept Works: concept_id,  num_works [work1, score1, work2, score2, ... ]
"""
import abc
import os

import pandas as pd
from tqdm.auto import tqdm

import src.objects as objects
from src.utils import Paths, IDMap, reconstruct_abstract, clean_string


class BaseIndexer:
    """
    Base class for indexers
    """

    def __init__(self, paths: Paths, indices, kind: str):
        import src.encoder as encoder
        self.paths = paths
        self.indices = indices
        self.kind = kind
        self.index_path = self.paths.compressed_path / kind
        if not self.index_path.exists():
            os.makedirs(self.index_path)

        self.offset_path = self.index_path / 'offsets.txt'
        self.data_path = self.index_path / 'data.txt'
        self.dump_path = self.index_path / 'dumps.txt'  # store the dumps of byte representations

        self.offsets = self.read_offsets()

        self.encoder = encoder.EncoderDecoder()
        self.decoder = encoder.EncoderDecoder()  # just me being lazy

        self.ref_indexer = None  # None except in Work Indexer class
        return

    def read_offsets(self) -> pd.DataFrame:
        """
        Read offsets from a file
        :return:
        """
        if not self.offset_path.exists():
            with open(self.offset_path, 'w') as fp:
                fp.write('work_id,offset,len\n')

        return (
            pd.read_csv(self.offset_path, index_col=0, engine='c')
            .drop_duplicates()
            .to_dict('index')
        )

    def update_index_from_dump(self):
        """
        Read the dumps.txt file and update the index
        """
        if not self.dump_path.exists():
            return

        updates = 0
        with open(self.dump_path, 'rb') as reader:
            while True:
                bite = reader.read(1)
                if not bite:
                    break

                work_id = self.decoder.decode_long_long_int(reader)  # the # sign is already read
                # print(f'Reading {work_id=} from dumps')
                len_ = self.decoder.decode_long_int(reader)
                work_bytes = reader.read(len_)  # read the bytes

                if work_id not in self.offsets:
                    # print(f'Writing new {work_id=} to offsets')
                    self.write_index(bites=work_bytes, id_=work_id)
                    updates += 1

        if updates > 0:
            print(f'{updates:,} new entries added from dump')

        # clear out the dumps file
        writer = open(self.dump_path, 'wb')
        writer.close()
        return

    def write_index(self, bites: bytes, id_: int):
        """
        Update the offsets dictionary
        Write the id, offset, and len(bites) into offsets file
        Write the bites to the data file
        """
        if len(self.offsets) == 0:
            previous_offset = 0
            previous_len = 0
        else:
            # possible bug here..
            last_key = list(self.offsets.keys())[-1]
            previous_offset, previous_len = self.offsets[last_key]['offset'], self.offsets[last_key]['len']

        offset = previous_offset + previous_len
        self.offsets[id_] = {'offset': offset, 'len': len(bites)}

        with open(self.offset_path, 'a') as offset_writer, open(self.data_path, 'ab') as data_writer:
            offset_writer.write(f'{id_},{offset},{len(bites)}\n')  # write the offset
            data_writer.write(bites)  # write the bytes
        return

    def __contains__(self, item):
        return item in self.offsets

    def __getitem__(self, item):
        """
        Use square brackets to get object using work id / concept id
        """
        if item in self.offsets:
            entry = self.parse_bytes(offset=self.offsets[item]['offset'])

            ## commented for now
            # if self.kind == 'works':  # get the references
            #     work_id, refs, cites = self.ref_indexer[item]  # get the entry from references index
            #     assert work_id == item, f'ID mismatch: {item} != {work_id}'
            #
            #     entry.references = refs
            #     entry.citations = len(cites)
            #     entry.citing_works = cites
        else:
            # process the entry and write the offset
            # print(f'Computing {self.kind!r} index for {item}')
            entry = self.process_entry(item, write=False)  # dont write to the index just yet

            # if self.kind == 'works':
            #     ## also process the references  # TODO: test
            #     work_id, refs, cites = self.ref_indexer.process_entry(item, write=True)  # process the references
            #     entry.references = refs
            #     entry.citations = len(cites)
            #     entry.citing_works = cites
        return entry

    @abc.abstractmethod
    def convert_to_bytes(self, entity) -> bytes:
        pass

    @abc.abstractmethod
    def process_entry(self, id_: int, write: bool):
        pass

    @abc.abstractmethod
    def parse_bytes(self, offset: int, reader=None):
        pass

    def validate_and_fix_index(self, fix: bool = True, start=0):
        """
        Validate the index by matching the work id / concept id from the extracted object with that of the offset file
        """

        # TODO: fix bug when the last entry is corrupted. The data file needs to be updated
        # TODO: otherwise the indices will not work

        errors = []
        self.offsets = self.read_offsets()
        ids = list(self.offsets.keys())[start:]
        if start != 0:
            print(f'Starting at {start=:,}')

        for id_ in tqdm(ids):

            offset = self.offsets[id_]['offset']
            # print(f'{id_=} {offset=}')
            try:
                obj = self.parse_bytes(offset=offset)
            except Exception as e:
                print(f'Error decoding {id_=} {offset=}')
                errors.append(id_)
                continue

            work_id = obj[0] if self.kind == 'references' else obj.work_id
            if work_id != id_:
                errors.append(id_)
                print(f'Error in index for {id_}')

        print(f'{len(errors)} errors found in the {self.kind!r} index')

        if fix and len(errors) > 0:
            index_col = 'concept_id' if self.kind == 'concepts' else 'work_id'
            offsets_df = pd.read_csv(self.offset_path, index_col=index_col)
            offsets_df[~offsets_df.index.isin(errors)].to_csv(self.index_path / 'fixed_offsets.txt')
            print(f'Fixed offsets written to file. Offsets updated for the object')
            self.offsets = self.read_offsets()
        return errors


class ConceptIndexer(BaseIndexer):
    """
    Indexing concept -> work id maps
    """
    def __init__(self, paths: Paths, indices):
        super().__init__(paths=paths, indices=indices, kind='concepts')
        return

    def convert_to_bytes(self, concept: objects.Concept) -> bytes:
        """
        #concept_id, concept_name, concept_level, num_works, w1, w2, ...
        """
        bites = [
            self.encoder.encode_id(id_=concept.concept_id),
            self.encoder.encode_string(string=concept.name),
            self.encoder.encode_int(i=concept.level),
            self.encoder.encode_long_long_int(lli=concept.works_count)
        ]

        for w, score in tqdm(concept.tagged_works):
            bites.extend([
                self.encoder.encode_long_long_int(lli=w),
                self.encoder.encode_float(f=score)
            ])

        return b''.join(bites)

    def process_entry(self, concept_id: int, write: bool):
        if concept_id in self.offsets:
            return

        concept = objects.Concept(concept_id=concept_id)
        concept.populate_tagged_works(indices=self.indices, paths=self.paths)
        bites = self.convert_to_bytes(concept=concept)
        if write:
            self.write_index(bites=bites, id_=concept_id)
        return

    def parse_bytes(self, offset: int, reader=None) -> objects.Concept:
        if reader is None:
            reader = open(self.data_path, 'rb')

        reader.seek(offset)

        concept_id = self.decoder.decode_id(reader=reader)
        concept_name = self.decoder.decode_string(reader=reader)
        level = self.decoder.decode_int(reader=reader)
        works_count = self.decoder.decode_long_long_int(reader)

        tagged_works = []
        for _ in range(works_count):
            work_id = self.decoder.decode_long_long_int(reader)
            score = self.decoder.decode_float(reader)
            tagged_works.append((work_id, score))
        return objects.Concept(concept_id=concept_id, name=concept_name, level=level,
                               tagged_works=tagged_works, works_count=len(tagged_works))


class RefIndexer(BaseIndexer):
    """
    For indexing citations and references in binary format
    format: #work_id #refs ref1 ref2 .. #citations cite1 cite2
    """

    def __init__(self, paths: Paths, indices):
        super().__init__(paths, indices, kind='references')
        return

    def convert_to_bytes(self, work: objects.Work) -> bytes:
        """
        Return bytes for the work
        #work_id, number of references, w1, w2, ...., wn
        """
        bites = [
            self.encoder.encode_id(id_=work.work_id),
            self.encoder.encode_long_int(li=len(work.references))
        ]

        # add references
        bites.extend(
            self.encoder.encode_long_long_int(lli=ref_w) for ref_w in work.references
        )

        # add citations
        bites.append(
            self.encoder.encode_long_int(li=len(work.citing_works))
        )

        bites.extend(
            self.encoder.encode_long_long_int(lli=cite_w) for cite_w in work.citing_works
        )
        return b''.join(bites)

    def process_entry(self, work_id, write):
        """
        Offset of current object = offset of previous object + length of previous object

        Take a work_id, check if it's already computed, if yes, pass
        Create a work object, find references
        Write bytes to a file
        Compute offsets and write offset
        """
        work = objects.Work(work_id=work_id, paths=self.paths)
        work.populate_references(self.indices)
        work.populate_citations(self.indices)

        bites = self.convert_to_bytes(work=work)
        if write:
            self.write_index(bites=bites, id_=work_id)
        return work

    def dump_bytes(self, work_id: int, bites: bytes):
        """
        Write the work_id, len(work_id), and bytes in dumps.txt
        """
        content = [
            self.encoder.encode_id(id_=work_id),
            self.encoder.encode_long_int(li=len(bites)),  # length of bytes
            bites,
        ]
        content = b''.join(content)
        with open(self.dump_path, 'ab') as writer:
            writer.write(content)
        return content

    def parse_bytes(self, offset: int, reader=None) -> (int, set, set):
        if reader is None:
            reader = open(self.data_path, 'rb')

        reader.seek(offset)

        work_id = self.decoder.decode_id(reader)
        # print(f'{work_id=}')

        num_refs = self.decoder.decode_long_int(reader)
        # print(f'{num_refs=}')

        refs = {self.decoder.decode_long_long_int(reader) for _ in range(num_refs)}

        num_cites = self.decoder.decode_long_int(reader)
        # print(f'{num_cites=}')

        cites = {self.decoder.decode_long_long_int(reader) for _ in range(num_cites)}

        return work_id, refs, cites


class WorkIndexer(BaseIndexer):
    """
    Write work information into a binary file
    #,work_id, type, DOI, title, venue_id, date, year, abstract
    """

    def __init__(self, paths: Paths, indices, id_map: IDMap):
        super().__init__(paths, indices, kind='works')
        self.id_map = id_map
        self.ref_indexer = RefIndexer(paths=self.paths, indices=self.indices)
        return

    def process_entry(self, work_id: int, write: bool = True):
        work = objects.Work(work_id=work_id, paths=self.paths)
        work.populate_info(indices=self.indices)
        work.populate_venue(indices=self.indices, id_map=self.id_map)
        work.populate_authors(indices=self.indices)
        work.populate_concepts(indices=self.indices, id_map=self.id_map)
        bites = self.convert_to_bytes(work=work)

        if write:
            self.write_index(id_=work_id, bites=bites)
        return work

    def convert_to_bytes(self, work) -> bytes:
        bites = [
            self.encoder.encode_id(id_=work.work_id),
            self.encoder.encode_int(i=work.part_no),
            self.encoder.encode_work_type(typ=work.type)
        ]

        cleaned_title = clean_string(work.title)
        year = work.publication_year if work.publication_year is not None else 0
        bites.extend([
            self.encoder.encode_string(string=work.doi),  # DOI
            self.encoder.encode_string(string=cleaned_title),  # title

            self.encoder.encode_int(i=year),
            self.encoder.encode_string(string=work.publication_date),

            self.encoder.encode_venue(venue=work.venue),  # venue
        ])

        abstract = clean_string(reconstruct_abstract(work.abstract_inverted_index))  # abstract
        # print(f'{abstract=!r}')
        bites.append(self.encoder.encode_string(abstract))

        # add author info
        bites.append(self.encoder.encode_int(i=len(work.authors)))  # number of authors

        bites.extend([
            self.encoder.encode_author(author=auth) for auth in work.authors  # add authors
        ])

        # add concept info
        bites.append(self.encoder.encode_int(i=len(work.concepts)))

        bites.extend([
            self.encoder.encode_concept(concept=concept) for concept in work.concepts
        ])
        return b''.join(bites)

    def dump_bytes(self, work_id: int, bites: bytes):
        """
        Write the work_id, len(work_id), and bytes in dumps.txt
        """
        content = [
            self.encoder.encode_id(id_=work_id),
            self.encoder.encode_long_int(li=len(bites)),  # length of bytes
            bites,
        ]
        content = b''.join(content)
        with open(self.dump_path, 'ab') as writer:
            writer.write(content)
        return

    def parse_bytes(self, offset: int, reader=None) -> objects.Work:
        if reader is None:
            reader = open(self.data_path, 'rb')
        reader.seek(offset)

        work_id = self.decoder.decode_id(reader)
        # print(f'{work_id=}')

        part_no = self.decoder.decode_int(reader)
        # print(f'{part_no=}')

        work_type = self.decoder.decode_work_type(reader)
        # print(f'{work_type=}')

        doi = self.decoder.decode_string(reader)
        # print(f'{doi=}')

        title = self.decoder.decode_string(reader)
        # print(f'{title=}')

        year = self.decoder.decode_int(reader)
        # print(f'{year=}')

        assert year < 3000, f'year {year} out of range!'

        date = self.decoder.decode_string(reader)
        # print(f'{date=}')

        venue = self.decoder.decode_venue(reader)
        # print(f'{venue=}')

        abstract = self.decoder.decode_string(reader)
        # print(f'{abstract=}')

        num_authors = self.decoder.decode_int(reader)
        # print(f'{num_authors=}')

        authors = [self.decoder.decode_author(reader) for _ in range(num_authors)]

        num_concepts = self.decoder.decode_int(reader)
        # print(f'{num_concepts=}')

        concepts = [self.decoder.decode_concept(reader) for _ in range(num_concepts)]
        reader.close()

        work = objects.Work(work_id=work_id, part_no=part_no, type=work_type, doi=doi, title=title,
                            publication_year=year,
                            publication_date=date, venue=venue, abstract=abstract, authors=authors, concepts=concepts)

        return work
