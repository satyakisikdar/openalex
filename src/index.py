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

from src.encoder import EncoderDecoder
from src.objects import Work
from src.utils import Paths, IDMap, reconstruct_abstract, strip_accents, clean_string


class BaseIndexer:
    """
    Base class for indexers
    """

    def __init__(self, paths: Paths, indices, kind: str):
        self.paths = paths
        self.indices = indices
        self.kind = kind
        self.index_path = self.paths.compressed_path / kind
        if not self.index_path.exists():
            os.makedirs(self.index_path)

        self.offset_path = self.index_path / 'offsets.txt'
        self.data_path = self.index_path / 'data.txt'
        self.offsets = self.read_offsets()

        self.encoder = EncoderDecoder()
        self.decoder = EncoderDecoder()  # just me being lazy
        return

    def read_offsets(self) -> pd.DataFrame:
        """
        Read offsets from a file
        :return:
        """
        if not self.offset_path.exists():
            with open(self.offset_path, 'w') as fp:
                fp.write('work_id,offset,len\n')

        return pd.read_csv(self.offset_path, index_col=0, engine='c').to_dict('index')

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
            last_key = list(self.offsets.keys())[-1]
            previous_offset, previous_len = self.offsets[last_key]['offset'], self.offsets[last_key]['len']

        offset = previous_offset + previous_len
        self.offsets[id_] = {'offset': offset, 'len': len(bites)}

        with open(self.offset_path, 'a') as offset_writer, open(self.data_path, 'ab') as data_writer:
            offset_writer.write(f'{id_},{offset},{len(bites)}\n')  # write the offset
            data_writer.write(bites)  # write the bytes
        return

    @abc.abstractmethod
    def convert_to_bytes(self, entity) -> bytes:
        pass

    @abc.abstractmethod
    def process_entry(self, id_: int):
        pass

    @abc.abstractmethod
    def parse_bytes(self, offset: int, reader=None):
        pass


class RefIndexer(BaseIndexer):
    """
    For indexing citations and references in binary format
    format: #work_id #refs ref1 ref2 .. #citations cite1 cite2
    """

    def __init__(self, paths: Paths, indices):
        super().__init__(paths, indices, kind='references')
        return

    def convert_to_bytes(self, work: Work) -> bytes:
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

    def process_entry(self, work_id):
        """
        Offset of current object = offset of previous object + length of previous object

        Take a work_id, check if it's already computed, if yes, pass
        Create a work object, find references
        Write bytes to a file
        Compute offsets and write offset
        """
        if work_id in self.offsets:  # already computed
            return

        work = Work(work_id=work_id, paths=self.paths)
        work.populate_references(self.indices)
        work.populate_citations(self.indices)

        bites = self.convert_to_bytes(work=work)
        self.write_index(bites=bites, id_=work_id)
        return

    def parse_bytes(self, offset: int, reader=None) -> Work:
        if reader is None:
            reader = open(self.data_path, 'rb')

        reader.seek(offset)

        work_id = self.decoder.decode_id(reader)
        print(f'{work_id=}')

        num_refs = self.decoder.decode_long_int(reader)
        print(f'{num_refs=}')

        refs = {self.decoder.decode_long_long_int(reader) for _ in range(num_refs)}

        num_cites = self.decoder.decode_long_int(reader)
        print(f'{num_cites=}')

        cites = {self.decoder.decode_long_long_int(reader) for _ in range(num_cites)}

        return Work(work_id=work_id, citing_works=cites, references=refs)


class WorkIndexer(BaseIndexer):
    """
    Write work information into a binary file
    #,work_id, type, DOI, title, venue_id, date, year, abstracts
    """

    def __init__(self, paths: Paths, indices, id_map: IDMap):
        super().__init__(paths, indices, kind='works')
        self.id_map = id_map
        return

    def process_entry(self, work_id: int):
        if work_id in self.offsets:  # already computed
            return

        work = Work(work_id=work_id, paths=self.paths)
        work.populate_info(indices=self.indices)
        work.populate_venue(indices=self.indices, id_map=self.id_map)
        work.populate_authors(indices=self.indices)
        bites = self.convert_to_bytes(work=work)

        self.write_index(id_=work_id, bites=bites)
        return

    def convert_to_bytes(self, work) -> bytes:
        bites = [
            self.encoder.encode_id(id_=work.work_id),
            self.encoder.encode_int(i=work.part_no),
            self.encoder.encode_work_type(typ=work.type)
        ]

        cleaned_title = clean_string(work.title)

        bites.extend([
            self.encoder.encode_string(string=work.doi),  # DOI
            self.encoder.encode_string(string=cleaned_title),  # title

            self.encoder.encode_int(i=work.publication_year),
            self.encoder.encode_string(string=work.publication_date),

            self.encoder.encode_venue(venue=work.venue),  # venue
        ])

        abstract = clean_string(reconstruct_abstract(work.abstract_inverted_index))  # abstract

        bites.append(self.encoder.encode_string(abstract))

        # add author info
        # num_authors, author1, inst1, inst2, ... , author2, ..

        # add concept info
        return b''.join(bites)

    def parse_bytes(self, offset: int, reader=None) -> Work:
        if reader is None:
            reader = open(self.data_path, 'rb')
        reader.seek(offset)

        work_id = self.decoder.decode_id(reader)
        print(f'{work_id=}')

        part_no = self.decoder.decode_int(reader)
        print(f'{part_no=}')

        work_type = self.decoder.decode_work_type(reader)
        print(f'{work_type=}')

        doi = self.decoder.decode_string(reader)
        print(f'{doi=}')

        title = self.decoder.decode_string(reader)
        print(f'{title=}')

        year = self.decoder.decode_int(reader)
        print(f'{year=}')

        assert year < 3000, f'year {year} out of range!'

        date = self.decoder.decode_string(reader)
        print(f'{date=}')

        venue = self.decoder.decode_venue(reader)

        abstract = self.decoder.decode_string(reader)
        reader.close()

        work = Work(work_id=work_id, part_no=part_no, type=work_type, doi=doi, title=title, publication_year=year,
                    publication_date=date, venue=venue, abstract=abstract)

        return work
