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
import struct

import pandas as pd

from src.objects import Work
from src.utils import Paths, Indices


class BaseIndexer:
    """
    Base class for indexers
    """

    def __init__(self, paths: Paths, indices: Indices, kind: str):
        self.paths = paths
        self.indices = indices
        self.kind = kind
        self.index_path = self.paths.compressed_path / kind
        if not self.index_path.exists():
            os.makedirs(self.index_path)

        self.offset_path = self.index_path / 'offsets.txt'
        self.data_path = self.index_path / 'data.txt'

        self.offsets = self.read_offsets()
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

    @abc.abstractmethod
    def get_bytes(self, entity) -> bytes:
        pass

    @abc.abstractmethod
    def process_entry(self, id_: int):
        pass

    @abc.abstractmethod
    def parse_entry(self, offset: int):
        pass


class RefIndexer(BaseIndexer):
    """
    For indexing citations and references in binary format
    format: #work_id #refs ref1 ref2 .. #citations cite1 cite2
    """

    def __init__(self, paths: Paths, indices: Indices):
        super().__init__(paths, indices, kind='references')
        return

    def get_bytes(self, work: Work) -> bytes:
        """
        Return bytes for the work
        #work_id, number of references, w1, w2, ...., wn
        """
        num_refs = len(work.references)

        bites = [
            struct.pack('c', '#'.encode('utf-8')),  # add a # sign
            struct.pack('Q', work.work_id),  # work id comes firs
            struct.pack('L', num_refs),  # number of references
        ]

        bites.extend(
            struct.pack('Q', ref_w) for ref_w in work.references
        )

        # add citations
        num_cites = len(work.citing_works)
        bites.append(struct.pack('L', num_cites))

        bites.extend(
            struct.pack('Q', cite_w) for cite_w in work.citing_works
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

        bites = self.get_bytes(work=work)
        # print(f'{work_id=} has {len(work.references)} references {work.citations} citations {len(bites)} bytes')

        if len(self.offsets) == 0:
            previous_offset = 0
            previous_len = 0
        else:
            last_key = list(self.offsets.keys())[-1]
            previous_offset, previous_len = self.offsets[last_key]['offset'], self.offsets[last_key]['len']

        offset = previous_offset + previous_len
        self.offsets[work_id] = {'offset': offset, 'len': len(bites)}

        with open(self.offset_path, 'a') as offset_writer, open(self.data_path, 'ab') as data_writer:
            offset_writer.write(f'{work_id},{offset},{len(bites)}\n')  # write the offset
            data_writer.write(bites)  # write the bytes

        return

    def parse_entry(self, offset: int) -> Work:
        with open(self.data_path, 'rb') as reader:
            reader.seek(offset)
            h_, = struct.unpack('c', reader.read(1))
            assert h_.decode('utf-8') == '#', 'missing #, something wrong with the index'

            id_, = struct.unpack('Q', reader.read(8))

            print(f'reading work_id: {id_}')
            num_refs, = struct.unpack('L', reader.read(8))
            print(f'References: {num_refs}')
            refs = set()
            for _ in range(num_refs):
                ref_, = struct.unpack('Q', reader.read(8))
                refs.add(ref_)

            num_cites, = struct.unpack('L', reader.read(8))
            print(f'Citations: {num_cites}')
            cites = set()
            for _ in range(num_cites):
                cite_, = struct.unpack('Q', reader.read(8))
                cites.add(cite_)

        work = Work(work_id=id_, references=refs, citing_works=cites, citations=num_cites)
        return work
