"""
For all the indexing stuff
"""
import os
import struct

import pandas as pd

from src.objects import Work
from src.utils import Paths, Indices


class RefIndex:
    """
    For indexing citations and references in binary format
    format: #work_id #refs ref1 ref2 .. #citations cite1 cite2
    """

    def __init__(self, paths: Paths, indices: Indices):
        self.paths = paths
        self.indices = indices
        self.index_path = self.paths.compressed_path / 'references'
        if not self.index_path.exists():
            os.makedirs(self.index_path)

        self.offset_path = self.index_path / 'offsets.txt'
        self.data_path = self.index_path / 'data.txt'

        self.offsets = self.read_offsets()
        return

    def read_offsets(self):
        """
        Read offsets from a file
        :return:
        """
        if not self.offset_path.exists():
            with open(self.offset_path, 'w') as fp:
                fp.write('work_id,offset,len\n')

        return pd.read_csv(self.offset_path, index_col=0, engine='c').to_dict('index')

    @staticmethod
    def get_bytes(w, refs, cites):
        """
        Return bytes for the work
        #work_id, number of references, w1, w2, ...., wn
        """
        num_refs = len(refs)

        bites = [
            struct.pack('c', '#'.encode('utf-8')),  # add a # sign
            struct.pack('Q', w),  # work id comes firs
            struct.pack('L', num_refs),  # number of references
        ]

        bites.extend(
            struct.pack('Q', ref_w) for ref_w in refs
        )

        # add citations
        num_cites = len(cites)
        bites.append(struct.pack('L', num_cites))

        bites.extend(
            struct.pack('Q', cite_w) for cite_w in cites
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

        bites = self.get_bytes(w=work_id, refs=work.references, cites=work.citing_works)
        print(f'{work_id=} has {len(work.references)} references {work.citations} citations {len(bites)} bytes')

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

    @staticmethod
    def read_stuff(reader, offset):
        reader.seek(offset, 0)
        h_, = struct.unpack('c', reader.read(1))
        id_, = struct.unpack('Q', reader.read(8))
        print(f'reading work_id: {id_}')
        num_refs, = struct.unpack('L', reader.read(8))
        print(f'References: {num_refs}')
        refs = []
        for _ in range(num_refs):
            ref_, = struct.unpack('Q', reader.read(8))
            refs.append(ref_)

        num_cites, = struct.unpack('L', reader.read(8))
        print(f'Citations: {num_cites}')
        cites = []
        for _ in range(num_cites):
            cite_, = struct.unpack('Q', reader.read(8))
            cites.append(cite_)

        return h_, id_, num_refs, refs, cites
