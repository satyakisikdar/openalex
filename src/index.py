"""
For all the indexing stuff
Do it for concepts, authorships, works

Works: workid, type, title, venue id, date, year, cited_by_count, references, num_authors
Works Authors: workid, num authors, author1, num_inst1, [inst1, inst2], author2, inst1, ...]
Works Concepts: workid, num concepts, [concept1, score1, concept2, score2, ... ]
Concept Works: concept_id,  num_works [work1, score1, work2, score2, ... ]
"""
import abc
import io
import os
import subprocess
from typing import List

import pandas as pd
import requests
from tqdm.auto import tqdm

import src.objects as objects
from src.utils import Paths, IDMap, reconstruct_abstract, clean_string, reconstruct_abstract_new, load_pickle


class BaseIndexer:
    """
    Base class for indexers
    """

    def __init__(self, paths: Paths, indices, kind: str, fast: bool = False):
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

        self.offsets = self.read_offsets(fast=fast)

        self.encoder = encoder.EncoderDecoder()
        self.decoder = encoder.EncoderDecoder()  # just me being lazy

        self.ref_indexer = None  # None except in Work Indexer class
        return

    def __len__(self) -> int:
        return len(self.offsets)

    def _read_offsets(self, parquet: bool = False) -> pd.DataFrame:
        """
        Read offsets from a file
        :return:
        """
        if not self.offset_path.exists():
            with open(self.offset_path, 'w') as fp:
                fp.write('work_id,offset,len\n')

        if parquet and (self.index_path / 'offsets.parquet').exists():
            df = pd.read_parquet(self.index_path / 'offsets.parquet')
        else:
            df = pd.read_csv(self.offset_path, index_col=0, dtype=int, engine='pyarrow')
        offsets_dict = df.to_dict('index')
        return offsets_dict

    def read_offsets(self, fast: bool = False):
        """
        New reader that reads the offset file and create the dictionary directly
        If fast, try reading the pickle
        """
        # how long does it take to read offsets using a file
        d = {}
        path = self.offset_path
        if not path.exists():
            with open(path, 'w') as writer:
                writer.write('work_id,offset,len\n')
            return d

        if fast:
            pickle_path = self.index_path / 'offsets.pkl'
            if pickle_path.exists():
                d = load_pickle(pickle_path)
                return d

        num_lines = int(subprocess.check_output(f'wc -l {path}', shell=True).decode('utf-8').split()[0])
        if num_lines == 0:  # empty or just the header
            with open(path, 'w') as writer:
                writer.write('work_id,offset,len\n')
        print(f'{num_lines - 1:,} entries in the offset')
        reader = io.StringIO(open(path).read())

        for i, line in enumerate(tqdm(reader, total=num_lines, unit_scale=True, unit=' works',
                                      desc='Reading offsets...')):
            if i == 0:
                continue
            work_id, offset, length = map(int, line.split(','))
            d[work_id] = {'offset': offset, 'len': length}
        reader.close()
        return d

    def update_index_from_dump(self):
        """
        Read the dumps.txt file and update the index
        """
        if not self.dump_path.exists():
            return

        updates = 0
        with open(self.dump_path, 'rb') as reader:
            stuff = reader.read()

        if len(stuff) == 0:
            return

        reader = io.BytesIO(stuff)

        id_list, bites_list = [], []  # use write indices instead of write index
        with tqdm(total=len(stuff), miniters=1, colour='orange', desc='Updating data...',
                  unit='B', unit_scale=True, unit_divisor=1024, leave=False) as pbar:
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
                    id_list.append(work_id)
                    bites_list.append(work_bytes)
                    # self.write_index(bites=work_bytes, id_=work_id)
                    updates += 1
                # pbar.set_postfix(updates=updates, refresh=False)
                pbar.update(1 + 8 + len_ + 8)
            self.write_indices(bites_list=bites_list, id_list=id_list)
        if updates > 0:
            print(f'{updates:,} new entries added from dump')

        # clear out the dumps file
        writer = open(self.dump_path, 'wb')
        writer.close()
        reader.close()
        return

    def _update_index_from_dump(self):
        """
        Read the dumps.txt file and update the index
        """
        if not self.dump_path.exists():
            return

        updates = 0
        with open(self.dump_path, 'rb') as reader:
            stuff = reader.read()

        if len(stuff) == 0:
            return

        reader = io.BytesIO(stuff)

        with tqdm(total=len(stuff), miniters=1, colour='orange', desc='Updating data...',
                  unit='B', unit_scale=True, unit_divisor=1024) as pbar:
            while True:
                bite = reader.read(1)
                if not bite:
                    break

                work_id = self.decoder.decode_long_long_int(reader)  # the # sign is already read
                # print(f'Reading {work_id=} from dumps')
                len_ = self.decoder.decode_long_int(reader)
                work_bytes = reader.read(len_)  # read the bytes
                pbar.update(1 + 8 + len_ + 8)

                if work_id not in self.offsets:
                    # print(f'Writing new {work_id=} to offsets')
                    self.write_index(bites=work_bytes, id_=work_id)
                    updates += 1
                # pbar.set_postfix(updates=updates, refresh=False)

        if updates > 0:
            print(f'{updates:,} new entries added from dump')

        # clear out the dumps file
        writer = open(self.dump_path, 'wb')
        writer.close()
        reader.close()
        return

    def write_indices(self, bites_list: List[bytes], id_list: List[int]):
        """
        Write multiple items at once, reducing file IO
        keep track of last offset and last len incrementally
        """
        if len(self.offsets) == 0:
            previous_offset = 0
            previous_len = 0
        else:
            last_key, _ = _, self.offsets[last_key] = self.offsets.popitem()
            previous_offset, previous_len = self.offsets[last_key]['offset'], self.offsets[last_key]['len']

        with open(self.offset_path, 'a') as offset_writer, open(self.data_path, 'ab') as data_writer:
            for id_, bites in zip(id_list, bites_list):
                offset = previous_offset + previous_len
                self.offsets[id_] = {'offset': offset, 'len': len(bites)}
                offset_writer.write(f'{id_},{offset},{len(bites)}\n')  # write the offset
                data_writer.write(bites)  # write the bytes

                # update the offsets and lens
                previous_offset = offset
                previous_len = len(bites)
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
            # last_key = list(self.offsets.keys())[-1]
            last_key, _ = _, self.offsets[last_key] = self.offsets.popitem()
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
        if isinstance(item, str):
            item = int(item[1:])
        if item in self.offsets:
            try:
                offset = self.offsets[item]['offset']
            except TypeError:  # offsets dictionary only has
                offset = self.offsets[item]
            entry = self.parse_bytes(offset=offset)

        else:
            entry = self.process_entry(item, write=False)  # dont write to the index just yet
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

    def validate_and_fix_index(self, fix: bool = True, start: int = 0, read_offsets: bool = True):
        """
        Validate the index by matching the work id / concept id from the extracted object with that of the offset file
        """

        # TODO: fix bug when the last entry is corrupted. The data file needs to be updated
        # TODO: otherwise the indices will not work

        errors = []
        if read_offsets:
            self.offsets = self.read_offsets()

        if start != 0:
            print(f'Starting at {start=:,}')

        if start < 0:
            cmd = f'tail {start} {self.offset_path}'
            stuff = subprocess.check_output(cmd, shell=True).decode('utf-8')
            ids = tuple(pd.read_csv(io.StringIO(stuff), dtype='int', header=None, engine='c').iloc[:, 0])
        else:
            ids = list(self.offsets.keys())[start:]

        with tqdm(total=len(ids), desc='Validating index', unit_scale=True, unit=' works') as pbar:
            for id_ in ids:
                offset = self.offsets[id_]['offset']
                pbar.update(1)
                # print(f'{id_=} {offset=}')
                try:
                    obj = self.parse_bytes(offset=offset)
                except Exception as e:
                    # print(f'Error decoding {id_=} {offset=}')
                    errors.append(id_)
                    continue

                work_id = obj[0] if self.kind == 'references' else obj.work_id
                if work_id != id_:
                    errors.append(id_)
                    # print(f'Error in index for {id_}')
                pbar.set_postfix_str(f'{len(errors):,} errors', refresh=False)
            print(f'{len(errors):,} errors found in the {self.kind!r} index')

        if fix and len(errors) > 0:
            index_col = 'concept_id' if self.kind == 'concepts' else 'work_id'
            offsets_df = pd.read_csv(self.offset_path, engine='pyarrow', index_col=index_col)
            offsets_df = offsets_df[~offsets_df.index.isin(set(errors))]
            offsets_df.to_csv(self.index_path / 'fixed_offsets.txt')

            last_key = offsets_df.tail(1).index.values[0]
            # last_key, _ = _, self.offsets[last_key] = self.offsets.popitem()
            previous_offset, previous_len = self.offsets[last_key]['offset'], self.offsets[last_key]['len']
            with open(self.data_path, 'rb') as reader:
                stuff = reader.read(previous_offset + previous_len)

            with open(self.index_path / 'fixed_data.txt', 'wb') as writer:
                writer.write(stuff)

            print(f'Fixed offsets & data written to files. Offsets updated for the object')

        return

    def _validate_and_fix_index(self, fix: bool = True, start=0):
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

        for id_ in tqdm(ids, desc='Validating index..'):

            offset = self.offsets[id_]['offset']
            # print(f'{id_=} {offset=}')
            try:
                obj = self.parse_bytes(offset=offset)
            except Exception as e:
                # print(f'Error decoding {id_=} {offset=}')
                errors.append(id_)
                continue

            work_id = obj[0] if self.kind == 'references' else obj.work_id
            if work_id != id_:
                errors.append(id_)
                # print(f'Error in index for {id_}')

        print(f'{len(errors):,} errors found in the {self.kind!r} index')

        if fix and len(errors) > 0:
            index_col = 'concept_id' if self.kind == 'concepts' else 'work_id'
            offsets_df = pd.read_csv(self.offset_path, engine='pyarrow', index_col=index_col)
            offsets_df = offsets_df[~offsets_df.index.isin(set(errors))]
            offsets_df.to_csv(self.index_path / 'fixed_offsets.txt')

            self.offsets = self.read_offsets()
            last_key = offsets_df.tail(1).index.values[0]
            # last_key, _ = _, self.offsets[last_key] = self.offsets.popitem()
            previous_offset, previous_len = self.offsets[last_key]['offset'], self.offsets[last_key]['len']
            with open(self.data_path, 'rb') as reader:
                stuff = reader.read(previous_offset + previous_len)

            with open(self.index_path / 'fixed_data.txt', 'wb') as writer:
                writer.write(stuff)

            print(f'Fixed offsets & data written to files. Offsets updated for the object')

        return


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
    For indexing cited_by_count and references in binary format
    format: #work_id #refs ref1 ref2 .. #cited_by_count cite1 cite2
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

        # add cited_by_count
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

        abstract = reconstruct_abstract(work.abstract_inverted_index)  # abstract
        if abstract == '':
            abstract = reconstruct_abstract_new(work.abstract_inverted_index)  # abstract
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


class NewWorkIndexer(BaseIndexer):
    """
        Write work information into a binary file
        #,work_id, type, DOI, title, venue_id, date, year, abstract

        TODO: improve parse_bytes by pre-loading some of the binary file in memory. load up lengths of objects
        TODO: alongside offsets. Load up 100k entries in memory
    """

    def __init__(self, paths: Paths, indices, id_map: IDMap, fast: bool = False):
        super().__init__(paths, indices, kind='new-works', fast=fast)
        self.id_map = id_map
        # self.ref_indexer = RefIndexer(paths=self.paths, indices=self.indices)
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

        # abstract = work.abstract
        # bites.append(self.encoder.encode_string(abstract))

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

        # cited by count
        bites.append(self.encoder.encode_int(work.cited_by_count))

        # add references
        bites.append(self.encoder.encode_long_int(li=len(work.references)))  # number of references
        bites.extend(
            self.encoder.encode_long_long_int(lli=ref_w) for ref_w in work.references
        )

        # add related works
        bites.append(self.encoder.encode_long_int(li=len(work.related_works)))  # number of references
        bites.extend(
            self.encoder.encode_long_long_int(lli=rel_w) for rel_w in work.related_works
        )

        # updated date
        bites.append(self.encoder.encode_string(work.updated_date))

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

        # abstract = self.decoder.decode_string(reader)
        # print(f'{abstract=}')

        num_authors = self.decoder.decode_int(reader)
        # print(f'{num_authors=}')

        authors = [self.decoder.decode_author(reader) for _ in range(num_authors)]

        num_concepts = self.decoder.decode_int(reader)
        # print(f'{num_concepts=}')

        concepts = [self.decoder.decode_concept(reader) for _ in range(num_concepts)]

        # cited by count
        cited_by_count = self.decoder.decode_int(reader)

        num_refs = self.decoder.decode_long_int(reader)
        # print(f'{num_refs=}')

        references = {self.decoder.decode_long_long_int(reader) for _ in range(num_refs)}

        num_related_works = self.decoder.decode_long_int(reader)
        # print(f'{num_related_works=}')

        related_works = {self.decoder.decode_long_long_int(reader) for _ in range(num_related_works)}

        updated_date = self.decoder.decode_string(reader)
        reader.close()

        work = objects.Work(work_id=work_id, cited_by_count=cited_by_count, type=work_type, doi=doi, title=title,
                            publication_year=year, references=references, related_works=related_works,
                            publication_date=date, venue=venue, abstract=abstract, authors=authors, concepts=concepts,
                            updated_date=updated_date)

        return work


class AuthorIndexer(BaseIndexer):
    """
    Indexer for Author works
    """

    def __init__(self, paths: Paths, indices, work_indexer, id_map):
        super().__init__(paths=paths, indices=indices, kind='authors')
        self.work_indexer = work_indexer
        self.id_map = id_map
        return

    def convert_to_bytes(self, author: objects.Author) -> bytes:
        """
        #author_id, author_name, num_works, w1, w2, ...
        """
        bites = [
            self.encoder.encode_id(id_=author.author_id),
            self.encoder.encode_string(string=author.name),
            self.encoder.encode_long_long_int(lli=len(author.work_ids))
        ]

        for w in tqdm(author.work_ids):
            bites.extend([
                self.encoder.encode_long_long_int(lli=w),
            ])

        return b''.join(bites)

    def process_entry(self, author_id: int, write: bool):
        if author_id in self.offsets:
            return

        email = 'xcs@nd.edu'

        with requests.Session() as session:
            url = f'https://api.openalex.org/A{author_id}'
            params = {'mailto': email}
            session.headers.update(params)
            response = session.get(url, headers=session.headers, params=params)
            if response.status_code != 200:
                print(f'Status code: {response.status_code} author_id: {author_id}')
                return
            data = response.json()

        author = objects.Author(author_id=author_id, name=data['display_name'])
        author.parse_all_author_works(id_map=self.id_map, work_indexer=self.work_indexer)

        bites = self.convert_to_bytes(author=author)
        if write:
            self.write_index(bites=bites, id_=author_id)
        return bites

    def parse_bytes(self, offset: int, reader=None):
        if reader is None:
            reader = open(self.data_path, 'rb')

        reader.seek(offset)

        author_id = self.decoder.decode_id(reader=reader)
        author_name = self.decoder.decode_string(reader=reader)
        works_count = self.decoder.decode_long_long_int(reader)

        work_ids = []
        for _ in range(works_count):
            work_id = self.decoder.decode_long_long_int(reader)
            work_ids.append(work_id)
        assert works_count == len(work_ids), 'work ids dont match'
        return author_id, author_name, work_ids
