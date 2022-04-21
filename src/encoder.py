"""
Houses the encoder decoder class - moved out of utils to prevent a circular import
"""
import struct

from src.objects import Venue, Institution, Author
from src.utils import clean_string


class EncoderDecoder:
    """
    Encoder and Decoders for different data types
    """
    work_types_dict = {typ: i for i, typ in enumerate([None, 'journal-article', 'unknown',
                                                       'book-chapter', 'proceedings-article',
                                                       'dissertation',
                                                       'book', 'posted-content', 'report', 'dataset',
                                                       'monograph', 'other', 'component',
                                                       'reference-entry', 'peer-review',
                                                       'reference-book', 'journal-issue', 'journal',
                                                       'standard', 'report-series', 'proceedings',
                                                       'book-part', 'book-section', 'book-series',
                                                       'proceedings-series', 'journal-volume',
                                                       'book-set', 'grant', 'book-track'])}

    work_types_inv_dict = {v: k for k, v in work_types_dict.items()}

    def encode_id(self, id_: int) -> bytes:
        """
        Encode IDs as # followed by unsigned long long ints
        """
        return b''.join([
            struct.pack('c', '#'.encode('utf-8')),  # add a # sign
            struct.pack('Q', id_),  # work id comes first
        ])

    def decode_id(self, reader) -> int:
        hash_, = struct.unpack('c', reader.read(1))
        assert hash_.decode('utf-8') == '#', 'missing # in ID, error'
        id_, = struct.unpack('Q', reader.read(8))
        return id_

    def encode_title(self, title: str) -> bytes:
        """
        Non latin alphabet appears to be messing up the encoding process
        """
        return self.encode_string(string=title, encoding='utf-16')

    def decode_title(self, reader) -> str:
        """
        Non latin alphabet appears to be messing up the encoding process
        """
        return self.decode_string(reader=reader, encoding='utf-16')

    def encode_string(self, string: str, encoding='utf-8') -> bytes:
        """
        Encode string into two pieces: a long storing the length in bytes, then the string in bytes
        """
        if string is None:
            string = ''
        return b''.join([
            struct.pack('L', len(string)),  # number of bytes for title
            bytes(string, encoding=encoding)  # the actual title
        ])

    def decode_string(self, reader, encoding='utf-8') -> str:
        str_len, = struct.unpack('L', reader.read(8))
        assert isinstance(str_len, int), f'String length {str_len} not an int'
        content, = struct.unpack(f'{str_len}s', reader.read(str_len))
        return content.decode(encoding)

    def encode_long_long_int(self, lli) -> bytes:
        return struct.pack('Q', lli)

    def decode_long_long_int(self, reader) -> int:
        return struct.unpack('Q', reader.read(8))[0]

    def encode_long_int(self, li) -> bytes:
        return struct.pack('L', li)

    def decode_long_int(self, reader) -> int:
        return struct.unpack('L', reader.read(8))[0]

    def encode_int(self, i) -> bytes:
        if i is None:
            i = 0
        return struct.pack('I', i)

    def decode_int(self, reader) -> int:
        return struct.unpack('I', reader.read(4))[0]

    def encode_work_type(self, typ: str) -> bytes:
        typ_int = EncoderDecoder.work_types_dict[typ]
        return struct.pack('B', typ_int)

    def decode_work_type(self, reader):
        typ, = struct.unpack('B', reader.read(1))
        return EncoderDecoder.work_types_inv_dict[typ]

    def encode_venue(self, venue) -> bytes:
        venue_id = venue.venue_id if venue is not None else 0
        venue_name = venue.name if venue is not None else ''
        return b''.join([
            self.encode_long_long_int(lli=venue_id),
            self.encode_string(string=venue_name)
        ])

    def decode_venue(self, reader) -> Venue:
        venue_id = self.decode_long_long_int(reader)
        if venue_id == 0:
            return None
        venue_name = self.decode_string(reader)
        return Venue(venue_id=venue_id, name=venue_name)

    def encode_author(self, author) -> bytes:
        if author is None:
            author_id = 0
            author_name = ''
            position = ''
            num_insts = 0  # number of institutions
            insts = []
        else:
            author_id = author.author_id
            author_name = author.name
            position = author.position[0]
            if author.insts[0] is None:  # no inst info available
                num_insts = 0
                insts = []
            else:
                num_insts = len(author.insts)
                insts = author.insts

        bites = [
            self.encode_long_long_int(lli=author_id),  # author id
            self.encode_string(string=clean_string(author_name)),  # author name
            self.encode_string(string=position),  # author position

            self.encode_int(i=num_insts),   # number of institutes
        ]

        # inst info
        bites.extend([
            self.encode_institute(inst=inst) for inst in insts
        ])
        return b''.join(bites)

    def decode_author(self, reader) -> Author:
        author_id = self.decode_long_long_int(reader)
        if author_id == 0:
            return None
        author_name = self.decode_string(reader)
        position = self.decode_string(reader)

        num_inst = self.decode_int(reader)
        if num_inst == 0:
            insts = [None]
        else:
            insts = [self.decode_institute(reader) for _ in range(num_inst)]

        return Author(author_id=author_id, name=author_name, position=position, insts=insts)

    def encode_institute(self, inst) -> bytes:
        if inst is None:
            inst_id = 0
            inst_name = ''
        else:
            inst_id = inst.institution_id
            inst_name = clean_string(inst.name)

        return b''.join([
            self.encode_long_long_int(lli=inst_id),
            self.encode_string(string=inst_name)
        ])

    def decode_institute(self, reader) -> Institution:
        inst_id = self.decode_long_long_int(reader)
        if inst_id == 0:
            return None
        inst_name = self.decode_string(reader)
        return Institution(institution_id=inst_id, name=inst_name)
