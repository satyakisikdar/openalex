import sys
sys.path.extend(['../', './'])

from src.entities import Authors, Works, Institutions
from src.utils import Paths


def parse_authors(num_workers):
    paths = Paths()
    authors = Authors(paths=paths)
    print(authors)
    ent = Authors(paths=paths)
    authors.process(num_workers=num_workers)
    return


def parse_works(num_workers):
    paths = Paths()
    works = Works(paths=paths)
    print(works)
    works.process(num_workers=num_workers, max_len=50)
    return


def parse_institutes(num_workers):
    paths = Paths()
    inst = Institutions(paths=paths)
    print(inst)
    inst.process(num_workers=num_workers)
    return


def main():
    # parse_authors(num_workers=4)
    parse_works(num_workers=5)
    # parse_institutes(num_workers=2)
    return


if __name__ == '__main__':
    main()
