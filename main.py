import sys
sys.path.extend(['../', './'])

from src.entities import Authors
from src.utils import Paths

def parse_authors():
    paths = Paths()
    authors = Authors(paths=paths)
    print(authors)
    ent = Authors(paths=paths)
    print(ent.get_finished_files())

    authors.process(num_workers=4)
    return

def main():
    parse_authors()
    return


if __name__ == '__main__':
    main()
