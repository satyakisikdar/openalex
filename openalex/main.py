import sys
sys.path.extend(['../', './'])

from openalex.src.entities import Authors
from openalex.src.utils import Paths


def main():
    paths = Paths()
    authors = Authors(paths=paths)
    print(authors)
    authors.process(num_workers=1)
    return


if __name__ == '__main__':
    main()
