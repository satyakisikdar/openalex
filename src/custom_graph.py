import igraph as ig

DESCRIPTIONS = {
    'cite_ref':
        'Directed graph linking works with references (out) and citations (in)',
    'related':
        'Directed graph linking a work with related works.',
    'concept':
        'Weighted bipartite graph linking works with concepts.',
    'authorship':
        'Directed graph linking a work with authors and institutions.\nwt = 1: first, 2: middle, 3: last',
    'venue':
        'Graph linking work with a venue.'
}


class CustomGraph(ig.Graph):
    """
    Custom graph container that overloads igraph Graph class
    overloads add_vertex to be more forgiving: add new node if it's not present
    """

    def __init__(self, kind: str, *args, **kwds):
        super().__init__(*args, **kwds)
        self.kind = kind
        self.description = DESCRIPTIONS[kind]
        return

    def has_vertex(self, name: str) -> bool:
        """
        returns True if node with the name exists, else return False
        """
        try:
            self.vs.find(name)
            present = True
        except (ValueError, KeyError):
            present = False
        return present

    def add_vertex(self, name=None, **kwds):
        """
        Create a new node if needed, else return the existing node
        """
        if self.has_vertex(name=name):
            # print(f'Existing vertex {name!r} found')
            return
        else:
            return super().add_vertex(name=name, **kwds)

    def __str__(self) -> str:
        st = f'Name: {self.kind!r} {self.summary()}\nDescription: {self.description!r}'
        return st

    def neighbors(self, *args, **kwargs):
        """
        Return the names of vertices that are neighbors
        """
        return [self.vs[nbr]['name'] for nbr in super().neighbors(*args, **kwargs)]
