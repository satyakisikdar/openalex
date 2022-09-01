import igraph as ig

DESCRIPTIONS = {
    'cite_ref':
        'Directed graph linking works with references (out) and cited_by_count (in)',
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

    def __init__(self, kind: str = '', *args, **kwds):
        self.kind = kind
        self.vertex_names = set()  # this is to speedup name lookups
        super().__init__(*args, **kwds)
        return

    def has_vertex(self, name: str) -> bool:
        """
        returns True if node with the name exists, else return False
        """
        return name in self.vertex_names

    def add_vertex(self, name=None, **kwds):
        """
        Create a new node if needed, else return the existing node
        """
        if self.has_vertex(name=name):
            # print(f'Existing vertex {name!r} found')
            return
        else:
            self.vertex_names.add(name)
            return super().add_vertex(name=name, **kwds)

    def __str__(self) -> str:
        description = DESCRIPTIONS.get(self.kind, '')
        st = f'Name: {self.kind!r} {self.summary()}\nDescription: {description!r}'
        return st

    def Read_Pickle(self, fname=None):
        g = super().Read_Pickle(fname)
        g.vertex_names = set(g.vs['name'])
        g.kind = self.kind
        return g

    def Read_GraphMLz(self, f, index=0):
        g = super().Read_GraphMLz(f)
        g.vertex_names = set(g.vs['name'])
        g.kind = self.kind
        return g

    def Read_GraphML(self, f, index=0):
        g = super().Read_GraphML(f)
        g.vertex_names = set(g.vs['name'])
        g.kind = self.kind
        return g

    def neighbors(self, *args, **kwargs):
        """
        Return the names of vertices that are neighbors
        """
        return [self.vs[nbr]['name'] for nbr in super().neighbors(*args, **kwargs)]
