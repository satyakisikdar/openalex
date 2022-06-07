import igraph as ig

DESCRIPTIONS = {
    'cite_ref': '''Directed graph linking works with references (out) and citations (in).
    work -> reference''',
    'related': '''Directed graph linking a work with related works.
    work -> related_work''',
    'concepts': '''Weighted directed bipartite graph linking works with concepts.
    work -wt-> concept''',
    'authorships': '''Directed graph linking a work with authors and institutions.
    Links: work -> author, work -> institute, author -> institute'''
}


class CustomGraph(ig.Graph):
    """
    Custom graph container that overloads igraph Graph class
    overloads add_vertex to be more forgiving: add new node if it's not present
    """

    def __init__(self, kind: str, *args, **kwds):
        super().__init__(*args, **kwds)
        self.kind = 'kind'
        self.description = DESCRIPTIONS[kind]
        return

    def has_node(self, name: str) -> bool:
        """
        returns the node object if node with the name exists, else return False
        """
        try:
            v = self.vs.find(name=name)
            present = v
        except ValueError:
            present = False
        return present

    def add_vertex(self, name=None, **kwds):
        """
        Create a new node if needed, else return the existing node
        """
        val = self.has_node(name=name)
        if isinstance(val, bool) and not val:
            return self.add_vertex(name=name, **kwds)
        else:
            return val

    def __str__(self) -> str:
        st = f'{self.kind!r} |V| = {self.vcount():,}\t |E| = {self.ecount():,}\n{self.description}'
        return st
