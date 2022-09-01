"""
Create iGraph graphs from the work indexer
"""
from tqdm.auto import tqdm
import igraph as ig

from src.custom_graph import CustomGraph
from src.index import NewWorkIndexer
from src.utils import Paths, load_pickle, dump_pickle


class _GraphCreator:
    def __init__(self, work_indexer: NewWorkIndexer, paths: Paths, fmt: str = 'graphml', refresh: bool = False):
        self.work_indexer = work_indexer
        self.paths = paths
        self.graphs_path = self.paths.scratch_dir / 'graphs'
        self.fmt = fmt  # format of graphs
        self.processed_works_path = self.graphs_path / 'processed_works.pkl'
        if not refresh and self.processed_works_path.exists():
            self.processed_works = load_pickle(self.processed_works_path)
        else:
            self.processed_works = set()
        print(f'{len(self.processed_works):,} processed works')

        self.cite_ref_g = self.load_graph(kind='cite_ref', refresh=refresh)
        self.related_g = self.load_graph(kind='related', refresh=refresh)
        # self.authorship_g = self.load_graph(kind='authorship', refresh=refresh)
        # self.concept_g = self.load_graph(kind='concept', refresh=refresh)
        # self.venue_g = self.load_graph(kind='venue', refresh=refresh)
        return

    def __len__(self) -> int:
        return len(self.processed_works)

    def load_graph(self, kind: str, refresh: bool = False):
        """
        Load graph from graphmlz
        """
        graph_path = self.graphs_path / f'{kind}.{self.fmt}'
        # g = CustomGraph(kind=kind)
        g = ig.Graph(directed=True)
        if not refresh and graph_path.exists():
            graph_path = str(graph_path)
            g = g.Load(graph_path)
            # if self.fmt == 'graphml':
            #
            # elif self.fmt == 'graphmlz':
            #     g = g.Read_GraphMLz(graph_path)
            # elif self.fmt == 'pickle':
            #     g = g.Read_Pickle(graph_path)
            print(f'Loading {kind!r} graph {g.vcount():,} nodes and {g.ecount():,} edges')
        return g

    def dump_graphs(self):
        """
        Dump all the graphs
        """
        kinds = 'cite_ref', 'related', 'authorship', 'concept', 'venue'

        with tqdm(total=len(kinds), desc='Writing pickles', colour='blue') as pbar:
            for kind in kinds:
                graph_path = str(self.graphs_path / f'{kind}.{self.fmt}')
                g = getattr(self, f'{kind}_g')
                if self.fmt == 'graphmlz':
                    g.write_graphmlz(graph_path)
                elif self.fmt == 'graphml':
                    g.write_graphml(graph_path)
                elif self.fmt == 'pickle':
                    g.write_pickle(graph_path)
                else:
                    raise Exception(f'Invalid format {self.fmt!r}')
                pbar.set_postfix_str(f'{kind!r}', refresh=False)
                pbar.update(1)
        return

    def update_graphs(self, work_id: int):
        """
        Update the graphs with the new work_id
        """
        # work_id = 2105767525
        work = self.work_indexer[work_id]
        vname_work = f'W{work_id}'  # vertex_name for work

        # add nodes
        for g in self.cite_ref_g, self.related_g, self.authorship_g, self.concept_g, self.venue_g:
            g.add_vertex(name=vname_work, date=work.publication_date, work_type=work.type)

        # add references
        ref_nodes, ref_node_attrs = [], {'date': []}
        ref_edges = []
        for ref in work.references:
            if ref not in self.work_indexer:
                continue
            ref_w = self.work_indexer[ref]
            vname_ref = f'W{ref_w.work_id}'
            ref_nodes.append(vname_ref)
            ref_node_attrs['date'].append(ref_w.publication_date)
            # self.cite_ref_g.add_vertex(name=vname_ref, date=ref_w.publication_date)
            ref_edges.append((vname_work, vname_ref))
        self.cite_ref_g.add_vertices(ref_nodes, ref_node_attrs)
        self.cite_ref_g.add_edges(ref_edges)

        # add related works
        rel_nodes, rel_node_attrs = [], {'date': []}
        rel_edges = []
        for rel in work.related_works:
            if rel not in self.work_indexer:
                continue
            rel_w = self.work_indexer[rel]
            vname_rel = f'W{rel_w.work_id}'
            # self.related_g.add_vertex(name=vname_rel, date=rel_w.publication_date)
            rel_nodes.append(vname_rel)
            rel_node_attrs['date'].append(rel_w.publication_date)
            rel_edges.append((vname_work, vname_rel))

        self.related_g.add_vertices(rel_nodes, rel_node_attrs)
        self.related_g.add_edges(rel_edges)

        # authorships
        # 1 for first, 2 for middle, 3 for last
        author_nodes, author_node_attrs = [], {'author_name': [], 'inst_name': []}
        author_edges, author_atts = [], {'position': []}
        for i, author in enumerate(work.authors):
            vname_author = f'A{author.author_id}'
            author_nodes.append(vname_author)
            author_node_attrs['author_name'].append(author.name)
            author_node_attrs['inst_name'].append(None)
            # self.authorship_g.add_vertex(name=vname_author, author_name=author.name)
            # self.authorship_g.add_edge(vname_work, vname_author, wt=i + 1)
            author_edges.append((vname_work, vname_author))
            author_atts['position'].append(author.position)

            # add institutions
            for inst in author.insts:
                if inst is None:
                    break
                vname_inst = f'I{inst.institution_id}'
                author_nodes.append(vname_inst)
                author_node_attrs['author_name'].append(None)
                author_node_attrs['inst_name'].append(inst.name)
                # self.authorship_g.add_vertex(name=vname_inst, inst_name=inst.name)
                # self.authorship_g.add_edge(vname_author, vname_inst)  # add an edge between author and inst
                author_edges.append((vname_author, vname_inst))
                author_atts['position'].append(None)
        self.authorship_g.add_vertices(author_nodes, author_node_attrs)
        self.authorship_g.add_edges(author_edges, author_atts)

        # concepts graph
        concept_nodes, concept_node_attrs = [], {'level': [], 'concept_name': []}
        concept_edges, concept_atts = [], {'wt': []}
        for concept in work.concepts:
            wt = concept.score
            vname_concept = f'C{concept.concept_id}'
            concept_nodes.append(vname_concept)
            concept_node_attrs['concept_name'].append(concept.name)
            concept_node_attrs['level'].append(concept.level)
            # self.concept_g.add_vertex(name=vname_concept, concept_name=concept.name, level=concept.level)
            # self.concept_g.add_edge(vname_work, vname_concept, weight=wt)
            concept_edges.append((vname_work, vname_concept))
            concept_atts['wt'].append(wt)
        self.concept_g.add_vertices(concept_nodes, concept_node_attrs)
        self.concept_g.add_edges(concept_edges, concept_atts)

        # venue graph
        venue_nodes, venue_node_attrs = [], {'venue_name': []}
        venue_edges = []
        if work.venue is not None:
            vname_venue = f'V{work.venue.venue_id}'
            venue_nodes.append(vname_venue)
            venue_node_attrs['venue_name'].append(work.venue.name)
            # self.venue_g.add_vertex(name=vname_venue, venue_name=work.venue.name)
            # self.venue_g.add_edge(vname_work, vname_venue)
            venue_edges.append((vname_work, vname_venue))
        self.venue_g.add_vertices(venue_nodes, venue_node_attrs)
        self.venue_g.add_edges(venue_edges)
        return

    def process_new_entries(self, n: int):
        """
        Process 'n' new entries and write pickles of graphs
        """
        write_every = 1_000  # write graphs every write_every works
        new = 0
        with tqdm(total=n, unit_scale=True, unit=' works', desc='Making graphs', miniters=0, colour='orange') as pbar:
            for work_id in self.work_indexer.offsets:
                if work_id in self.processed_works:
                    continue
                else:
                    self.processed_works.add(work_id)
                    self.update_graphs(work_id=work_id)
                    new += 1
                    pbar.update(1)
                    if new == n:
                        self.dump_graphs()
                        dump_pickle(self.processed_works, self.processed_works_path)
                        break
                    elif new % write_every == 0:
                        self.dump_graphs()
                        dump_pickle(self.processed_works, self.processed_works_path)

        return


class GraphCreator:
    def __init__(self, work_indexer: NewWorkIndexer, paths: Paths, fmt: str = 'graphml', refresh: bool = False):
        self.work_indexer = work_indexer
        self.paths = paths
        self.graphs_path = self.paths.scratch_dir / 'graphs'
        self.fmt = fmt  # format of graphs
        self.processed_works_path = self.graphs_path / 'processed_works.pkl'
        if not refresh and self.processed_works_path.exists():
            self.processed_works = load_pickle(self.processed_works_path)
            print(f'{len(self.processed_works):,} processed works')
        else:
            self.processed_works = set()

        self.cite_ref_g = self.load_graph(kind='cite_ref', refresh=refresh)
        self.related_g = self.load_graph(kind='related', refresh=refresh)
        self.existing_vertices = set()
        return

    def __len__(self) -> int:
        return len(self.processed_works)

    def load_graph(self, kind: str, refresh: bool = False):
        """
        Load graph from graphmlz
        """
        graph_path = self.graphs_path / f'{kind}.{self.fmt}'
        # g = CustomGraph(kind=kind)
        g = ig.Graph(directed=True)
        if not refresh and graph_path.exists():
            graph_path = str(graph_path)
            g = g.Load(graph_path)
            print(f'Loading {kind!r} graph {g.vcount():,} nodes and {g.ecount():,} edges')
        return g

    def dump_graphs(self):
        """
        Dump all the graphs
        """
        kinds = 'cite_ref', 'related'

        with tqdm(total=len(kinds), desc='Writing pickles', colour='blue') as pbar:
            for kind in kinds:
                pbar.set_postfix_str(f'{kind!r}', refresh=False)
                graph_path = str(self.graphs_path / f'{kind}.{self.fmt}')
                g = getattr(self, f'{kind}_g')
                g.save(graph_path)
                pbar.update(1)
        return

    def update_graphs(self, work_id: int):
        """
        Update the graphs with the new work_id
        """
        # work_id = 2105767525
        work = self.work_indexer[work_id]
        vname_work = f'W{work_id}'  # vertex_name for work

        # add nodes

        if vname_work not in self.existing_vertices:
            self.existing_vertices.add(vname_work)
            self.cite_ref_g.add_vertex(name=vname_work, date=work.publication_date, work_type=work.type)
            self.related_g.add_vertex(name=vname_work, date=work.publication_date, work_type=work.type)

        # add references
        ref_edges = []
        for ref in work.references:
            vname_ref = f'W{ref}'
            if ref not in self.work_indexer: # or vname_ref in self.existing_vertices:
                continue
            if vname_ref not in self.existing_vertices:
                self.cite_ref_g.add_vertices(n=vname_ref)
                self.existing_vertices.add(vname_ref)
            # ref_w = self.work_indexer[ref]
            # self.cite_ref_g.add_vertex(name=vname_ref, date=ref_w.publication_date, work_type=ref_w.type)

            ref_edges.append((vname_work, vname_ref))
        self.cite_ref_g.add_edges(ref_edges)

        # add related works
        # rel_edges = []
        # for rel in work.related_works:
        #     vname_rel = f'W{rel}'
        #     if rel not in self.work_indexer: # or vname_rel in self.existing_vertices:
        #         continue
        #     if vname_rel not in self.existing_vertices:
        #         self.related_g.add_vertices(n=vname_rel)
        #         self.existing_vertices.add(vname_rel)
        #     # rel_w = self.work_indexer[rel]
        #     # self.related_g.add_vertex(name=vname_rel, date=rel_w.publication_date, work_type=rel_w.type)
        #     rel_edges.append((vname_work, vname_rel))
        #
        # self.related_g.add_edges(rel_edges)
        return

    def process_new_entries(self, n: int):
        """
        Process 'n' new entries and write pickles of graphs
        """
        write_every = 10_000  # write graphs every write_every works
        new = 0
        with tqdm(total=n, unit_scale=True, unit=' works', desc='Making graphs', miniters=0, colour='orange') as pbar:
            for work_id in self.work_indexer.offsets:
                if work_id in self.processed_works:
                    continue
                else:
                    self.processed_works.add(work_id)
                    self.update_graphs(work_id=work_id)
                    new += 1
                    pbar.update(1)
                    if new == n:
                        self.dump_graphs()
                        dump_pickle(self.processed_works, self.processed_works_path)
                        break
                    elif new % write_every == 0:
                        self.dump_graphs()
                        dump_pickle(self.processed_works, self.processed_works_path)

        return
