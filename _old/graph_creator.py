"""
Create iGraph graphs from the work indexer
"""
from tqdm.auto import tqdm

from src.custom_graph import CustomGraph
from src.index import NewWorkIndexer
from src.utils import Paths, load_pickle, dump_pickle


class GraphCreator:
    def __int__(self, work_indexer: NewWorkIndexer, paths: Paths):
        self.work_indexer = work_indexer
        self.paths = paths
        self.graphs_path = self.paths.scratch_dir / 'graphs'

        self.processed_works_path = self.graphs_path / 'processed_works.pkl'
        if self.processed_works_path.exists():
            self.processed_works = load_pickle(self.processed_works_path)
        else:
            self.processed_works = set()

        self.cite_ref_g = self.load_graph(kind='cite_ref', directed=True)
        self.related_g = self.load_graph(kind='related', directed=True)
        self.authorship_g = self.load_graph(kind='authorship', directed=True)
        self.concept_g = self.load_graph(kind='concept', directed=False)
        self.venue_g = self.load_graph(kind='venue', directed=False)
        return

    def load_graph(self, kind: str, **args):
        """
        Load graph from graphmlz
        """
        graph_path = self.graphs_path / f'{kind}.pickle'
        print(f'Loading {kind!r} graph')
        if graph_path.exists():
            g = load_pickle(graph_path)
        else:
            g = CustomGraph(kind=kind, **args)
        return g

    def dump_graphs(self):
        """
        Dump all the graphs
        """
        kinds = 'cite_ref', 'related', 'authorship', 'concept', 'venue'
        with tqdm(total=len(kinds), desc='Writing pickles') as pbar:
            for kind in kinds:
                graph_path = self.graphs_path / f'{kind}.pickle'
                g = getattr(self, f'{kind}_g')
                dump_pickle(g, graph_path)
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
        for ref in work.references:
            ref_w = self.work_indexer[ref]
            vname_ref = f'W{ref_w.work_id}'
            self.cite_ref_g.add_vertex(name=vname_ref, date=ref_w.publication_date)
            self.cite_ref_g.add_edge(vname_work, vname_ref)

        # add related works
        for rel in work.related_works:
            rel_w = self.work_indexer[rel]
            vname_rel = f'W{rel_w.work_id}'
            self.related_g.add_vertex(name=vname_rel, date=rel_w.publication_date)
            self.related_g.add_edge(vname_work, vname_rel)

        # authorships
        # 1 for first, 2 for middle, 3 for last
        for i, author in enumerate(work.authors):
            vname_author = f'A{author.author_id}'
            self.authorship_g.add_vertex(name=vname_author, author_name=author.name)
            self.authorship_g.add_edge(vname_work, vname_author, wt=i + 1)

            # add institutions
            for inst in author.insts:
                vname_inst = f'I{inst.institution_id}'
                self.authorship_g.add_vertex(name=vname_inst, inst_name=inst.name)
                self.authorship_g.add_edge(vname_author, vname_inst)  # add an edge between author and inst

        # concepts graph
        for concept in work.concepts:
            wt = concept.score
            vname_concept = f'C{concept.concept_id}'
            self.concept_g.add_vertex(name=vname_concept, concept_name=concept.name, level=concept.level)
            self.concept_g.add_edge(vname_work, vname_concept, weight=wt)

        # venue graph
        if work.venue is not None:
            vname_venue = f'V{work.venue.venue_id}'
            self.venue_g.add_vertex(name=vname_venue, venue_name=work.venue.name)
            self.venue_g.add_edge(vname_work, vname_venue)

        return

    def process_new_entries(self, n: int):
        """
        Process 'n' new entries and write pickles of graphs
        """
        write_every = 1_000  # write graphs every write_every works
        new = 0
        with tqdm(total=n, unit_scale=True, unit=' works', desc='Making graphs', miniters=0) as pbar:
            for work_id in self.work_indexer.offsets:
                if work_id in self.processed_works:
                    continue
                else:
                    self.processed_works.add(work_id)
                    self.update_graphs(work_id=work_id)
                    new += 1
                    if new % write_every == 0:
                        self.dump_graphs()
                        dump_pickle(self.processed_works, self.processed_works_path)
                    pbar.update(1)
        self.dump_graphs()
        return
