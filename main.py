import argparse
from ccf_pyspark import run

def load_snap_rdd(path):
  rdd = sc.textFile(path).filter(
    lambda s: not s.startswith('#')
  ).map(lambda s: tuple([int(vertex) for vertex in s.split('\t')]))
  rdd.persist()
  return rdd

small_citation_graph = load_snap_rdd('/FileStore/tables/Cit_HepTh-f0eba.txt')
google = load_snap_rdd('/FileStore/tables/web-Google.txt')

toy_graph = sc.parallelize([
  ('A','B'),
  ('B','C'),
  ('B','D'),
  ('D','E'),
  ('F','G'),
  ('G','H'),
])

if __name__=='__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--method', default='offline', help='offline, online, or secondary. Method used for the computation')
  parser.add_argument('--graph', default='toy_graph', help='toy_graph, small_citations_graph, or google. Graph to compute the connected components of.')
  parser.add_argument('--show', default=False, type=bool, help='Whether to show the final RDD')
  args = parser.parse_args()

  if args.graph == 'toy_graph':
    graph = toy_graph
  elif args.graph == 'small_citations_graph':
    graph = small_citations_graph
  elif args.graph == 'google':
    graph = google
  
  run(graph, method=args.method, show=args.show)