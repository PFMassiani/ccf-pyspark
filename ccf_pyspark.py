import time
from pyspark.rdd import portable_hash
from itertools import groupby, starmap, chain, filterfalse


def partition_by_first_value(key):
  return portable_hash(key[0])


def get_sorted_values(lines):
  groups_with_key_duplicates = groupby(lines, key=lambda x: x[0])
  return groups_with_key_duplicates


def concat_and_min(listval1, listval2):
  return (listval1[0] + listval2[0], min(listval1[1], listval2[1]))


def ccf_iterate_online_min(graph):
  sym = graph.flatMap(lambda edge : [edge, edge[::-1]])
  frmt_for_reduce = sym.map(
    lambda keyval : (keyval[0], ([keyval[1]], keyval[1]))
  )
  grps_mins = frmt_for_reduce.reduceByKey(concat_and_min)
  fltr = grps_mins.filter(
    lambda keyval : keyval[1][1] < keyval[0]
  )
  
  new_pairs = sc.accumulator(0)
  def pair_and_count(keyval):
    minval = keyval[1][1]
    key_min_pair = [(keyval[0], minval)]
    other_pairs = [(val, minval) for val in keyval[1][0] if minval != val]
    new_pairs.add(len(other_pairs))
    return chain(key_min_pair, other_pairs)
  
  return fltr.flatMap(pair_and_count), new_pairs


def ccf_iterate_offline_min(graph):
  sym = graph.flatMap(lambda edge : [edge, edge[::-1]])
  grps = sym.groupByKey()
  grps_mins = grps.map(
    lambda keyval : (keyval[0], (keyval[1], min(keyval[1])))
  )
  fltr = grps_mins.filter(
    lambda keyval : keyval[1][1] < keyval[0]
  )
  
  new_pairs = sc.accumulator(0)
  def pair_and_count(keyval):
    minval = keyval[1][1]
    out = [(value, minval) for value in keyval[1][0] if value != minval]
    new_pairs.add(len(out))
    return [(keyval[0], minval)] + out
  
  return fltr.flatMap(pair_and_count), new_pairs


def ccf_iterate_secondary_sorting(graph):
  sym = graph.flatMap(lambda edge: [edge, edge[::-1]])
  composite_key = sym.map(lambda edge: (edge,None))
  partition_sorted_composite = composite_key.\
    repartitionAndSortWithinPartitions(
      partitionFunc=partition_by_first_value
    )
  partition_sorted = partition_sorted_composite.map(
    lambda compkey:tuple(compkey[0])
  )
  sorted_groups = partition_sorted.mapPartitions(
    get_sorted_values, preservesPartitioning=True
  )
  groups_with_min = sorted_groups.mapValues(
    lambda group: (next(group), group)
  )
  fltr = groups_with_min.filter(
    lambda keymingroup: keymingroup[1][0][1] < keymingroup[0]
  )
  
  new_pairs = sc.accumulator(0)
  def pair_and_count(keymingroup):
    minval = keymingroup[1][0][1]
    key_min_pair = zip([keymingroup[0]],[minval])
    
    def pair_and_increment(duplicated_key, value):
      new_pairs.add(1)
      return (value, minval)
    other_pairs = starmap(
      pair_and_increment,
      keymingroup[1][1]
    )
    return chain(key_min_pair, other_pairs)
  return fltr.flatMap(pair_and_count), new_pairs


def ccf_dedup(graph):
  temp = graph.map(lambda keyval : (keyval, None))
  reduced = temp.reduceByKey(lambda v1,v2 : v1)
  return reduced.map(lambda keyval:keyval[0])


def find_connected_components(graph, method):
  if method=='online':
    ccf_iterate = ccf_iterate_online_min
  elif method=='offline':
    ccf_iterate = ccf_iterate_offline_min
  elif method=='secondary':
    ccf_iterate = ccf_iterate_secondary_sorting
  new_pairs = -1
  n_loops = 0
  while new_pairs != 0:
    graph, acc = ccf_iterate(graph)
    graph = ccf_dedup(graph)
    graph.persist()
    graph.foreach(lambda x:x)
    new_pairs = acc.value
    n_loops += 1
  return graph, n_loops


def run(graph, method, show=False):
  start = time.time()
  output, iterations = find_connected_components(graph, method=method)
  end = time.time()
  if show:
    print(output.collect())
  print('Finding connected components with '
        f'method={method} in {end - start:.3} s '
        f'in {iterations} iterations.')