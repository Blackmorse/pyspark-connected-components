## Connected Components

This repository contains an implementation of the connected components algorithm in PySpark, as described in the [whitepaper](https://dl.acm.org/doi/10.1145/2670979.2670997). The implementation is inspired by [kwartile/connected-component](https://github.com/kwartile/connected-component) and essentially represents a PySpark DataFrame API rewrite of the Scala code.

### Problem Statement

The problem of finding connected components in a graph is a common one, particularly in Big Data. Spark's GraphX module is often used for distributed computations on graphs, emulating the Pregel approach on Spark.

However, there is an opinion, as expressed [here](https://github.com/kwartile/connected-component#description), that such solutions suffer a significant performance degradation on large datasets (billions+ nodes in the graph).

To address this issue, an algorithm for finding connected components in a graph was implemented in PySpark, based on the concepts outlined in the aforementioned [whitepaper](https://dl.acm.org/doi/10.1145/2670979.2670997).

## Implementation

The repository features an implementation of the alternating algorithm for connected components. Feel free to suggest the implementation of two-phase algorithms from the [whitepaper](https://dl.acm.org/doi/10.1145/2670979.2670997). Additionally, the first optimization from the Load Balancing section (3.5) has been applied.

Differences from the [original implementation](https://github.com/kwartile/connected-component#):
1. PySpark instead of Scala 2.11
2. DataFrame API instead of UDF
3. Utilization of dataset caching to avoid recomputation

Tested on PySpark 3.4.1 and 1.5 billin nodes Dataset

### Usage:

The `connected_components` method takes two arguments:
1. `node_pairs` - a DataFrame with two columns representing connections in the graph: `src` and `dst`.
2. `max_iterations_count` - an upper limit on the number of iterations.

Returns:
1. `cc` - DataFrame where each node corresponds to the minimum of its connected component.
2. `did_converge` - whether the algorithm succeeded, i.e., whether it converged or reached `max_iteration_count`.
3. `iter_count` - the number of iterations.

