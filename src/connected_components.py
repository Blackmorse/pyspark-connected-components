from pyspark.sql.functions import col, when, collect_list, array,\
        concat, array_min, expr, array_distinct, size, sum, explode, element_at


def small_star(node_pairs, previously_cached):
    neighbors = node_pairs.select(
            when(col("src") > col("dst"), col("src")).otherwise(col("dst")).alias("self"),
            when(col("src") > col("dst"), col("dst")).otherwise(col("src")).alias("neighbor")
        )

    all_neighbors = neighbors.groupBy(col("self")).agg(collect_list(col("neighbor")).alias("neighbors"))

    new_node_pairs_with_change_count = all_neighbors \
        .withColumn("self_neighbors", concat(col("neighbors"), array(col("self")))) \
        .withColumn("minNode", array_min(col("self_neighbors"))) \
        .withColumn("newNodes", expr("filter(self_neighbors, el -> (el <= self AND el != minNode) OR self = el)")) \
        .withColumn("newNodePairs", expr("transform(newNodes, el -> array(el, minNode))")) \
        .withColumn("uniqueNewNodePairs", array_distinct(col("newNodePairs"))) \
        .withColumn("connectivityCountChange", size(expr("array_except(uniqueNewNodePairs, transform(neighbors, el -> array(self, el)))")))\
        .drop("newNodePairs").drop("newNodes").drop("minNode").drop("self_neighbors").drop("neighbors").drop("self")\
        .cache()

    total_connectivity_count_change = new_node_pairs_with_change_count.select(sum("connectivityCountChange").alias("sm")).collect()[0].sm

    new_node_pairs = new_node_pairs_with_change_count.selectExpr("explode(uniqueNewNodePairs) as newNodePairs") \
        .selectExpr("element_at(newNodePairs, 1) as src", "element_at(newNodePairs, 2) as dst")

    if previously_cached is not None:
        previously_cached.unpersist(False)
    return new_node_pairs, total_connectivity_count_change, new_node_pairs_with_change_count


def large_star(node_pairs, previously_cached):
    neighbors = node_pairs.select(
        when(col("src") == col("dst"), array(array(col("src"), col("dst")))).otherwise(array(array(col("src"), col("dst")), array(col("dst"), col("src")))).alias("to_explode")
    ).select(explode("to_explode").alias("pairs"))\
        .select(element_at(col("pairs"), 1).alias("self"), element_at(col("pairs"), 2).alias("neighbor"))\

    all_neighbors = neighbors.groupBy(col("self")).agg(collect_list(col("neighbor")).alias("neighbors"))

    new_node_pairs_with_change_count = all_neighbors \
        .withColumn("self_neighbors", concat(col("neighbors"), array(col("self")))) \
        .withColumn("minNode", array_min(col("self_neighbors"))) \
        .withColumn("newNodes", expr("filter(self_neighbors, el -> el >= self)")) \
        .withColumn("newNodePairs", expr("transform(newNodes, el -> array(el, minNode))")) \
        .withColumn("uniqueNewNodePairs", array_distinct(col("newNodePairs"))) \
        .withColumn("connectivityCountChange", size(expr("array_except(uniqueNewNodePairs, transform(neighbors, el -> array(self, el)))")))\
        .drop("newNodePairs").drop("newNodes").drop("minNode").drop("self_neighbors").drop("neighbors").drop("self")\
        .cache()

    total_connectivity_count_change = new_node_pairs_with_change_count.select(sum("connectivityCountChange").alias("sm")).collect()[0].sm

    new_node_pairs = new_node_pairs_with_change_count.selectExpr("explode(uniqueNewNodePairs) as newNodePairs") \
        .selectExpr("element_at(newNodePairs, 1) as src", "element_at(newNodePairs, 2) as dst")

    if previously_cached is not None:
        previously_cached.unpersist(False)
    return new_node_pairs, total_connectivity_count_change, new_node_pairs_with_change_count


def alternating(node_pairs, large_star_connectivity_change_count, small_star_connectivity_change_count, did_converge,
                     curr_iteration_count, max_iteration_count, cached):
    iteration_count = curr_iteration_count + 1
    if did_converge:
        return node_pairs, True, curr_iteration_count, cached
    elif curr_iteration_count >= max_iteration_count:
        return node_pairs, False, curr_iteration_count, cached
    else:
        node_pairs_large_star, currr_large_star_connectivity_change_count, cached_1 = large_star(node_pairs, cached)
        node_pairs_small_star, curr_small_star_connectivity_change_count, cached_2 = small_star(node_pairs_large_star, cached_1)

        if (currr_large_star_connectivity_change_count == large_star_connectivity_change_count and curr_small_star_connectivity_change_count == small_star_connectivity_change_count) or (curr_small_star_connectivity_change_count == 0 and currr_large_star_connectivity_change_count == 0):
            return alternating(node_pairs_small_star, currr_large_star_connectivity_change_count, curr_small_star_connectivity_change_count, True, iteration_count, max_iteration_count, cached_2)
        else:
            return alternating(node_pairs_small_star, currr_large_star_connectivity_change_count, curr_small_star_connectivity_change_count, False, iteration_count, max_iteration_count, cached_2)


def connected_components(node_pairs, max_iteration_count):
    cc, did_converge, iter_count, cached = alternating(node_pairs, 9999999, 9999999, False, 0, max_iteration_count, None)

    return cc, did_converge, iter_count, cached
