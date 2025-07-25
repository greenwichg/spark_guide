# Handling Data Skew in Apache Spark - Complete Guide

## Understanding Data Skew

Data skew occurs when certain keys have disproportionately more data than others. For example, if you're processing user activity data and one user has millions of events while others have just a few, that creates skew. This leads to:

- Some partitions being overloaded while others are underutilized
- Slower overall job performance
- Potential out-of-memory errors on heavily loaded partitions

## 1. Salting Technique

**What it does:** Artificially distributes skewed keys across multiple partitions by adding random suffixes.

```python
from pyspark.sql.functions import col, concat, lit, rand

# Original skewed data
# key "popular_user" appears millions of times
# key "regular_user1" appears 100 times

# Add salt (random number 0-9) to create multiple versions of each key
salted_df = df.withColumn("salted_key", 
                         concat(col("key"), lit("_"), (rand() * 10).cast("int")))

# Now "popular_user" becomes:
# "popular_user_0", "popular_user_1", ..., "popular_user_9"
# Each salted key gets roughly 1/10th of the original data
```

**When to use:** When you have a few keys with massive amounts of data that can't be easily avoided.

**Trade-offs:** 
- **Pros:** Distributes load evenly
- **Cons:** Makes downstream operations more complex (you need to handle the salted keys)

## 2. Repartition Skewed Keys

**What it does:** Identifies problematic keys and redistributes only those keys across partitions.

```python
# Step 1: Find skewed keys (keys with more than threshold records)
threshold = 1000000  # 1 million records
skewed_keys = df.groupBy("key").count().filter(f"count > {threshold}").select("key")

# Step 2: Separate skewed data from normal data
skewed_df = df.join(skewed_keys, "key", "inner")  # Only skewed data
normal_df = df.join(skewed_keys, "key", "left_anti")  # Non-skewed data

# Step 3: Repartition only the skewed data
balanced_skewed_df = skewed_df.repartition("key")

# Step 4: Union back together
final_df = balanced_skewed_df.union(normal_df)
```

**When to use:** When you can identify specific keys causing problems and want targeted optimization.

**Benefits:** More efficient than salting because it only affects problematic keys.

## 3. Map-Side Joins (Broadcast Joins)

**What it does:** Sends the smaller dataset to all nodes, eliminating the need to shuffle the large, skewed dataset.

```python
from pyspark.sql.functions import broadcast

# Instead of shuffling both datasets and risking skew
# result_df = large_df.join(small_df, "key")  # BAD: causes shuffle

# Broadcast the small dataset to all nodes
result_df = large_df.join(broadcast(small_df), "key")  # GOOD: no shuffle needed
```

**How it works:**
- **Normal join:** Both datasets are shuffled by join key, causing skew problems
- **Broadcast join:** Small dataset is copied to every node, large dataset stays put

**Requirements:** 
- Small dataset must fit in memory on each node
- Spark automatically broadcasts tables < 200MB (configurable)

**When to use:** Any time you're joining a large skewed dataset with a smaller one.

## 4. Optimize Partitioning

**What it does:** Adjusts the number of partitions to better distribute data.

```python
# Check current partition distribution
print(f"Current partitions: {df.rdd.getNumPartitions()}")

# Repartition to distribute data more evenly
optimal_partitions = 200  # Rule of thumb: 2-3x number of CPU cores
df_repartitioned = df.repartition(optimal_partitions, "key")

# Alternative: Use coalesce to reduce partitions (doesn't trigger full shuffle)
df_coalesced = df.coalesce(100)  # Only reduces partitions
```

**Key considerations:**
- **Too few partitions:** Some partitions become overloaded
- **Too many partitions:** Overhead of managing many small partitions
- **Sweet spot:** Usually 2-3x your total CPU cores across the cluster

## Complete Example: Handling E-commerce Data Skew

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, broadcast, concat, lit, rand, split, sum

spark = SparkSession.builder.appName("DataSkewExample").getOrCreate()

# Sample: Order data where some products are very popular
orders_df = spark.read.parquet("orders.parquet")
products_df = spark.read.parquet("products.parquet")

# Problem: Product "iPhone" has millions of orders, others have few
# This will cause skew in joins and aggregations

# Solution 1: Use broadcast join for product lookup
result = orders_df.join(broadcast(products_df), "product_id")

# Solution 2: For aggregations, use salting for skewed products
# First, identify skewed products
skewed_products = (orders_df
                  .groupBy("product_id")
                  .count()
                  .filter("count > 100000")
                  .select("product_id"))

# Apply salting to skewed products
salted_orders = (orders_df
                .join(skewed_products, "product_id", "inner")
                .withColumn("salted_product_id", 
                           concat(col("product_id"), lit("_"), 
                                 (rand() * 20).cast("int"))))

# Process normal products separately
normal_orders = orders_df.join(skewed_products, "product_id", "left_anti")

# Aggregate both separately, then combine results
salted_agg = salted_orders.groupBy("salted_product_id").agg(count("*").alias("order_count"))
normal_agg = normal_orders.groupBy("product_id").agg(count("*").alias("order_count"))

# Clean up salted results and union
final_agg = (salted_agg
            .withColumn("product_id", split(col("salted_product_id"), "_")[0])
            .groupBy("product_id")
            .agg(sum("order_count").alias("total_orders"))
            .union(normal_agg.select("product_id", col("order_count").alias("total_orders"))))
```

## Advanced Techniques

### Adaptive Query Execution (AQE)
Spark 3.0+ includes Adaptive Query Execution which can automatically handle some skew scenarios:

```python
# Enable AQE (usually enabled by default in Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

### Custom Partitioner
For complex scenarios, you can implement custom partitioning logic:

```python
from pyspark import Partitioner

class CustomPartitioner(Partitioner):
    def __init__(self, num_partitions, skewed_keys):
        self.num_partitions = num_partitions
        self.skewed_keys = set(skewed_keys)
    
    def getPartition(self, key):
        if key in self.skewed_keys:
            # Distribute skewed keys across multiple partitions
            return hash(key) % (self.num_partitions // 2)
        else:
            # Normal distribution for regular keys
            return (hash(key) % (self.num_partitions // 2)) + (self.num_partitions // 2)

# Apply custom partitioner
custom_partitioner = CustomPartitioner(200, ["popular_key1", "popular_key2"])
rdd_with_custom_partitions = df.rdd.partitionBy(custom_partitioner)
```

## Monitoring and Detection

### Identifying Data Skew
```python
# Check partition sizes
partition_sizes = df.rdd.glom().map(len).collect()
print(f"Partition sizes: {partition_sizes}")
print(f"Max partition size: {max(partition_sizes)}")
print(f"Min partition size: {min(partition_sizes)}")
print(f"Skew ratio: {max(partition_sizes) / min(partition_sizes) if min(partition_sizes) > 0 else 'Infinite'}")

# Identify top keys by frequency
key_distribution = df.groupBy("key").count().orderBy(col("count").desc())
key_distribution.show(20)
```

### Spark UI Monitoring
Monitor these metrics in Spark UI:
- **Task duration variance:** High variance indicates skew
- **Shuffle read/write sizes:** Uneven distribution suggests skew
- **GC time:** Excessive GC on some executors indicates memory pressure from skew

## Choosing the Right Technique

| Scenario | Recommended Technique | Why |
|----------|----------------------|-----|
| Join with small dataset | Broadcast Join | Eliminates shuffle entirely |
| Few extremely hot keys | Salting | Distributes load evenly |
| Many moderately skewed keys | Repartitioning | Targeted optimization |
| General performance tuning | Partition optimization | Baseline improvement |
| Unknown skew patterns | Enable AQE | Automatic detection and handling |

## Best Practices

1. **Start with broadcast joins** - They're the most effective when applicable
2. **Monitor before optimizing** - Use Spark UI to identify actual bottlenecks
3. **Test with realistic data** - Skew patterns in production may differ from test data
4. **Combine techniques** - Often multiple approaches work better together
5. **Consider data preprocessing** - Sometimes fixing skew at the source is most effective
6. **Use AQE when available** - Spark 3.0+ can handle many cases automatically

## Performance Impact

| Technique | Setup Overhead | Runtime Improvement | Memory Impact |
|-----------|---------------|-------------------|---------------|
| Broadcast Join | Low | High (10-100x) | Moderate |
| Salting | Medium | High (5-50x) | Low |
| Repartitioning | Low | Medium (2-10x) | Low |
| Custom Partitioner | High | Variable | Low |

The key is often combining multiple techniques - use broadcast joins where possible, salt the remaining hot keys, and tune your partitioning for optimal performance.