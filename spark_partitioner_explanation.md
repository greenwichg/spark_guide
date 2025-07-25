# Understanding Spark Custom Partitioner Code

This document explains the custom partitioner code step by step to help you understand how it works.

## Code Breakdown

### 1. Import Statement
```python
from pyspark import Partitioner
```
- This imports the base `Partitioner` class from PySpark
- Your custom partitioner must inherit from this class

### 2. Custom Partitioner Class Definition
```python
class CustomPartitioner(Partitioner):
```
- Creates a new class called `CustomPartitioner` that extends the base `Partitioner` class
- This inheritance gives you access to Spark's partitioning framework

### 3. Constructor Method
```python
def __init__(self, num_partitions): 
    self.num_partitions = num_partitions
```
- `__init__` is called when you create a new instance: `CustomPartitioner(3)`
- `num_partitions` parameter tells the partitioner how many partitions to create
- `self.num_partitions = num_partitions` stores this value as an instance variable

### 4. numPartitions Method
```python
def numPartitions(self): 
    return self.num_partitions
```
- **Required method** that Spark calls to know how many partitions this partitioner creates
- Simply returns the number you specified in the constructor
- Spark uses this to set up the correct number of partitions

### 5. getPartition Method
```python
def getPartition(self, key): 
    return hash(key) % self.num_partitions
```
- **Required method** that Spark calls for each key-value pair
- `key` parameter is the key from each (key, value) pair in your RDD
- `hash(key)` converts the key into a number (hash value)
- `% self.num_partitions` uses modulo to ensure result is between 0 and (num_partitions - 1)

**Example:** If you have 3 partitions:
- Key `1`: `hash(1) % 3` might return `1` → goes to partition 1
- Key `2`: `hash(2) % 3` might return `2` → goes to partition 2  
- Key `3`: `hash(3) % 3` might return `0` → goes to partition 0

### 6. Usage Examples

**First Example:**
```python
rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')]) 
partitioned_rdd = rdd.partitionBy(3, CustomPartitioner(3))
```
- Creates an RDD with key-value pairs: `(1, 'a'), (2, 'b'), (3, 'c')`
- `partitionBy(3, CustomPartitioner(3))` applies your custom partitioner
- First `3` is the number of partitions to create
- `CustomPartitioner(3)` creates an instance of your partitioner

**Second Example:**
```python
pair_rdd = rdd.map(lambda x: (x[0], x[1])) 
partitioned_rdd = pair_rdd.partitionBy(3, CustomPartitioner(3))
```
- `rdd.map(lambda x: (x[0], x[1]))` converts each element to a key-value pair
- Since the original RDD already had pairs, this step is actually redundant here
- Then applies the same partitioning as the first example

## How It Works in Practice

Let's trace through what happens:

1. **You create the partitioner:** `CustomPartitioner(3)`
   - `num_partitions = 3` is stored

2. **You call partitionBy:** `rdd.partitionBy(3, CustomPartitioner(3))`
   - Spark creates 3 empty partitions

3. **For each key-value pair, Spark calls getPartition:**
   - For `(1, 'a')`: `getPartition(1)` → `hash(1) % 3` → let's say result is `1`
   - For `(2, 'b')`: `getPartition(2)` → `hash(2) % 3` → let's say result is `2`  
   - For `(3, 'c')`: `getPartition(3)` → `hash(3) % 3` → let's say result is `0`

4. **Final distribution:**
   - Partition 0: `(3, 'c')`
   - Partition 1: `(1, 'a')`
   - Partition 2: `(2, 'b')`

## Complete Code Example

Here's the complete code with comments:

```python
from pyspark import Partitioner 

# Define the custom partitioner class
class CustomPartitioner(Partitioner): 
    def __init__(self, num_partitions): 
        # Store the number of partitions
        self.num_partitions = num_partitions 

    def numPartitions(self): 
        # Tell Spark how many partitions we want
        return self.num_partitions 

    def getPartition(self, key): 
        # Determine which partition each key should go to
        return hash(key) % self.num_partitions

# Usage examples
rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c')]) 
partitioned_rdd = rdd.partitionBy(3, CustomPartitioner(3))

# Alternative approach (redundant in this case)
pair_rdd = rdd.map(lambda x: (x[0], x[1])) 
partitioned_rdd = pair_rdd.partitionBy(3, CustomPartitioner(3))
```

## Key Points to Remember

- **Only works with pair RDDs** (key-value pairs)
- **getPartition must return 0 to (numPartitions-1)**
- **Same key always goes to same partition** (deterministic)
- **Hash function distributes keys roughly evenly** across partitions

## What This Partitioner Does

This particular implementation is essentially recreating Spark's default hash partitioner, but the framework allows you to replace the `hash(key) % self.num_partitions` logic with any custom logic you need.

For example, you could partition by:
- Geographic regions
- Date ranges  
- String length
- Business-specific categories
- Any custom logic that makes sense for your data

The power of custom partitioners lies in replacing that simple hash-based logic with domain-specific partitioning strategies that optimize your particular use case.