# Spark Custom Aggregation Function (UDAF) - Detailed Guide

## Overview
This guide provides a line-by-line explanation of implementing a custom aggregation function in Apache Spark using the DataFrame API. The example demonstrates a custom sum aggregation function.

> **Note**: This code represents a conceptual implementation similar to Scala/Java API. Modern PySpark typically uses `pandas_udf` for custom aggregations.

## Part 1: Defining the Custom Aggregation Function

### Imports
```python
from pyspark.sql.expressions import UserDefinedAggregateFunction 
from pyspark.sql.types import *
```
These imports bring in the necessary classes for creating a UDAF and defining data types.

### Class Definition
```python
class CustomSum(UserDefinedAggregateFunction):
```
Creates a custom aggregation function class that inherits from `UserDefinedAggregateFunction`. This example implements a simple sum aggregation.

### Schema Definition Methods

#### Input Schema
```python
def inputSchema(self): 
    return StructType([StructField("input", IntegerType())])
```
- **Purpose**: Defines the schema of input data to the aggregation function
- **Returns**: A `StructType` with one field named "input" of type `IntegerType`
- This means the function expects to receive integer values as input

#### Buffer Schema
```python
def bufferSchema(self): 
    return StructType([StructField("sum", IntegerType())])
```
- **Purpose**: Defines the schema of the intermediate buffer used during aggregation
- **Returns**: A `StructType` with one field named "sum" that stores the running total
- The buffer is what Spark uses to maintain state during the aggregation process

#### Data Type
```python
def dataType(self): 
    return IntegerType()
```
- **Purpose**: Specifies the data type of the final aggregation result
- **Returns**: `IntegerType`, indicating the final sum will be an integer

#### Deterministic Flag
```python
def deterministic(self): 
    return True
```
- **Purpose**: Indicates whether the function is deterministic
- **Returns**: `True` means given the same input, it always produces the same output
- Important for Spark's optimization and caching strategies

### Aggregation Logic Methods

#### Initialize
```python
def initialize(self, buffer): 
    buffer[0] = 0
```
- **Purpose**: Initializes the buffer at the start of aggregation
- **Action**: Sets the buffer's first (and only) element to 0
- Called once per aggregation group/partition

#### Update
```python
def update(self, buffer, input): 
    buffer[0] += input[0]
```
- **Purpose**: Updates the buffer with a new input row
- **Action**: Adds the input value to the running sum in the buffer
- Called for each row in the group being aggregated

#### Merge
```python
def merge(self, buffer1, buffer2): 
    buffer1[0] += buffer2[0]
```
- **Purpose**: Merges two partial aggregation buffers
- **Action**: Combines results from different partitions by adding buffer2 to buffer1
- Critical for distributed computing - allows Spark to aggregate in parallel

#### Evaluate
```python
def evaluate(self, buffer): 
    return buffer[0]
```
- **Purpose**: Produces the final result from the buffer
- **Action**: Returns the accumulated sum
- Called once at the end to get the final aggregation result

## Part 2: Using the Custom Aggregation Function

### Setting Up Spark Session
```python
from pyspark.sql import SparkSession 
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("CustomAggregation").getOrCreate()
```
Creates a SparkSession, which is the entry point for DataFrame operations.

### Registering the UDAF
```python
custom_sum = CustomSum() 
spark.udf.register("custom_sum", custom_sum)
```
- Creates an instance of the custom aggregation function
- Registers it with Spark SQL under the name "custom_sum"
- Makes it available for use in SQL queries

### Creating Test Data
```python
df = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
```
Creates a DataFrame with one column "value" containing integers 1, 2, and 3.

### Creating SQL View
```python
df.createOrReplaceTempView("values_table")
```
Creates a temporary SQL view named "values_table" that can be queried with SQL.

### Executing the Aggregation
```python
result_df = spark.sql("SELECT custom_sum(value) FROM values_table") 
result_df.show()
```
- Executes a SQL query using the custom aggregation function
- The query calculates the sum of all values (1 + 2 + 3 = 6)
- `show()` displays the result

## How It Works Internally

The aggregation process follows these steps:

1. **Initialization**: Spark creates a buffer with value 0
2. **Updates**: For each row, it calls `update()`:
   - Row 1: buffer = 0 + 1 = 1
   - Row 2: buffer = 1 + 2 = 3
   - Row 3: buffer = 3 + 3 = 6
3. **Merge**: If data is partitioned, Spark merges partial results from different partitions
4. **Evaluate**: Finally returns the buffer value (6)

## Execution Flow Diagram

```
Input Data: [1, 2, 3]
     |
     v
Initialize: buffer = 0
     |
     v
Update with 1: buffer = 0 + 1 = 1
     |
     v
Update with 2: buffer = 1 + 2 = 3
     |
     v
Update with 3: buffer = 3 + 3 = 6
     |
     v
Evaluate: return 6
```

## Important Considerations

### Distributed Execution
- Spark may process data across multiple partitions
- The `merge()` method is crucial for combining partial results
- Each partition maintains its own buffer during processing

### Performance
- Custom aggregations can be slower than built-in functions
- Consider using Spark's native aggregation functions when possible
- For simple operations like sum, use `spark.sql.functions.sum()`

### Modern Alternatives
In PySpark 3.0+, custom aggregations are typically implemented using:
- `pandas_udf` with `GroupedData.applyInPandas()`
- Aggregate functions with `@pandas_udf` decorator
- Native DataFrame API methods

### Example with Modern PySpark
```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType
import pandas as pd

@pandas_udf(returnType=IntegerType())
def custom_sum_pandas(values: pd.Series) -> int:
    return values.sum()

# Usage
df.groupBy().agg(custom_sum_pandas("value").alias("total"))
```

## Conclusion
Understanding UDAF implementation helps in:
- Grasping Spark's distributed aggregation model
- Implementing complex custom aggregations when needed
- Optimizing aggregation performance
- Debugging aggregation-related issues

While the direct `UserDefinedAggregateFunction` API is primarily available in Scala/Java, the concepts apply to all Spark implementations and help understand how aggregations work under the hood.