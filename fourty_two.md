# Processing Multiple Kafka Topics with Different Schemas in Spark

## The Challenge

When working with multiple Kafka topics in a single Spark application, each topic often has its own data schema. This presents challenges because:
- Kafka stores data as binary (byte arrays)
- Different topics may have different serialization formats (JSON, Avro, Protobuf, etc.)
- You need to apply the correct deserialization logic based on which topic the data came from

## Detailed Solution Breakdown

### 1. Reading from Multiple Topics

```python
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
    .option("subscribe", "topic1,topic2") \
    .load()
```

**What happens here:**
- `readStream`: Creates a streaming DataFrame that continuously reads new data
- `format("kafka")`: Specifies the Kafka data source
- `kafka.bootstrap.servers`: Lists Kafka brokers to connect to
- `subscribe`: Comma-separated list of topics to subscribe to (you can also use `subscribePattern` for regex patterns)

**The resulting DataFrame schema:**
```
root
 |-- key: binary
 |-- value: binary
 |-- topic: string
 |-- partition: integer
 |-- offset: long
 |-- timestamp: timestamp
 |-- timestampType: integer
```

### 2. Deserializing and Parsing Messages

```python
from pyspark.sql.functions import col, when

parsed_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("topic", col("topic").cast("string")) \
    .withColumn("parsed_value", 
        when(col("topic") == "topic1", parse_topic1_udf(col("value")))
        .when(col("topic") == "topic2", parse_topic2_udf(col("value")))
    )
```

**Breaking this down:**
- `selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")`: Converts binary key/value to strings
- The `topic` column tells us which Kafka topic each record came from
- `when()` statements create conditional logic to apply different parsing based on the topic
- `parse_topic1_udf` and `parse_topic2_udf` are User Defined Functions (UDFs) containing schema-specific parsing logic

**Example UDF implementation:**
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json

# Define schema for topic1
topic1_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Create UDF for topic1
@udf(returnType=topic1_schema)
def parse_topic1_udf(value):
    try:
        data = json.loads(value)
        return (data.get("user_id"), 
                data.get("event_type"), 
                data.get("timestamp"))
    except:
        return (None, None, None)

# Similarly for topic2 with different schema
topic2_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("quantity", IntegerType(), True)
])

@udf(returnType=topic2_schema)
def parse_topic2_udf(value):
    try:
        data = json.loads(value)
        return (data.get("product_id"), 
                data.get("price"), 
                int(data.get("quantity", 0)))
    except:
        return (None, None, None)
```

### 3. Processing Each Schema Separately

```python
topic1_df = parsed_df.filter(col("topic") == "topic1")
topic2_df = parsed_df.filter(col("topic") == "topic2")
```

**What this achieves:**
- Creates separate DataFrames for each topic
- Each DataFrame now has records with the appropriate schema
- You can apply topic-specific transformations, aggregations, or business logic

## Enhanced Approach with Better Schema Handling

Here's a more robust implementation using `from_json` instead of UDFs:

```python
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schemas for each topic
topic1_schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", StringType())
])

topic2_schema = StructType([
    StructField("product_id", StringType()),
    StructField("price", StringType()),
    StructField("quantity", IntegerType())
])

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic1,topic2") \
    .load()

# Parse based on topic
parsed_df = kafka_df.select(
    col("topic"),
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("timestamp")
).withColumn("parsed_data",
    when(col("topic") == "topic1", 
         from_json(col("value"), topic1_schema))
    .when(col("topic") == "topic2", 
          from_json(col("value"), topic2_schema))
)

# Process each topic
topic1_processed = parsed_df \
    .filter(col("topic") == "topic1") \
    .select(
        col("parsed_data.user_id"),
        col("parsed_data.event_type"),
        col("parsed_data.timestamp")
    )

topic2_processed = parsed_df \
    .filter(col("topic") == "topic2") \
    .select(
        col("parsed_data.product_id"),
        col("parsed_data.price"),
        col("parsed_data.quantity")
    )

# Write outputs
query1 = topic1_processed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query2 = topic2_processed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
```

## Key Considerations

### 1. Performance
- Using `from_json` is generally more efficient than UDFs
- Consider partitioning strategy for better parallelism
- Monitor resource usage when processing multiple topics

### 2. Error Handling
```python
# Add error handling for malformed messages
parsed_df = kafka_df.select(
    col("topic"),
    col("value").cast("string").alias("value")
).withColumn("parsed_data",
    when(col("topic") == "topic1", 
         from_json(col("value"), topic1_schema))
    .otherwise(None)
).filter(col("parsed_data").isNotNull())  # Filter out parsing errors
```

### 3. Schema Evolution
- Consider using a schema registry for managing schema changes
- Implement backward compatibility strategies
- Version your schemas appropriately

### 4. Watermarking for Late Data
```python
# Add watermarks for handling late data
parsed_df_with_watermark = parsed_df \
    .withWatermark("timestamp", "10 minutes")
```

### 5. Checkpointing for Fault Tolerance
```python
query = parsed_df.writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .format("parquet") \
    .start("/path/to/output")
```

## Alternative Approaches

### 1. Separate Streams
Create separate streaming queries for each topic:
```python
# Stream for topic1
topic1_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic1") \
    .load()

# Stream for topic2
topic2_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic2") \
    .load()
```

### 2. Union Approach
Parse all topics to a common schema with a discriminator column:
```python
# Define a common schema with all possible fields
common_schema = StructType([
    StructField("topic_type", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("price", StringType()),
    StructField("quantity", IntegerType()),
    StructField("timestamp", StringType())
])
```

### 3. Schema Registry Integration
Use Confluent Schema Registry with Avro:
```python
# Example with schema registry
from pyspark.sql.avro.functions import from_avro

# Configure schema registry
schema_registry_url = "http://localhost:8081"

# Read with Avro deserialization
df = kafka_df.select(
    col("topic"),
    from_avro(
        col("value"), 
        schema_registry_url, 
        col("topic")  # Use topic name to lookup schema
    ).alias("data")
)
```

## Best Practices

1. **Monitoring**: Implement comprehensive monitoring for each topic stream
2. **Testing**: Unit test your parsing logic separately
3. **Documentation**: Document schema changes and version history
4. **Resource Management**: Monitor and tune Spark resources based on topic volumes
5. **Data Quality**: Implement data quality checks post-parsing
6. **Dead Letter Queue**: Route failed parsing attempts to a separate topic for investigation

## Conclusion

This solution provides a flexible and scalable way to handle multiple Kafka topics with different schemas in a single Spark application. The key is to:
- Use the topic metadata to route messages to appropriate parsing logic
- Maintain clean separation between different schemas
- Implement proper error handling and monitoring
- Choose the approach that best fits your use case and performance requirements