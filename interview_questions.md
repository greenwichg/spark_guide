# Complete Problem Set - Record Count Analysis Steps

## Problem 1: Table Comparison - Record Count Analysis

### Given Data:
- **Table_1**: [1, 1, 1, NULL, NULL]
- **Table_2**: [1, 1, NULL, NULL, NULL]

### Scenario A: FULL OUTER JOIN Analysis
```sql
SELECT * FROM Table_1 
FULL OUTER JOIN Table_2 ON Table_1.value = Table_2.value 
WHERE Table_1.value IS NULL OR Table_2.value IS NULL
```

#### Step-by-Step Analysis:

**Step 1: Understand FULL OUTER JOIN**
- Returns all records from both tables
- Matches records where Table_1.value = Table_2.value
- Includes unmatched records with NULL values for missing side

**Step 2: Perform the JOIN Operation**
- Table_1 ones (3) × Table_2 ones (2) = 6 matched pairs
- Table_1 NULLs (2) get paired with all Table_2 records = 10 pairs
- Table_2 NULLs (3) get paired with all Table_1 records = 15 pairs
- Remove duplicates from cross-product

**Step 3: Apply WHERE Filter**
`WHERE Table_1.value IS NULL OR Table_2.value IS NULL`

**Result: 7 records** (differences between tables)

---

### Scenario B: INNER JOIN Analysis
```sql
SELECT * FROM Table_1 
INNER JOIN Table_2 ON Table_1.value = Table_2.value
```

#### Step-by-Step Analysis:

**Step 1: Understand INNER JOIN**
- Returns only records that have matches in both tables
- NULLs don't match with anything (including other NULLs)

**Step 2: Identify Matching Values**
- Table_1 has: 1, 1, 1 (3 ones)
- Table_2 has: 1, 1 (2 ones)
- Only value '1' appears in both tables

**Step 3: Calculate Cross Product of Matches**
- Table_1 ones (3) × Table_2 ones (2) = 6 combinations

**Result: 6 records** (all combinations of matching '1' values)

---

### Scenario C: LEFT JOIN Analysis
```sql
SELECT * FROM Table_1 
LEFT JOIN Table_2 ON Table_1.value = Table_2.value
```

**Result: 5 records** (all Table_1 records, with matches where available)

---

## Problem 2: PySpark Date Range Analysis

### Objective: Find missing dates for each ID within their date range

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, min, max, sequence, explode

# Input data
data = [(1, "2024-01-01"), (1, "2024-01-03"), (1, "2024-01-04"), 
        (2, "2024-02-01"), (2, "2024-02-02")]

# Step 1: Create DataFrame and convert date column
spark = SparkSession.builder.appName("DateRangeAnalysis").getOrCreate()
df = spark.createDataFrame(data, ["ID", "date"])
df = df.withColumn("date", to_date(col("date")))

print("Original DataFrame:")
df.show()
# +---+----------+
# | ID|      date|
# +---+----------+
# |  1|2024-01-01|
# |  1|2024-01-03|
# |  1|2024-01-04|
# |  2|2024-02-01|
# |  2|2024-02-02|
# +---+----------+

# Step 2: Find min/max dates per ID
date_range = df.groupBy("ID").agg(
    min(col("date")).alias("start_date"), 
    max(col("date")).alias("end_date")
)

print("Date ranges per ID:")
date_range.show()
# +---+----------+----------+
# | ID|start_date|  end_date|
# +---+----------+----------+
# |  1|2024-01-01|2024-01-04|
# |  2|2024-02-01|2024-02-02|
# +---+----------+----------+

# Step 3: Generate complete date sequence for each ID
date_range = date_range.withColumn(
    "all_dates", 
    sequence(col("start_date"), col("end_date"))
)

print("With complete date sequences:")
date_range.show(truncate=False)
# +---+----------+----------+------------------------------------------------+
# | ID|start_date|  end_date|                                       all_dates|
# +---+----------+----------+------------------------------------------------+
# |  1|2024-01-01|2024-01-04|[2024-01-01, 2024-01-02, 2024-01-03, 2024-01-04]|
# |  2|2024-02-01|2024-02-02|                        [2024-02-01, 2024-02-02]|
# +---+----------+----------+------------------------------------------------+

# Explode the date sequences to individual rows
all_dates_df = date_range.select(
    col("ID"), 
    explode(col("all_dates")).alias("date")
)

print("All possible dates per ID:")
all_dates_df.show()
# +---+----------+
# | ID|      date|
# +---+----------+
# |  1|2024-01-01|
# |  1|2024-01-02|  ← This date is missing in original data
# |  1|2024-01-03|
# |  1|2024-01-04|
# |  2|2024-02-01|
# |  2|2024-02-02|
# +---+----------+

# Step 4: Use left_anti join to find missing dates
missing_dates_df = all_dates_df.join(df, ["ID", "date"], "left_anti")

print("Missing dates:")
missing_dates_df.show()
# +---+----------+
# | ID|      date|
# +---+----------+
# |  1|2024-01-02|
# +---+----------+

# Record count analysis:
print(f"Original records: {df.count()}")           # 5 records
print(f"All possible dates: {all_dates_df.count()}") # 6 records (4 for ID=1, 2 for ID=2)
print(f"Missing dates: {missing_dates_df.count()}")  # 1 record (2024-01-02 for ID=1)
```

**Record Count Analysis:**
- **Original DataFrame**: 5 records
- **All possible dates**: 6 records (4 days for ID=1, 2 days for ID=2)
- **Missing dates**: 1 record (ID=1 missing 2024-01-02)

**Steps to Calculate Expected Records:**
1. For each ID, calculate date range: end_date - start_date + 1
2. Sum all date ranges: ID=1 has 4 days, ID=2 has 2 days = 6 total
3. Missing records = All possible dates - Original records = 6 - 5 = 1

---

## Problem 3: SQL Sales Analysis

### Objective: Get oldest and latest sales amounts per product

```sql
WITH ranked AS (
    SELECT 
        product, date, sales_amount,
        ROW_NUMBER() OVER (PARTITION BY product ORDER BY date ASC) as rn_oldest,
        ROW_NUMBER() OVER (PARTITION BY product ORDER BY date DESC) as rn_latest
    FROM sales
)
SELECT 
    product, 
    MAX(CASE WHEN rn_oldest = 1 THEN sales_amount END) as oldest_sales_amount, 
    MAX(CASE WHEN rn_latest = 1 THEN sales_amount END) as latest_sales_amount 
FROM ranked 
GROUP BY product;
```

**Expected Records**: One record per unique product

---

## Problem 4: Explode Functionality

### Objective: Expand rows based on quantity

**Input Data:**
```
order   prd    quantity
ord1    prd1   3
ord2    prd2   2
ord3    prd3   1
```

**Expected Output:**
```
order   prd    quantity
ord1    prd1   1 
ord1    prd1   1
ord1    prd1   1
ord2    prd2   1
ord2    prd2   1
ord3    prd3   1
```

### PySpark Approach:
```python
from pyspark.sql.functions import expr

# Transform quantity into individual rows
exploded_df = df.select(
    "order", "prd", 
    expr("explode(array_repeat(1, quantity))").alias("quantity")
)
```

### SQL Approaches:

#### Method 1: Recursive CTE (PostgreSQL, SQL Server, etc.)
```sql
WITH RECURSIVE exploded AS (
    -- Base case: start with quantity = 1 for each row
    SELECT order_col, prd, 1 as quantity, quantity as original_qty
    FROM orders
    
    UNION ALL
    
    -- Recursive case: add rows until we reach original quantity
    SELECT order_col, prd, quantity + 1, original_qty
    FROM exploded
    WHERE quantity < original_qty
)
SELECT order_col, prd, 1 as quantity
FROM exploded
ORDER BY order_col, prd;
```

#### Method 2: Using Numbers Table/Sequence (Most SQL Databases)
```sql
-- Create a numbers table or use existing sequence
WITH numbers AS (
    SELECT 1 as n UNION ALL SELECT 2 UNION ALL SELECT 3 
    UNION ALL SELECT 4 UNION ALL SELECT 5  -- extend as needed
),
exploded AS (
    SELECT o.order_col, o.prd, 1 as quantity
    FROM orders o
    JOIN numbers n ON n.n <= o.quantity
)
SELECT order_col, prd, quantity
FROM exploded
ORDER BY order_col, prd;
```

#### Method 3: Using GENERATE_SERIES (PostgreSQL)
```sql
SELECT 
    o.order_col,
    o.prd,
    1 as quantity
FROM orders o
CROSS JOIN generate_series(1, o.quantity);
```

#### Method 4: Using UNNEST with Array (BigQuery/PostgreSQL)
```sql
SELECT 
    order_col,
    prd,
    1 as quantity
FROM orders,
UNNEST(GENERATE_ARRAY(1, quantity)) as pos;
```

#### Method 5: Self-Join Approach (MySQL/Traditional SQL)
```sql
-- For small quantities, use multiple self-joins
SELECT o1.order_col, o1.prd, 1 as quantity FROM orders o1 WHERE o1.quantity >= 1
UNION ALL
SELECT o2.order_col, o2.prd, 1 as quantity FROM orders o2 WHERE o2.quantity >= 2
UNION ALL  
SELECT o3.order_col, o3.prd, 1 as quantity FROM orders o3 WHERE o3.quantity >= 3;
-- Continue for expected max quantity
```

**Record Count Calculation**: 
- Sum of all quantity values in original data
- Input: 3 + 2 + 1 = 6 total records expected
- Each original row becomes N rows where N = its quantity value

---

## Problem 5: Count Character Occurrences

### Objective: Count 'a' occurrences in names

```python
from pyspark.sql.functions import col, length, regexp_replace, lower

df_with_count = df.withColumn(
    "a_count", 
    length(col("name")) - length(regexp_replace(lower(col("name")), "a", ""))
)
```

**Expected Records**: Same as input records, with additional count column

---

## General Steps for Record Count Analysis:

1. **Identify Operation Type** (JOIN, GROUP BY, EXPLODE, etc.)
2. **Understand Data Relationships** (one-to-one, one-to-many, many-to-many)
3. **Calculate Cartesian Products** for JOINs
4. **Apply Filters and Conditions** (WHERE, HAVING clauses)
5. **Account for Aggregations** (GROUP BY reduces records)
6. **Consider NULL Handling** (affects JOIN results)
7. **Verify with Sample Data** when possible