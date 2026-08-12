# PySpark Learning Log: Part 21 - User Defined Functions (UDFs)

PySpark provides hundreds of built-in functions (like `sum()`, `when()`, `concat()`). Because these run directly in the JVM (Java Virtual Machine) backend of Spark, they are incredibly fast. 

However, sometimes you need to apply complex custom Python logic, machine learning models, or external library transformations to a column. To do this, you must register a **User Defined Function (UDF)**.

## 1. Standard Python UDFs (Avoid if possible)

Standard UDFs work, but they are notoriously slow. Spark has to serialize the data, send it from the JVM to a Python process *row by row*, run your function, and send it back.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# 1. Write standard Python logic
def categorize_temperature(temp):
    if temp is None:
        return "Unknown"
    elif temp > 100:
        return "Critical"
    return "Normal"

# 2. Register as a UDF and specify the exact return type
temp_udf = udf(categorize_temperature, StringType())

# 3. Apply to the DataFrame
df = df.withColumn("temp_status", temp_udf(col("temperature")))
```

## 2. Pandas UDFs (Vectorized UDFs - The Modern Standard)

To solve the performance bottleneck, Spark introduced **Pandas UDFs** (leveraging Apache Arrow). Instead of processing data row-by-row, Spark sends data to Python in massive *batches* (Pandas Series). 

This allows you to use highly optimized, vectorized Pandas operations. **If you must use a UDF, always use a Pandas UDF.**

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

# 1. Use the @pandas_udf decorator and specify the return type
# 2. Type hints (pd.Series) are highly recommended
@pandas_udf(StringType())
def categorize_temp_vectorized(temp_series: pd.Series) -> pd.Series:
    # We apply Pandas vectorized logic to the entire batch at once!
    # No "for" loops allowed inside this function!
    
    # Example using pandas .apply (or numpy/pandas vectorization)
    def logic(t):
        if pd.isna(t): return "Unknown"
        return "Critical" if t > 100 else "Normal"
        
    return temp_series.apply(logic)

# 3. Apply it exactly like a normal PySpark function
df = df.withColumn("temp_status", categorize_temp_vectorized(col("temperature")))
```

**Key Takeaway:** Only use UDFs when PySpark's built-in `pyspark.sql.functions` cannot solve your problem. When you do need them, default to `@pandas_udf`.
