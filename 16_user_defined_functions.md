# PySpark Learning Log: Part 14 - User-Defined Functions (UDFs)

While PySpark provides a massive library of built-in functions (like `upper()`, `split()`, `sum()`), you will inevitably encounter business logic that is too complex for standard SQL-like operations. This is where **User-Defined Functions (UDFs)** come in.

UDFs allow you to write custom Python functions and apply them directly to PySpark DataFrame columns.

## 1. Standard Python UDFs

To create a UDF, you write a standard Python function, pass it to PySpark's `udf()` method, and specify the return data type.

```python
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# 1. Define standard Python logic
def categorize_spend(amount):
    if amount == None: return "Unknown"
    if amount < 50: return "Low"
    elif amount < 200: return "Medium"
    else: return "High"

# 2. Register as a PySpark UDF
spend_category_udf = udf(categorize_spend, StringType())

# 3. Use it in a DataFrame pipeline
# df.withColumn("spend_tier", spend_category_udf(col("transaction_amount")))
```

## 2. Pandas (Vectorized) UDFs

Standard UDFs process data row-by-row, which can be slow on massive datasets. **Pandas UDFs** (also known as Vectorized UDFs) use Apache Arrow to process data in batches (Series), offering massive performance improvements.

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Define a Pandas UDF using type hints
@pandas_udf("double")
def apply_tax_vectorized(price_series: pd.Series) -> pd.Series:
    # Operations are performed on the entire batch at once
    return price_series * 1.08 

# Usage is exactly the same:
# df.withColumn("price_with_tax", apply_tax_vectorized(col("base_price")))
```

*Best Practice Note: Always try to use PySpark's built-in functions first, as they are optimized by the Catalyst engine. Use UDFs only when the logic cannot be expressed with native functions.*
