# PySpark Learning Log: Part 10 - Conditional Operations (The `CASE WHEN` of PySpark)

In data engineering, it is very common to create new columns based on specific conditions. In PySpark, when you want to create a new column based on an `if/else` logic tree (similar to SQL's `CASE WHEN`), you use the `when()` function along with relational and logical operators.

You must import the `when` function from `pyspark.sql.functions`.

## 1. Relational Operators

PySpark uses standard Python relational operators to evaluate conditions within a column:

* `<` (Less than)
* `<=` (Less than or equal to)
* `>` (Greater than)
* `>=` (Greater than or equal to)
* `==` (Equal to)
* `!=` (Not equal to)

## 2. The Basic `when` and `otherwise`

The `when()` function takes two arguments: 
1.  **Condition:** The test using relational operators (e.g., is a column greater than 10?).
2.  **Value:** What to put in the column if the condition is `True`.

The `.otherwise()` method is chained onto the end and acts as the "else" catch-all. If you don't use `.otherwise()`, PySpark inserts `null` for unmatched rows.

```python
from pyspark.sql.functions import col, when

# Scenario: Tagging expensive flights
# IF ticket_price > 500 THEN 'High' ELSE 'Normal'
categorized_df = df.withColumn(
    "price_category",
    when(col("ticket_price") > 500, "High").otherwise("Normal")
)
categorized_df.show()
```

## 3. Chaining Multiple Conditions (IF / ELIF / ELSE)

If you have multiple categories (like a grading scale or tiered logic), you can chain `.when()` methods together. PySpark evaluates them in order from top to bottom.

```python
# Scenario: Creating a tiered loyalty system based on miles flown
loyalty_df = df.withColumn(
    "loyalty_tier",
    when(col("miles") >= 100000, "Gold")
    .when(col("miles") >= 50000, "Silver")
    .when(col("miles") >= 10000, "Bronze")
    .otherwise("Standard")
)
loyalty_df.show()
```
*(Formatting tip: Wrap the whole `when().when().otherwise()` block in parentheses inside `.withColumn()` to put them on separate lines for readability!)*

## 4. Logical Operators (AND / OR / NOT)

You can check conditions across multiple columns at the same time using PySpark's logical operators:
* `&` (AND)
* `|` (OR)
* `~` (NOT)

**CRITICAL RULE:** You **MUST** wrap each individual relational condition in parentheses `()` when using `&`, `|`, or `~`. If you forget the parentheses, Python will misunderstand the order of operations and throw an error.

```python
# Scenario: Finding expensive international flights
# Correct: (condition A) & (condition B)
flagged_df = df.withColumn(
    "flag",
    when(
        (col("ticket_price") > 800) & (col("is_international") == True), 
        "Review Needed"
    ).otherwise("OK")
)

# Scenario: Discounting either cheap flights OR domestic flights
discount_df = df.withColumn(
    "status",
    when(
        (col("ticket_price") <= 100) | (col("is_international") == False),
        "Discount Applied"
    ).otherwise("Regular Price")
)
```

## 5. Referencing Other Columns in the Outcome

The value you return doesn't have to be a static string or number. You can return the value of *another column*, or perform math on another column.

```python
# IF they bought more than 5 items, give a 10% discount on the total
# OTHERWISE, keep the original total.
final_price_df = df.withColumn(
    "final_price",
    when(
        col("items_purchased") > 5, col("order_total") * 0.90
    ).otherwise(col("order_total"))
)
```
