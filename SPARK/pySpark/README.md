![alt text](http://datascience-enthusiast.com/figures/spark_python.PNG "SparkSQL Logo")
# pySpark Overview
The Spark Python API (PySpark) exposes the Spark programming model to Python.
There are a few key differences between the Python and Scala APIs:

Python is dynamically typed, so RDDs can hold objects of multiple types.
PySpark does not yet support a few API calls, such as lookup and non-text input files, though these will be added in future releases. In PySpark, RDDs support the same methods as their Scala counterparts but take Python functions and return Python collection types.

### Resources
Programming Guides - 
* https://spark.apache.org/docs/0.9.0/python-programming-guide.html
* http://spark.apache.org/docs/latest/sql-programming-guide.html

Pyspark functions - http://takwatanabe.me/pyspark/generated/sql.functions.html

### Spark's Base Unit - DataFrame ###
Like SparkSQL the base unit used in pySpark is a DataFrame.
A DataFrame is a Dataset organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood. DataFrames can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDDs.

### Usage ###
To use pySpark: 
* At the command line one may type `pyspark`
* To run a pyspark script  `spark-submit <insert filename>.py`

### Handling UPDATE, or DELETE in translation
When translating from PLSQL to pySpark one must make note of the fast that there is not `UPDATE` or `DELETE` functionality in Spark. This is because Dataframes are immutable by design.

Instead, as engineers we are responsible for translating these `UPDATE` / `DELETE` itms into `SELECT` statements. Instead up updating a record, one might wait to include the given record unil the end, and then when composing the resulting DataFrame, make the change then.

### Examples ###
    # The entry point into all functionality in Spark is the SparkSession class.
    #  To create a basic SparkSession, just use SparkSession.builder:
    from pyspark.sql import SparkSession

      spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    ---------------------------------------------------------------------------------------
    df = spark.read.json("path/to/file/file.csv")
    # Displays the content of the DataFrame
    df.show()
    ---------------------------------------------------------------------------------------
    sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    ---------------------------------------------------------------------------------------
    # This line will preform a Group By on the DataFrame and will then aggregate 
    # the max EFFDT forthose records.
    grouped_df = df.groupBy(df["FIELDNAME"],df["FIELDVALUE"]).agg(max(df["EFFDT"]))