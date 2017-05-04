![alt text](https://bradwulf.files.wordpress.com/2016/05/0.png?w=700 "SparkSQL Logo")
# SparkSQL Overview
Spark SQL is a Spark module for structured data processing. Unlike the basic Spark RDD API, the interfaces provided by Spark SQL provide Spark with more information about the structure of both the data and the computation being performed. Internally, Spark SQL uses this extra information to perform extra optimizations. There are several ways to interact with Spark SQL including SQL and the Dataset API. When computing a result the same execution engine is used, independent of which API/language you are using to express the computation. This unification means that developers can easily switch back and forth between different APIs based on which provides the most natural way to express a given transformation.

### Spark's Base Unit - DataFrame ###

A DataFrame is a Dataset organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood. DataFrames can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDDs.

### Resources ###
SparkSQL documentation - http://spark.apache.org/docs/latest/sql-programming-guide.html

### Usage ###
To use SparkSQL: 
* At the command line one may type `spark-sql`
* To run a SQL script simply add the `-f <insert filename>` argument to the command.
* 
### Handling UPDATE, or DELETE in translation
When translating from PLSQL to SparkSQL one must make note of the fast that there is not UPDATE or DELETE functionality in Spark. This is because Dataframes are immutable by design.

Instead, as engineers we are responsible for translating these `UPDATE` / `DELETE` itms into `SELECT` statements. Instead up updating a record, one might wait to include the given record unil the end, and then when composing the resulting DataFrame, make the change then. I have encountered this and an example lies within the STG_HCM_FACT_JOB_SPARK.hql file in this repository.