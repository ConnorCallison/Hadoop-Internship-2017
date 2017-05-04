from pyspark import SparkContext, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from decimal import Decimal  
import pickle

sc = SparkContext("local",
				  "CREATE_STG_HCM_MAXEFFDT_XLAT",
  				  pyFiles=['stg_hcm_maxeffdt_xlat.py']
  				 )
sqlContext = SQLContext(sc)

# Define the Schema for our dataframe.
schema = StructType([
StructField("FIELDNAME", StringType(), False),
StructField("FIELDVALUE", StringType(), False),
StructField("EFFDT", StringType(), False),
StructField("EFF_STATUS", StringType(), False),
StructField("XLATLONGNAME", StringType(), False),
StructField("XLATSHORTNAME", StringType(), False),
StructField("LASTUPDDTTM", StringType(), False),
StructField("LASTUPDOPRID", StringType(), False),
StructField("SYNCID", StringType(), False)])

# Load the data from our textfile.
raw_psxlatitem = \
 		sc.textFile("hdfs:///tmp/delim-test3/part-m-00000")

# Split our data on the delimiter.
split_psxlatitem = raw_psxlatitem.map(lambda l: l.split('^'))

# Make our data into tuples that represent a row.
string_psxlatitem = split_psxlatitem.map(lambda p: (p[0], p[1], p[2],p[3],p[4],p[5], p[6],p[7],p[8]))

# Create our dataframe using our prepared data and predefined Schema.
df = sqlContext.createDataFrame(string_psxlatitem, schema)
#df.cache()

print "############################################# EFFDTS #####################################################"
effdts_df = df.select(df["EFFDT"])
# #effdts_df.show()

print "############################################# EFFDTS to today #####################################################"

# #Filter our data to only contain the records where our EFFDT is before or equal to today.
effdt_to_today = effdts_df.where(unix_timestamp(effdts_df["EFFDT"]) <= unix_timestamp(current_timestamp())).distinct()
effdt_to_today.show()

df = df.join(effdt_to_today, "EFFDT", 'inner')

# #print "############################################# MAX EFFDTS #####################################################"
grouped_df = df.groupBy(df["FIELDNAME"],df["FIELDVALUE"]).agg(max(df["EFFDT"]))
print grouped_df.count()

df = df.collect()
#print df
#df.write.parquet("hdfs:///spark_data/output.parquet")
#df.write.format("com.databricks.spark.csv").options(delimiter='^').save("hdfs:///spark_data/output_1.csv")
#df.write.option("sep","^").option("header","true").csv("stg_hcm_maxeffdt_xlat.csv")

with open("stg_hcm_maxeffdt_xlat_data", 'wb') as f:
    pickle.dump(df, f)