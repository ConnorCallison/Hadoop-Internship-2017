from pyspark import SparkContext, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from decimal import Decimal  
import pickle

sc = SparkContext("local",
				  "CREATE_STG_HCM_MAXEFFDT_ACTION",
  				  pyFiles=['stg_hcm_maxeffdt_action.py']
  				 )
sqlContext = SQLContext(sc)

# Define the Schema for our dataframe.
schema = StructType([
StructField("ACTION", StringType(), False),
StructField("EFFDT", StringType(), False),
StructField("EFF_STATUS", StringType(), False),
StructField("ACTION_DESCR", StringType(), False),
StructField("ACTION_DESCRSHORT", StringType(), False),
StructField("OBJECTOWNERID", StringType(), False),
StructField("LASTUPDDTTM", StringType(), False),
StructField("LASTUPDOPRID", StringType(), False),
StructField("SYSTEM_DATA_FLG", StringType(), False)])

# Load the data from our textfile.
raw_ps_action_tbl = \
 		sc.textFile("hdfs:///tmp/stg_hcm_maxeffdt_action/part-m-00000")

# Split our data on the delimiter.
split_ps_action_tbl = raw_ps_action_tbl.map(lambda l: l.split('^'))

# Make our data into tuples that represent a row.
string_ps_action_tbl = split_ps_action_tbl.map(lambda p: (p[0], p[1], p[2],p[3],p[4],p[5], p[6],p[7],p[8]))

# Create our dataframe using our prepared data and predefined Schema.
df = sqlContext.createDataFrame(string_ps_action_tbl, schema)

effdts_df = df.select(df["EFFDT"])

# #Filter our data to only contain the records where our EFFDT is before or equal to today.
effdt_to_today = effdts_df.where(unix_timestamp(effdts_df["EFFDT"]) <= unix_timestamp(current_timestamp())).distinct()
effdt_to_today.show()

df = df.join(effdt_to_today, "EFFDT", 'inner')

grouped_df = df.groupBy(df["ACTION"]).agg(max(df["EFFDT"]))
print grouped_df.count()

#Output our dataframe to a file.
df = df.collect()
with open("stg_hcm_maxeffdt_action", 'wb') as f:
    pickle.dump(df, f)