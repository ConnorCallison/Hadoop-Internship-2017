from pyspark import SparkContext, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from decimal import Decimal  
import pickle

sc = SparkContext("local",
				  "CREATE_STG_HCM_MAXEFFDT_POSITION",
  				  pyFiles=['stg_hcm_maxeffdt_position.py']
  				 )
sqlContext = SQLContext(sc)

# Define the Schema for our dataframe.
schema = StructType([
StructField("POSITION_NBR", StringType(), False),
StructField("EFFDT", StringType(), False),
StructField("EFF_STATUS", StringType(), False),
StructField("DESCR", StringType(), False),
StructField("DESCRSHORT", StringType(), False),
StructField("ACTION", StringType(), False),
StructField("ACTION_REASON", StringType(), False),
StructField("ACTION_DT", StringType(), False),
StructField("BUSINESS_UNIT", StringType(), False),
StructField("DEPTID", StringType(), False),
StructField("JOBCODE", StringType(), False),
StructField("POSN_STATUS", StringType(), False),
StructField("DESCRSHORT", StringType(), False),
StructField("STATUS_DT", StringType(), False),
StructField("BUDGETED_POSN", StringType(), False),
StructField("CONFIDENTIAL_POSN", StringType(), False),
StructField("KEY_POSITION", StringType(), False),
StructField("JOB_SHARE", StringType(), False),
StructField("MAX_HEAD_COUNT", StringType(), False),
StructField("UPDATE_INCUMBENTS", StringType(), False),
StructField("REPORTS_TO", StringType(), False),
StructField("REPORT_DOTTED_LINE", StringType(), False),
StructField("ORGCODE", StringType(), False),
StructField("ORGCODE_FLAG", StringType(), False),
StructField("LOCATION", StringType(), False),
StructField("MAIL_DROP", StringType(), False),
StructField("COUNTRY_CODE", StringType(), False),
StructField("PHONE", StringType(), False),
StructField("COMPANY", StringType(), False),
StructField("STD_HOURS", StringType(), False),
StructField("STD_HRS_FREQUENCY", StringType(), False),
StructField("UNION_CD", StringType(), False),
StructField("SHIFT", StringType(), False),
StructField("REG_TEMP", StringType(), False),
StructField("FULL_PART_TIME", StringType(), False),
StructField("BARG_UNIT", StringType(), False),
StructField("SEASONAL", StringType(), False),
StructField("TRN_PROGRAM", StringType(), False),
StructField("LANGUAGE_SKILL", StringType(), False),
StructField("MANAGER_LEVEL", StringType(), False),
StructField("FLSA_STATUS", StringType(), False),
StructField("AVAIL_TELEWORK_POS", StringType(), False),
StructField("CLASS_INDC", StringType(), False),
StructField("ENCUMBER_INDC", StringType(), False),
StructField("FTE", StringType(), False),
StructField("POSITION_POOL_ID", StringType(), False),
StructField("EG_ACADEMIC_RANK", StringType(), False),
StructField("EG_GROUP", StringType(), False),
StructField("ENCUMB_SAL_OPTN", StringType(), False),
StructField("ENCUMB_SAL_AMT", StringType(), False),
StructField("HEALTH_CERTIFICATE", StringType(), False),
StructField("SIGN_AUTHORITY", StringType(), False),
StructField("ADDS_TO_FTE_ACTUAL", StringType(), False),
StructField("SAL_ADMIN_PLAN", StringType(), False),
StructField("GRADE", StringType(), False)])

# Load the data from our textfile.
raw_ps_position_data = \
 		sc.textFile("hdfs:///tmp/ps_position_data/part-m-00000")

# Split our data on the delimiter.
split_ps_position_data = raw_ps_position_data.map(lambda l: l.split('^'))

# Make our data into tuples that represent a row.
string_ps_position_data = split_ps_position_data.map(lambda p: (p[0], p[1], p[2],p[3],p[4],p[5],p[6],
	p[7],p[8],p[9],p[10],p[11],
	p[12],p[13],p[14],p[15],p[16],
	p[17],p[18],p[19],p[20],p[21],
	p[22],p[23],p[24],p[25],p[26],
	p[27],p[28],p[29],p[30],p[31],
	p[32],p[33],p[34],p[35],p[36],
	p[37],p[38],p[39],p[40],p[41],
	p[42],p[43],p[44],p[45],p[46],
	p[47],p[48],p[49],p[50],p[51],
	p[52],p[53],p[54]))

# Create our dataframe using our prepared data and predefined Schema.
df = sqlContext.createDataFrame(string_ps_position_data, schema)

effdts_df = df.select(df["EFFDT"])

# #Filter our data to only contain the records where our EFFDT is before or equal to today.
effdt_to_today = effdts_df.where(unix_timestamp(effdts_df["EFFDT"]) <= unix_timestamp(current_timestamp())).distinct()

df = df.join(effdt_to_today, "EFFDT", 'inner')

grouped_df = df.groupBy(df["POSITION_NBR"]).agg(max(df["EFFDT"]))
print grouped_df.count()

#Output our dataframe to a file.
df = df.collect()
with open("stg_hcm_maxeffdt_position", 'wb') as f:
    pickle.dump(df, f)