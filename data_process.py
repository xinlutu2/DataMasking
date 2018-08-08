# spark-submit data_process.py DIM_full.csv replace.csv output
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import when
from pyspark.sql import functions as F
import pandas as pd
import numpy as np
import sys
import os

# This script takes three arguments, an input a replace, and a output
if len(sys.argv) != 4:
  print('Usage: ' + sys.argv[0] + ' <input> <replcace> <out>')
  sys.exit(1)

input = sys.argv[1]
replace = sys.argv[2]
out = sys.argv[3]

if os.path.isfile('STATUS.txt'):
	os.remove("STATUS.txt")

# sc = SparkContext.getOrCreate()
# sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName('DataScrub').getOrCreate()

df = spark.read.csv(input,inferSchema =True,header=True) 
re = pd.read_csv(replace,header=None)

# # reverse string for certain numeric columns
# df = df.withColumn("GM_LOAN_ID", reverse(df.GM_LOAN_ID))\
# 	.withColumn("MSS_LOAN_ID", reverse(df.MSS_LOAN_ID)).withColumn("ISSR_ID", reverse(df.ISSR_ID))\
# 	.withColumn("POOL_ID", reverse(df.POOL_ID))\
# 	.withColumn("AGNCY_CASE_ID", reverse(df.AGNCY_CASE_ID))\
# 	.withColumn("ORGNL_LOAN_ID", reverse(df.ORGNL_LOAN_ID))\

increment_udf = udf(lambda x: x + 5, IntegerType())
df = df.withColumn('index', monotonically_increasing_id())
df = df.withColumn('index', increment_udf('index'))
df = df.withColumn('GM_LOAN_ID', lit('index'))
df = df.drop('index')

increment_udf = udf(lambda x: x + 5, IntegerType())
df = df.withColumn('index', monotonically_increasing_id())
df = df.withColumn('index', increment_udf('index'))
df = df.withColumn('MSS_LOAN_ID', lit('index'))
df = df.drop('index')

increment_udf = udf(lambda x: x + 5, IntegerType())
df = df.withColumn('index', monotonically_increasing_id())
df = df.withColumn('index', increment_udf('index'))
df = df.withColumn('POOL_ID', lit('index'))
df = df.drop('index')

increment_udf = udf(lambda x: x + 5, IntegerType())
df = df.withColumn('index', monotonically_increasing_id())
df = df.withColumn('index', increment_udf('index'))
df = df.withColumn('AGNCY_CASE_ID', lit('index'))
df = df.drop('index')

increment_udf = udf(lambda x: x + 5, IntegerType())
df = df.withColumn('index', monotonically_increasing_id())
df = df.withColumn('index', increment_udf('index'))
df = df.withColumn('ORGNL_LOAN_ID', lit('index'))
df = df.drop('index')


# change valid address
add_df = spark.read.csv("statewide_ma.csv",header=True,inferSchema=True,nullValue=None,nanValue=None) 
# create temp view
add_df.createOrReplaceTempView("addressData")
# Remove rows with None on certain columns
sql_add_df = spark.sql("SELECT * FROM addressData WHERE STREET != 'None' AND UNIT != 'None' AND CITY != 'None' AND REGION !='None'")
# Add index
sql_add_df = sql_add_df.withColumn('ADDR_LN_1', concat(sql_add_df.NUMBER,lit(" "), sql_add_df.STREET))
sql_add_df = sql_add_df.withColumn('ADDR_LN_2', concat(lit("UNIT"), lit(" "),sql_add_df.UNIT))
# Drop unnecessary columns from the OPENADDRESSES.IO dataframe
drop_list = ['LON','LAT','NUMBER','STREET','UNIT','DISTRICT','ID','HASH','row_index']
sql_add_df = sql_add_df.drop(*drop_list)
# Select DISTINCT rows. Currently using DISTINCT. Later, need to use distinct on a specific column value.
sql_add_df = sql_add_df.distinct()
sql_add_df = sql_add_df.withColumnRenamed('CITY','NEW_CITY')
# Join both dataframes to create one final dataframe
sql_add_df = sql_add_df.withColumn('row_index', F.monotonically_increasing_id())
df = df.withColumn('row_index', F.monotonically_increasing_id())
df_new = df.join(sql_add_df, 'row_index').drop('row_index')

df_new = df_new.withColumn('CITY', lit('NEW_CITY'))\
         .withColumn('STAT', lit('REGION'))\
         .withColumn('ZIP', lit('POSTCODE'))\
         .withColumn('PRPTY_ADDR_LN1', lit('ADDR_LN_1'))\
         .withColumn('PRPTY_ADDR_LN2', lit('ADDR_LN_2'))


df_drop_list = ['NEW_CITY','REGION','POSTCODE','ADDR_LN_1','ADDR_LN_2']
df_new = df_new.drop(*df_drop_list)
# Define a dictionary and create a function to retrieve State Name using State Code
states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
}

def stateCodeToName(stateCode):
    if stateCode in states:
        return states[stateCode]
# Register the above created function as an User Defined Function (UDF)
state_name_udf = udf(stateCodeToName, StringType())
# Use UDF to replace state description
df_new = df_new.withColumn('STAT_CD_DESC', state_name_udf('STAT'))

re_dict = dict(zip(re[0], re[1]))
flag = True

# ## unpack keys into a list literal
# for each in [*re_dict]:
# 	if (each not in df_new.columns):
# 		flag = False

if (flag):
	# replace fixed value to certain values
	for each in [*re_dict]:
		df_new = df_new.withColumn(each, lit(re_dict[each]))

	# df.toPandas().to_csv("normalized.csv", header=True)
	df_new.write.csv(out, header=True)
	# df.coalesce(1).write.csv(output_file, header=True)
	# Finally, let Spark know that the job is done.
	with open("STATUS.txt", "w") as txt:
		txt.write("Scrubbing success!")
	SparkSession.stop
else:
	with open("STATUS.txt", "w") as txt:
		txt.write("Input criteria for scrubbing invalid!")
	SparkSession.stop