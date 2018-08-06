import pandas as pd
import sys
import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import when
from pyspark.sql import functions as F
import random

#Remove Status file if already existing
if os.path.isfile('SparkExecutionStatus.txt'):
    os.remove('SparkExecutionStatus.txt')

successStatusMsg = "Scrubbing process successfully completed"
failureStatusMsg = "Invalid scrubbing requirement. Please verify the file criteria mentioned in the file"

f = open('SparkExecutionStatus.txt','w')

# This script takes two arguments, a data file and the scrubbing needs
if len(sys.argv) != 4:
  print('Usage: ' + sys.argv[0] + 'Script name followed by 3 files required as input to the script. Input file, Scrubbing needs and Output folder name')
  sys.exit(1)

data_file = sys.argv[1]
scrubbing_needs = sys.argv[2]
output_file = sys.argv[3]

spark = SparkSession.builder.appName('DataScrub').getOrCreate()
changes = pd.read_csv(scrubbing_needs, header=None)
psp_datafile_df = spark.read.csv(data_file,header=True,inferSchema=True)

# Using Spark DataFrames for managing the OPENADDRESSES dataset
psp_addfile_df = spark.read.csv("statewide_*.csv",header=True,inferSchema=True,nullValue=None,nanValue=None)

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
#############################################################################################################################
# Using pandas dataframe
# big_frame = pd.concat([pd.read_csv(f, sep=',', low_memory=False) for f in glob.glob(path + "/*.csv")], ignore_index=True)
# The next statement is used for shuffling the rows in dataframe
# big_frame = big_frame.sample(frac=1).reset_index(drop=True)
# big_frame[(big_frame['STREET'].notnull()) & (big_frame['UNIT'].notnull()) & (big_frame['REGION'].notnull())]
# big_frame["ADD_LN_1"] = big_frame["NUMBER"].map(str) + " " + big_frame["STREET"]
# big_frame.head(data_rows_count) - 11744584
#11744584
############################################################################################################################
# # Option 1. Select random rows using sample function.
# max_rows = 11744584
# output = list(range(11744585))

# #Select rows with non Null values and then select random rows from the dataframe.
# psp_addfile_df.where(col("STREET").isNotNull())
# psp_addfile_df.sample(False, 1.0, 100).collect() 

# # Add row_index column on each dataframe to be able to join the 2 dfs.
# psp_addfile_df=psp_addfile_df.withColumn('row_index', F.monotonically_increasing_id())
# psp_datafile_df=psp_datafile_df.withColumn('row_index', F.monotonically_increasing_id())

# # Join the dfs on row_index
# psp_datafile_df = psp_datafile_df.join(psp_addfile_df, on=["row_index"]).sort("row_index").drop("row_index")
# psp_datafile_df.show()
###########################################################################################################################
# Option 2
psp_addfile_df.createOrReplaceTempView("addressData")
sql_addfile_df = spark.sql("SELECT * FROM addressData WHERE STREET != 'None' AND UNIT != 'None' AND CITY != 'None' AND REGION !='None'")
add_file_count=sql_addfile_df.count()
sql_addfile_df=sql_addfile_df.withColumn('row_index', F.monotonically_increasing_id())

def sampler(df, column_name, records):
    print("column Name:" + column_name)
    #Calculate number of rows and round off
    #rows_max=df.count
    #print(rows_max)
    rows_max_int = records
    #Create random sample
    nums=[int(x) for x in range(rows_max_int)]
    random.shuffle(nums)
    print(nums[0:5])
    #Use 'nums' to filter dataframe using 'isin'
    #return df[df.column_name.isin(nums)]
    return df.filter(col(column_name).isin(*nums))

# Select 100000 random rows. This process is time consuming and CPU intensive
psp_addfile_df = sampler(sql_addfile_df,"row_index",100000)

# Format column values to create ADDR_LN_1 and ADDR_LN_2 to match the data file format
psp_addfile_df = psp_addfile_df.withColumn('ADDR_LN_1', concat(psp_addfile_df.NUMBER,lit(" "), psp_addfile_df.STREET))
psp_addfile_df = psp_addfile_df.withColumn('ADDR_LN_2', concat(lit("UNIT"), lit(" "),psp_addfile_df.UNIT))

# Drop unnecessary columns from the OPENADDRESSES.IO dataframe
drop_list = ['LON','LAT','NUMBER','STREET','UNIT','DISTRICT','ID','HASH','row_index']
psp_addfile_drop1_df = psp_addfile_df.drop(*drop_list)

# Select DISTINCT rows. Currently using DISTINCT. Later, need to use distinct on a specific column value.
psp_addfile_df_dist = psp_addfile_drop1_df.distinct()

# Rename column CITY in OPENADDRESSES.IO to NEW_CITY. This is done to avoid ambiguous reference to CITY column in the data file
psp_addfile_df_dist = psp_addfile_df_dist.withColumnRenamed('CITY','NEW_CITY')

# Join both dataframes to create one final dataframe
psp_addfile_df_dist=psp_addfile_df_dist.withColumn('row_index', F.monotonically_increasing_id())
psp_datafile_df=psp_datafile_df.withColumn('row_index', F.monotonically_increasing_id())
psp_datafile_newAdd_df = psp_datafile_df.join(psp_addfile_df_dist, 'row_index').drop('row_index')

# Replace data file field values with values from OPENADDRESS.IO dataframe
psp_datafile_newAdd_df = psp_datafile_newAdd_df.withColumn('CITY',lit(psp_datafile_newAdd_df.NEW_CITY)).withColumn('STAT',lit(psp_datafile_newAdd_df.REGION)).withColumn('ZIP',lit(psp_datafile_newAdd_df.POSTCODE)).withColumn('PRPTY_ADDR_LN1',lit(psp_datafile_newAdd_df.ADDR_LN_1)).withColumn('PRPTY_ADDR_LN2',lit(psp_datafile_newAdd_df.ADDR_LN_2))

# Use UDF to replace state description
psp_datafile_newState_df=psp_datafile_newAdd_df.withColumn('STAT_CD_DESC', state_name_udf('STAT'))

# Finally drop unnecessary columns from datafile Dataframe
datafile_drop_list = ['NEW_CITY','REGION','POSTCODE','ADDR_LN_1','ADDR_LN_2']
psp_datafile_addChanged = psp_datafile_newState_df.drop(*datafile_drop_list)
psp_datafile_addChanged.take(2)
###########################################################################################################################
# columns = psp_datafile_df.columns
# dict_changes = dict(zip(changes[0],changes[1]))
# flag = True

# for key in dict_changes:
#     if key not in columns:
#         flag = False

# if (flag):
#     for key in dict_changes:
#         psp_datafile_df = psp_datafile_df.withColumn(key, lit(dict_changes[key]))
#     psp_datafile_df.write.csv(output_file, header=True)
#     f.write(successStatusMsg)
#     SparkSession.stop
# else:
#     f.write(failureStatusMsg)
#     print(failureStatusMsg)
#     SparkSession.stop
