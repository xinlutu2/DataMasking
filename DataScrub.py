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
from faker import Faker
fake = Faker()

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
psp_addfile_df = spark.read.csv("statewide_*.csv",header=True,inferSchema=True)
psp_addfile_df.take(4)

# Using pandas dataframe
# big_frame = pd.concat([pd.read_csv(f, sep=',', low_memory=False) for f in glob.glob(path + "/*.csv")], ignore_index=True)
# The next statement is used for shuffling the rows in dataframe
# big_frame = big_frame.sample(frac=1).reset_index(drop=True)
# big_frame[(big_frame['STREET'].notnull()) & (big_frame['UNIT'].notnull()) & (big_frame['REGION'].notnull())]
# big_frame["ADD_LN_1"] = big_frame["NUMBER"].map(str) + " " + big_frame["STREET"]
# big_frame.head(data_rows_count) - 11744584
#11744584

max_rows = 11744584
output = list(range(11744585))

psp_addfile_df.where(col("STREET").isNotNull())
psp_addfile_df.sample(True, 11.11, 100).limit(1) 

# Add row_index column on each dataframe to be able to join the 2 dfs.
psp_addfile_df=psp_addfile_df.withColumn('row_index', F.monotonically_increasing_id())
psp_datafile_df=psp_datafile_df.withColumn('row_index', F.monotonically_increasing_id())

# Join the dfs on row_index
psp_datafile_df = psp_datafile_df.join(psp_addfile_df, on=["row_index"]).sort("row_index").drop("row_index")
psp_datafile_df.show()

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
