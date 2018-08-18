# spark-submit data_process_general.py export1.txt replace.csv output
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import when
from pyspark.sql import functions as F
from pyspark.sql.window import Window 
import pandas as pd
import numpy as np
import sys
import os
import random,csv

# This script takes three arguments, an input a replace, and a output
if len(sys.argv) != 4:
  print('Usage: ' + sys.argv[0] + 'Script name should be followed by Input file, replace file and the output directory location. All 3 arguments seperated by single space')
  sys.exit(1)

# Variables declarations
re_dict_col = {}
successStatusMsg = "Scrubbing process successfully completed"
failureStatusMsg = "Invalid scrubbing requirement. Please verify the file criteria mentioned in the file"
valid_replace_changes = ['address', 'fix', 'multiple', 'numeric']
input = sys.argv[1]
replace = sys.argv[2]
output_file = sys.argv[3]

# Remove status file created in the previous execution
if os.path.isfile('SparkExecutionStatus.txt'):
    os.remove("SparkExecutionStatus.txt")

f = open('SparkExecutionStatus.txt','w')

# Flags to control the processing 
valid_replace_keys = False
valid_columns = False

# Define a dictionary of state codes and names to retrieve State Name using State Code
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

# Function to return state names based on state codes
def stateCodeToName(stateCode):
    if stateCode in states:
        return states[stateCode]

# Function to strip white spaces from the dictionary entries
def strip_dict(d):
    for key, value in d.items():
        if ' ' in key:
            d[key.strip()] = value
            del d[key]
        if isinstance(value, dict):
            strip_dict(value)
        elif isinstance(value, list):
            d[key.strip()] = [x.strip() for x in value]
        elif isinstance(value, str):
            d[key.strip()] = value.strip()

# Function to select random address data
def sampler(df, col, num_of_output_records):
    colmax = df.count()
    #print(colmax)
    vals=random.sample(range(1, colmax), num_of_output_records)
    print(len(vals))
    return df.filter(df[col].isin(vals))

# Read the input Data Scrubbing needs file to create appropriate data structures
#re = pd.read_csv("replace.csv",header=None, encoding='utf-8')
re = pd.read_csv("replaceMultiple.csv",skiprows=1,header=None)

# Create Dictionay 1: Key as Column name and value is replacement data
re_dict_val = dict(zip(re[0], re[2]))

# Update values to list
for key, value in re_dict_val.items():
    re_dict_val[key] = value.split(",")

# Create Dictionary 2: Key as type of change and value is a list of column names
scrub_file = open('replaceMultiple.csv', 'r')
reader = csv.DictReader(scrub_file)

# Create list of values for a given key
for row in reader:
    if row['Type'] in re_dict_col:
        re_dict_col[row['Type']].append(row['Column'])
    else:
        re_dict_col[row['Type']] = [row['Column']]   

# Strip whitespaces from the above created dictionaries. This is needed to match column names in the dataframes
strip_dict(re_dict_val)
strip_dict(re_dict_col)

# Create spark session
spark = SparkSession.builder.appName('DataScrub').getOrCreate()
#df = spark.read.csv(input,inferSchema =True,header=True) 
df = spark.read.option("header", "true") \
    .option("delimiter", "|") \
    .option("inferSchema", "true") \
    .csv(input) 

# Register the functions as User Defined Functions (UDF)
state_name_udf = udf(stateCodeToName, StringType())
increment_udf = udf(lambda x: x + 5, IntegerType())
add_des = udf(lambda x: x + "123", StringType())

# Create a list of columns present in the data file
df_columns = df.columns
datafile_count = df.count()

# Check if the column names in the replace file are present in the data file
for key in re_dict_val:
    if key in df_columns:
        valid_columns = True
    else:
        valid_columns = False

# Check if the type mentioned in the replace file is valid
for key in re_dict_col:
    if key in valid_replace_changes:
        valid_replace_keys = True
    else:
        valid_replace_keys = False

# Execute logic only if the the content in the replace file is valid
if ((valid_columns == True) & (valid_replace_keys == True)):
    # Logic to perform data scrubbing according to user needs mentioned in the replace file
    for key, value in re_dict_col.items():
        if key == "numeric":
            for column in value:
                df = df.withColumn('index', monotonically_increasing_id())\
                .withColumn(column, increment_udf('index')).drop('index')
        if key == "multiple":
            for column in value:
                type_select = re_dict_val[column]
                df = df.withColumn(column, lit(random.choice(type_select)))
                df = df.withColumn(column, add_des(column))
        if key == "fix":
            for column in value:
                df = df.withColumn(column, lit(' '.join(re_dict_val[column])))             
        if key == "address":
            # change valid address
            add_df = spark.read.csv("statewide_*.csv",header=True,inferSchema=True,nullValue=None,nanValue=None) 
            # create temp view
            add_df.createOrReplaceTempView("addressData")
            # Remove rows with None on certain columns
            sql_add_df = spark.sql("SELECT * FROM addressData WHERE STREET != 'None' AND UNIT != 'None' AND CITY != 'None' AND REGION !='None'")
            # Add index
            sql_add_df = sql_add_df.withColumn('ADDR_LN_1', concat(sql_add_df.NUMBER,lit(" "), sql_add_df.STREET))
            sql_add_df = sql_add_df.withColumn('ADDR_LN_2', concat(lit("UNIT"), lit(" "),sql_add_df.UNIT))
            # Drop unnecessary columns from the OPENADDRESSES.IO dataframe
            drop_list = ['LON','LAT','NUMBER','STREET','UNIT','DISTRICT','ID','HASH']
            sql_add_df = sql_add_df.drop(*drop_list)
            # Select DISTINCT rows. Currently using DISTINCT. Later, need to use distinct on a specific column value.
            sql_add_df = sql_add_df.distinct()
            # Rename column CITY in OPENADDRESSES.IO to NEW_CITY. This is done to avoid ambiguous reference to CITY column in the data file
            sql_add_df = sql_add_df.withColumnRenamed('CITY','NEW_CITY')
            # Select random address rows
            w = Window.orderBy("NEW_CITY")
            w2 = Window.orderBy("ASST_DIM_ID")
            sql_add_df = sql_add_df.withColumn("new_index", row_number().over(w))
            df = df.withColumn("join_index", row_number().over(w2))
            # Select random address data
            sql_addfile_df = sampler(sql_add_df,"new_index",datafile_count)
            # Adding index column once again to the address dataframe to be able to join with the data file dataframe
            sql_addfile_df = sql_addfile_df.withColumn("join_index", row_number().over(w))
            # Join both dataframes to create one final dataframe
            df = df.join(sql_addfile_df, 'join_index')
            # Replace the required values in the data file dataframe
            df = df.withColumn('CITY', lit(df.NEW_CITY))\
                    .withColumn('STAT', lit(df.REGION))\
                    .withColumn('ZIP', lit(df.POSTCODE))\
                    .withColumn('PRPTY_ADDR_LN1', lit(df.ADDR_LN_1))\
                    .withColumn('PRPTY_ADDR_LN2', lit(df.ADDR_LN_2))

            # Finally drop the unnecessary columns
            df_drop_list = ['NEW_CITY','REGION','POSTCODE','ADDR_LN_1','ADDR_LN_2','new_index']
            df = df.drop(*df_drop_list)
            # Use UDF to replace state description
            df = df.withColumn('STAT_CD_DESC', state_name_udf('STAT'))
    df.write.csv(output_file, header=True)
    f.write(successStatusMsg)
    f.close()
    SparkSession.stop
else:
    f.write(failureStatusMsg)
    print(failureStatusMsg)
    f.close()
    SparkSession.stop