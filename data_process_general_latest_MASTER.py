# spark-submit data_process_general_latest_MASTER.py DIM_ASST_MSTR.txt replaceDIMASSTMSTR.csv output
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import when, col
from pyspark.sql import functions as F
from pyspark.sql.window import Window 
import pandas as pd
import numpy as np
import sys
import os
import random, csv, shutil
import traceback
from ast import literal_eval

# This script takes three arguments, an input a replace, and a output
if len(sys.argv) != 5:
  print('Usage: ' + sys.argv[0] + 'Script name should be followed by Input file, Address File, replace file and the output directory location. All 3 arguments seperated by single space')
  sys.exit(1)

# Variables declarations
re_dict_col = {}
re_dict_val = {}

dataFileInvalid = "User input error. Data file mentioned is not available. Please check the name and re-run the script!!!!"
replaceFileInvalid = "User input error. Replace file mentioned is not available. Please check the name and re-run the script!!!!"
successStatusMsg = "Scrubbing process successfully completed"
failureStatusMsg = "Scrubbing process failed. Please check the exception file log for more information"
duplicate_column_scrub_needs = "User input error. Invalid scrubbing criteria. Two scrubbing needs mentioned for the same column. Please check the replace file content!!!!"
invalid_column_names = "User input error. Invalid scrubbing criteria. One or more column names mentioned in the replace file are incorrect. Please check the replace file contents!!!!"
invalid_type_values = "User input error. Invalid scrubbing criteria. One or more values in the TYPE column of replace file are incorrect. Please check the replace file contents!!!!"
exceptionStatusMsg = "Processing error. Error occured during code execution. Please check the Execution exception log file for more details!!!!"

valid_replace_changes = ['address', 'fix', 'multiple', 'numeric','regex','single','cross']
input_file = sys.argv[1]
address_file = sys.argv[2]
replace_file = sys.argv[3]
output_directory = sys.argv[4]
execution_status_files = ['ExecutionResult.txt','Excecution_Exception_Log.txt']

# Remove output directory if exists
if os.path.exists(output_directory):
    shutil.rmtree(output_directory)

# Remove status file created in the previous execution
for status_file in execution_status_files:
    if os.path.isfile(status_file):
        os.remove(status_file)

# Create file to write the status of process execution
f = open('ExecutionResult.txt','w')

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

# Function to use for REGEX replace
def replaceAcctNum(preRegexString):
    (i,replaceString) = (0,"")

    preRegexString = str(preRegexString)
    lengthofString = len(preRegexString)
    withholdString = preRegexString[0:1]

    while i < (lengthofString-1):
        replaceString = replaceString+str(i)
        i=i+1
    newStringValue = withholdString+replaceString
    return newStringValue

# Read the input Data Scrubbing needs file to create appropriate data structures
try:
    scrub_file = open(replace_file, 'r')
except Exception as e:
    f.write(replaceFileInvalid)
    f.close()

reader = csv.DictReader(scrub_file)

# Create Dictionary 1: Key as type of change and value is a list of column names
for row in reader:
    if row['Type'] in re_dict_col:
        re_dict_col[row['Type']].append(row['Column'])
    else:
        re_dict_col[row['Type']] = [row['Column']]   
# Create Dictionay 2: Key as Column name and value is replacement data
    if row['Column'] in re_dict_val:
        f.write(duplicate_column_scrub_needs)
        f.close()
        sys.exit(duplicate_column_scrub_needs)
    else:
        re_dict_val[row['Column']] = row['Value']

# Update values in dictionary 2 to list
# for key, value in re_dict_val.items():
#     re_dict_val[key] = value.split(",")

# # Convert value 'blob' for fields which have to be replaced using "multiple" for changes into a dictionary
# for key, value in re_dict_val.items():
#     if "{" in value:
#         dict_value = re_dict_val[key]
#         multi_dict = literal_eval(dict_value)
#         re_dict_val[key] = multi_dict

# Strip whitespaces from the above created dictionaries. This is needed to match column names in the dataframes
strip_dict(re_dict_val)
strip_dict(re_dict_col)

# Create spark session
spark = SparkSession.builder.appName('DataScrub').getOrCreate()

# Read the data file and create a Spark Dataframe
try:
    if input_file.endswith('.txt'):
        df = spark.read.option("header", "true") \
        .option("delimiter", "|") \
        .option("inferSchema", "true") \
        .csv(input_file)
    elif input_file.endswith('.csv'):
        df = spark.read.csv(input_file,inferSchema =True,header=True)
    else:
        f.write(dataFileFormatInvalid)
        f.close()
        sys.exit(dataFileFormatInvalid)

except Exception as e:
    f.write(dataFileInvalid)
    f.close()
    
# Drop unnecessary columns from the data file dataframe
drop_list = ['BTCH_CR_RUN_ID', 'BTCH_CR_TS', 'BTCH_LST_UPD_RUN_ID', 'BTCH_LST_UPD_TS']
df = df.drop(*drop_list)

# Register the functions as User Defined Functions (UDF)
state_name_udf = udf(stateCodeToName, StringType())
replace_acct_num_udf = udf(replaceAcctNum, StringType())

# Create a list of columns present in the data file
df_columns = df.columns
datafile_count = df.count()

# Check if the column names in the replace file are present in the data file. If not present stop execution
for key in re_dict_val:
    if key in df_columns:
        valid_columns = True
    else:
        # valid_columns = False
        f.write(invalid_column_names)
        f.close()
        sys.exit(invalid_column_names)

# Check if the type mentioned in the replace file is valid. If not valid stop execution
for key in re_dict_col:
    if key in valid_replace_changes:
        valid_replace_keys = True
    else:
        # valid_replace_keys = False
        f.write(invalid_type_values)
        f.close()
        sys.exit(invalid_type_values)

# Logic to perform data scrubbing according to user needs mentioned in the replace file
try:
    for key, value in re_dict_col.items():
        if key == "numeric":
            for column in value:
                startint_value = int(re_dict_val[column])
                df = df.withColumn('temp', lit(startint_value))
                df = df.withColumn(column, col(column)-col('temp')).drop('temp')
                # df = df.withColumn(column, increment_udf(column))
        if key == "cross":
            for column in value:
                startint_value = int(float(re_dict_val[column]))
                increment_udf = udf(lambda x: x + startint_value, LongType())
                # df = df.withColumn('index', monotonically_increasing_id())\
                # .withColumn(column, increment_udf('index')).drop('index')
                df = df.withColumn(column, increment_udf('ASST_ID'))
        if key == "multiple":
            for column in value:
                # type_select = re_dict_val[column]
                # type_random = udf(lambda x: random.choice(type_select), StringType())
                # df = df.withColumn(column, type_random(column))
                # for key in re_dict_val[column]:
                #     df = df.withColumn(column, when(df[column] == key, re_dict_val[column][key]).otherwise(df[column]))
                dict_value = re_dict_val[column]
                multi_dict = literal_eval(dict_value)
                multi_dict_udf = udf(lambda x: multi_dict[x] if x in multi_dict else x, StringType())
                df = df.withColumn(column, multi_dict_udf(column))  
        if key == "fix":
            for column in value:
                df = df.withColumn(column, lit(' '.join(re_dict_val[column])))   
        if key == "regex":
            for column in value:
                df = df.withColumn('preRegex', regexp_extract(column, '(\d+)-(\d+)',1))
                df = df.withColumn('postRegex', regexp_extract(column,'(\d+)-(\d+)',2))
                df = df.withColumn('preRegex', replace_acct_num_udf('preRegex'))
                df = df.withColumn('newValue', concat(df.preRegex,lit(re_dict_val[column]),df.postRegex))
                df = df.withColumn(column, lit(df.newValue)).drop('newValue')
        if key == "single":
            for column in value:
                delimiter = re_dict_val[column][-2:-1]
                flag = int(re_dict_val[column][-1])
                parse_udf = udf(lambda x: x.split(delimiter)[flag], StringType())
                df = df.withColumn(column, parse_udf(re_dict_val[column][:-2]))
        if key == "address":
            # Read the ASSET ID to Address CSV file
            add_df = spark.read.csv(address_file,header=True,inferSchema=True,nullValue=None,nanValue=None) 
            # create temp view
            add_df.createOrReplaceTempView("addressData")
            # Remove rows with None on certain columns
            sql_add_df = spark.sql("SELECT * FROM addressData WHERE STREET != 'None' AND CITY != 'None' AND REGION !='None'")
            # Add index
            sql_add_df = sql_add_df.withColumn('ADDR_LN_1', concat(sql_add_df.NUMBER,lit(" "), sql_add_df.STREET))
            sql_add_df = sql_add_df.withColumn('ADDR_LN_2', lit(" "))
            # Comment the line above and uncomment the below line of code if you would like to include Apartments in the address values. Appropriate values should be present in the address file
            # sql_add_df = sql_add_df.withColumn('ADDR_LN_2', concat(lit("UNIT"), lit(" "),sql_add_df.UNIT))
            # Drop unnecessary columns from the OPENADDRESSES.IO dataframe
            drop_list = ['LON','LAT','NUMBER','STREET','UNIT','DISTRICT','ID','HASH']
            sql_add_df = sql_add_df.drop(*drop_list)
            # Select DISTINCT rows. Currently using DISTINCT. Later, need to use distinct on a specific column value.
            # sql_add_df = sql_add_df.distinct()
            # Rename column CITY in OPENADDRESSES.IO to NEW_CITY. This is done to avoid ambiguous reference to CITY column in the data file
            sql_add_df = sql_add_df.withColumnRenamed('CITY','NEW_CITY')
            ##################################################################################################################################
            ################################################# OLD CODE #######################################################################
            # # Select random address rows
            # w = Window.orderBy("NEW_CITY")
            # w2 = Window.orderBy("ASST_DIM_ID")
            # sql_add_df = sql_add_df.withColumn("new_index", row_number().over(w))
            # df = df.withColumn("join_index", row_number().over(w2))
            # # Select random address data
            # sql_addfile_df = sampler(sql_add_df,"new_index",datafile_count)
            # # Adding index column once again to the address dataframe to be able to join with the data file dataframe
            # sql_addfile_df = sql_addfile_df.withColumn("join_index", row_number().over(w))
            ####################################################################################################################################
            # Join both dataframes to create one final dataframe
            df = df.join(sql_add_df, 'ASST_ID')

            # df = df.join(sql_add_df, 'ASST_ID', "left_outer")
            # Replace the required values in the data file dataframe
            df = df.withColumn('CITY', lit(df.NEW_CITY))\
                    .withColumn('STAT', lit(df.REGION))\
                    .withColumn('ZIP', lit(df.POSTCODE))\
                    .withColumn('PRPTY_ADDR_LN1', lit(df.ADDR_LN_1))\
                    .withColumn('PRPTY_ADDR_LN2', lit(df.ADDR_LN_2))
            # Finally drop the unnecessary columns
            df_drop_list = ['NEW_CITY','REGION','POSTCODE','ADDR_LN_1','ADDR_LN_2','new_index','join_index']
            df = df.drop(*df_drop_list)
            # Use UDF to replace state description
            df = df.withColumn('STAT_CD_DESC', state_name_udf('STAT'))
            
    df.coalesce(1).write.csv(output_directory+'/csv', header=True)
    # If the output has to be written in parquet format uncomment the below line of code
    df.coalesce(1).write.csv(output_directory+'/pipe', sep = '|', header=True)
    df.coalesce(1).write.parquet(output_directory+'/parquet')

    f.write(successStatusMsg)
    f.close()
    SparkSession.stop
except Exception as e:
    f.write(exceptionStatusMsg)
    print(exceptionStatusMsg)
    f.close()
    with open ('Excecution_Exception_Log.txt', 'a') as exceptfile:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        exceptfile.write(str(e))
        exceptfile.write(traceback.format_exc(exc_type, exc_value))
        #f.write(traceback.format_exc())
        exceptfile.close()
        SparkSession.stop