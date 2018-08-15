
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

spark = SparkSession.builder.appName('DataScrub').getOrCreate()

df = spark.read.csv("DIM.csv",header=True,inferSchema=True)
psp_addfile_df = spark.read.csv("statewide_*.csv",header=True,inferSchema=True,nullValue=None,nanValue=None)
drop_list_initial = ['BTCH_CR_RUN_ID', 'BTCH_CR_TS', 'BTCH_LST_UPD_RUN_ID', 'BTCH_LST_UPD_TS']
df = df.drop(*drop_list_initial)
re = pd.read_csv("replaceMultiple.csv",header=None)
re_dict_val = dict(zip(re[0], re[2]))
re_dict_col = dict(zip(re[1], re[0]))
print(re_dict_val)
# re_dict_col = sorted(re_dict_col, reverse=True)
# print(re_dict_col)

# Function to get the state description when State Code is given as input
def stateCodeToName(stateCode):
    if stateCode in states:
        return states[stateCode]

# Register the above created function as an User Defined Function (UDF)
state_name_udf = udf(stateCodeToName, StringType())

# Function to remove whitescapes
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

# Stip whitespaces from both the dictionaries
strip_dict(re_dict_val)
strip_dict(re_dict_col)

# print(re_dict_val)
# print(re_dict_col)
# # removew(re_dict_col)

# Sort the dictionary in order to control the execution of logic
re_dict_col_new = {}
re_col_list = sorted(re_dict_col, reverse=True)
for value in re_col_list:
    re_dict_col_new[value] = re_dict_col[value]
    
# print(re_dict_val)
# print(re_dict_col)
# print(re_dict_col_new)

# Convert the scrubbing needs
for key, value in re_dict_val.items():
    re_dict_val[key] = value.split(",")

# Set the flags to False initially
df_changed = False
df_numeric_change = False
df_string_change = False

# Start of data masking or encryption logic 
for key, value in re_dict_col.items():
    if key == "numeric":
        df_1 = df.withColumn('index', monotonically_increasing_id())\
             .withColumn(re_dict_col[key], increment_udf('index')).drop('index') 
        df_changed = True
        df_numeric_change = True

print(df_changed)

# Logic for handling String values replacements
for key, value in re_dict_col_new.items():
    if (key == "mutiple"):
        type_select = re_dict_val[re_dict_col[key]]
        add_des = udf(lambda x: x + "123", StringType())
        if (df_changed == False):
            df_new = df.withColumn(re_dict_col[key], lit(random.choice(type_select)))
            df_new = df.withColumn(re_dict_col[key], add_des(re_dict_col[key]))
            df_changed = True
            df_string_change = True
        else:
            df_new = df.withColumn(re_dict_col[key], lit(random.choice(type_select)))
            df_new = df_new.withColumn(re_dict_col[key], add_des(re_dict_col[key]))
            df_string_change = True
    if (key == "fix"):
        if (df_changed == False):
            print("in FIX loop and change is False")
            df_new = df.withColumn(re_dict_col[key], lit(' '.join(re_dict_val[re_dict_col[key]])))
            df_changed = True
            df_string_change = True
        else:
            print("in the last if")
            df_new = df_new.withColumn(re_dict_col_new[key], lit(' '.join(re_dict_val[re_dict_col[key]])))
            df_string_change = True

# Logic for handling address replacement
for key, value in re_dict_col.items():
    if (key == "address"):
        # create temp view
        psp_addfile_df.createOrReplaceTempView("addressData")
        # Remove rows with None on certain columns
        sql_addfile_df = spark.sql("SELECT * FROM addressData WHERE STREET != 'None' AND UNIT != 'None' AND CITY != 'None' AND REGION !='None'")
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
        psp_addfile_df = sampler(sql_addfile_df,"row_index",1000)
        # Format column values to create ADDR_LN_1 and ADDR_LN_2 to match the data file format
        psp_addfile_df = psp_addfile_df.withColumn('ADDR_LN_1', concat(sql_addfile_df.NUMBER,lit(" "), sql_addfile_df.STREET))
        psp_addfile_df = psp_addfile_df.withColumn('ADDR_LN_2', concat(lit("UNIT"), lit(" "),sql_addfile_df.UNIT))
        # Drop unnecessary columns from the OPENADDRESSES.IO dataframe
        drop_list = ['LON','LAT','NUMBER','STREET','UNIT','DISTRICT','ID','HASH','row_index']
        psp_addfile_drop1_df = psp_addfile_df.drop(*drop_list)
        # Select DISTINCT rows. Currently using DISTINCT. Later, need to use distinct on a specific column value.
        psp_addfile_df_dist = psp_addfile_drop1_df.distinct()
        # Rename column CITY in OPENADDRESSES.IO to NEW_CITY. This is done to avoid ambiguous reference to CITY column in the data file
        psp_addfile_df_dist = psp_addfile_df_dist.withColumnRenamed('CITY','NEW_CITY')
        # Create an index column to be able to 2 both dataframes
        psp_addfile_df_dist = psp_addfile_df_dist.withColumn('row_index', F.monotonically_increasing_id())
        if (df_changed == False):
            # Join both dataframes to create one final dataframe
            df = df.withColumn('row_index', F.monotonically_increasing_id())
            df_data_address = df.join(psp_addfile_df_dist, 'row_index').drop('row_index')
            # Replace data file field values with values from OPENADDRESS.IO dataframe
            df_data_address = df_data_address.withColumn('CITY', lit(df_data_address.NEW_CITY))\
                                                        .withColumn('STAT', lit(df_data_address.REGION))\
                                                        .withColumn('ZIP', lit(df_data_address.POSTCODE))\
                                                        .withColumn('PRPTY_ADDR_LN1', lit(df_data_address.ADDR_LN_1))\
                                                        .withColumn('PRPTY_ADDR_LN2', lit(df_data_address.ADDR_LN_2))


            df_drop_list = ['NEW_CITY','REGION','POSTCODE','ADDR_LN_1','ADDR_LN_2']
            df_add_after_changes = df_data_address.drop(*df_drop_list)
            # Use UDF to replace state description
            df_add_final = df_add_after_changes.withColumn('STAT_CD_DESC', state_name_udf('STAT'))
        if (df_numeric_change == True):
            # Join both dataframes to create one final dataframe
            df_1 = df_1.withColumn('row_index', F.monotonically_increasing_id())
            df_data_address = df_1.join(psp_addfile_df_dist, 'row_index').drop('row_index')
            # Replace data file field values with values from OPENADDRESS.IO dataframe
            df_data_address = df_data_address.withColumn('CITY', lit(df_data_address.NEW_CITY))\
                                                        .withColumn('STAT', lit(df_data_address.REGION))\
                                                        .withColumn('ZIP', lit(df_data_address.POSTCODE))\
                                                        .withColumn('PRPTY_ADDR_LN1', lit(df_data_address.ADDR_LN_1))\
                                                        .withColumn('PRPTY_ADDR_LN2', lit(df_data_address.ADDR_LN_2))


            df_drop_list = ['NEW_CITY','REGION','POSTCODE','ADDR_LN_1','ADDR_LN_2']
            df_add_after_changes = df_data_address.drop(*df_drop_list)
            # Use UDF to replace state description
            df_add_final = df_add_after_changes.withColumn('STAT_CD_DESC', state_name_udf('STAT'))
        elif (df_string_change == True):
            print("in elif at string change condition check")
            # Join both dataframes to create one final dataframe
            df_new = df_new.withColumn('row_index', F.monotonically_increasing_id())
            df_data_address = df_new.join(psp_addfile_df_dist, 'row_index').drop('row_index')
            # Replace data file field values with values from OPENADDRESS.IO dataframe
            df_data_address = df_data_address.withColumn('CITY', lit(df_data_address.NEW_CITY))\
                                                        .withColumn('STAT', lit(df_data_address.REGION))\
                                                        .withColumn('ZIP', lit(df_data_address.POSTCODE))\
                                                        .withColumn('PRPTY_ADDR_LN1', lit(df_data_address.ADDR_LN_1))\
                                                        .withColumn('PRPTY_ADDR_LN2', lit(df_data_address.ADDR_LN_2))


            df_drop_list = ['NEW_CITY','REGION','POSTCODE','ADDR_LN_1','ADDR_LN_2']
            df_add_after_changes = df_data_address.drop(*df_drop_list)
            # Use UDF to replace state description
            df_add_final = df_add_after_changes.withColumn('STAT_CD_DESC', state_name_udf('STAT'))

        
