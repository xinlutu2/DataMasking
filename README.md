# DataFieldsReplacement
Replace field values based on input criteria mentioned in the Scrubbing needs file. The address related fields are replaced with addresses from openaddresses.io dataset.

The script uses PySpark and is intended to be run on a cluster in order to acheive high processing speeds. 

This python script takes 3 inputs,

1. Input File name
2. Scrubbing Needs file Name
3. Output Directory location
