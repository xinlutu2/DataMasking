# DataFieldsReplacement
Replace field values based on input criteria mentioned in the Scrubbing needs file. The address related fields are replaced with addresses from openaddresses.io dataset.

The script uses PySpark and is intended to be run on a cluster in order to acheive high processing speeds. 

This python script takes 4 inputs,

1. Input file name
2. Address file
3. Scrubbing needs file Name
4. Output directory location
