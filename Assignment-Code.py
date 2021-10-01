#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 29 14:34:57 2021

This file contains all the code used in order to perform the ETL process.
As the process denotes, the first step consists in extracting the data from GCP.

"""


## First Step of ETL process -> Extract the Data ##
from google.cloud import storage
import pandas as pd
from io import StringIO
import csv
import os

#%%


#Fetch the data -> Version 1
"""
Using the following code line on the comandline is possible to download the entire set of data in
the designated bucket. However, it performs it in a single batch.


gsutil cp -r gs://de-assignment-data-bucket/data .


However, according to gsutil documentation:
    If you have a large number of files to transfer, you can perform a parallel 
    multi-threaded/multi-processing copy using the top-level gsutil -m option (see gsutil help options):


gsutil -m -o "Boto:parallel_thread_count=25" cp -r gs://de-assignment-data-bucket/data .


"""



#%%

#get list of all the filenames in data
entries = os.listdir('data')



def getdate(filename):
    parsed = filename.split("_")
    
    #information in yyyy-MM-dd HH:mm:ss format
    date = parsed[-2]+parsed[-1][:6]
    
    return date;
    
print(getdate(entries[0]))

# SHALL I USE PANDAS OR ARRAYS

#for file in entries:

file_date = getdate(entries[0])
    
data = pd.read_csv('data/'+entries[0])
data['timestamp'] = pd.to_datetime(file_date)

data['id_new'] = data['id'].str.split('-',  expand = True)[2]

# rename Pandas columns to lower case
data.columns= data.columns.str.lower()
    


#%%
#creates a client with anonymous credentials
client = storage.Client.create_anonymous_client()

#connects to the bucket
bucket = client.bucket('de-assignment-data-bucket')

#retrieves the list of all the files in the bucket
blob = bucket.blob('data/rocket_venus_20210331_021752.csv')
blob = blob.download_as_string()
blob = blob.decode('utf-8')

blob = StringIO(blob)  #tranform bytes to string here
names = pd.read_csv(blob)  #then use pandas library to read the content into a dataframe

    
    
#blob = storage.Blob("lander_saturn_20210301_013306.csv", bucket)
'''
csv_list = ["lander_saturn_20210301_013306.csv", "lander_venus_20210301_003124.csv", 
            "rocket_saturn_20210301_121033.csv", "rocket_venus_20210308_035720.csv" ]
'''



#blob = bucket.get_blob('data/lander_saturn_20210301_013306.csv')

#a = blob.download_as_string("lander_saturn_20210301_013306.csv")


#%%

