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
    
#print(getdate(entries[0]))

# SHALL I USE PANDAS OR ARRAYS

#for file in entries:

#get file date from name
file_date = getdate(entries[0])

#csv to dataframe    
data = pd.read_csv('data/'+entries[0])

#create a timestamp
data['timestamp'] = pd.to_datetime(file_date)

#new id column
id_parsed = data['id'].str.split('-',  expand = True)


data['id'] = id_parsed[len(id_parsed.columns)//2]

# rename Pandas columns to lower case
data.columns= data.columns.str.lower()

#substitutes the old column named 'size' and filters the lines without integer
data['size'] = pd.to_numeric(data['size'], errors='coerce', downcast=('integer'))
data = data[data['size'].notnull()]




def label_size (row):
    
    #print(row['size'])
    #print(type(row['size']))
    
    if row['size'] <= 10 and row['size'] >= 1:
        return 'tiny'
    
    if row['size'] < 50:
        return 'small'
    
    if row['size'] < 100:
        return 'medium'
    if row['size'] < 500:
        return 'big'
    if row['size'] < 1000:
        return 'massive'
    else:
        return 'Error';

# create magnitude column
data['magnitude'] = data.apply (lambda row: label_size(row), axis=1)

#create column magnitude
def main ():
    
    return;


#%%



#%%

