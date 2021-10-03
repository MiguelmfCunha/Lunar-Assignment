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
import glob
import numpy as np

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

def getname(filename):
    parsed = filename.split("_")
    
    #information in yyyy-MM-dd HH:mm:ss format
    name = parsed[0]+"_"+parsed[0]
    
    return name;

def getdate(filename):
    parsed = filename.split("_")
    
    #information in yyyy-MM-dd HH:mm:ss format
    date = parsed[-2]+parsed[-1][:6]
    
    return date;

#%%
file_name = getname(entries[0])

#%%

'''
    
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

# create magnitude column
#data['magnitude'] = data.apply (lambda row: label_size(row), axis=1)



'''


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



#Function tranformation which applies multiple data transformations
def transform (data_f, date_f):
    #create a timestamp
    data_f['timestamp'] = pd.to_datetime(file_date)
    #new id column
    id_parsed = data_f['id'].str.split('-',  expand = True)


    data_f['id'] = id_parsed[len(id_parsed.columns)//2]

    # rename Pandas columns to lower case
    data_f.columns= data_f.columns.str.lower()

    #substitutes the old column named 'size' and filters the lines without integer
    data_f['size'] = pd.to_numeric(data_f['size'], errors='coerce', downcast=('integer'))
    data_f = data_f[data_f['size'].notnull()]
    
    # create magnitude column
    data_f['magnitude'] = data_f.apply (lambda row: label_size(row), axis=1)
    
    return data_f;
    

#%%

tablenames = ['lander_saturn', 'lander_venus', 'rocket_saturn', 'rocket_venus']
#tablenames = ['lander_saturn']

path = "/Users/miguelcunha/Documents/GitHub/Lunar-Assignment/data"


#final function to be created
for name in tablenames:
    all_files = glob.glob(os.path.join(path, name+"*.csv"))
    
    all_df = []
    for f in all_files:
        
        df_from_each_file = pd.read_csv(f)
        file_date = getdate(f)
        df_from_each_file = transform(df_from_each_file,file_date)  
        all_df.append(df_from_each_file)
        
    df_merged   = pd.concat(all_df, ignore_index=True)
    
    df_merged.to_csv( name+".csv")



#%%

'''
#create 4 data frames
data_keys = {}

def getuniquename(files):
    
    new = []
    for file in files:
        new.append(file[:9])
        
    return np.unique(np.array(new));
        
#print(getuniquename(entries))
    
test =  entries[:][:9]
new_test = test[:][:9]
#data_keys[]
for file in entries:
    ;
    

#%%

def main ():
    
    
    
    return;


#%%



#%%
'''
