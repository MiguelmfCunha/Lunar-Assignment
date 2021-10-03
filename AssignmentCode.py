#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 29 14:34:57 2021

This file contains all the code used in order to perform the ETL process.
As the process denotes, the first step consists in extracting the data from GCP.
The second part, in transforming the data.
And finally, testing it.

"""


## First Step of ETL process -> Extract the Data ##
from google.cloud import storage
import pandas as pd
from io import StringIO
import csv
import os
import glob
import numpy as np

#set working directory
os.chdir("/Users/miguelcunha/Documents/GitHub/Lunar-Assignment")



def getdate(filename):
    '''
    Parameters
    ----------
    filename : str
        The name of the file from which we wish to extract the date.

    Returns
    -------
    date: str
        Retunrs a date extracted from the name of the file and ready to be transformed into yyyy-MM-dd HH:mm:ss format 
    '''
    parsed = filename.split("_")
    date = parsed[-2]+parsed[-1][:6]
    return date;





def label_size (row):
    '''
    Parameters
    ----------
    row : pandas.core.series.Series
        Row from Dataframe.

    Returns
    -------
    str
        Returns a category 'str' based on the size column.
    '''
    
    if row['size'] < 1:
        return 'Error, size lower then 1.'
    elif row['size'] < 10:
        return 'tiny'
    elif row['size'] < 50:
        return 'small'
    elif row['size'] < 100:
        return 'medium'
    elif row['size'] < 500:
        return 'big'
    elif row['size'] < 1000:
        return 'massive'
    else:
        return 'Error, the size is bigger then 1000.';




#Function tranformation which applies multiple data transformations
def transform (data_f, file_date):
    '''
    Parameters
    ----------
    data_f : pandas.core.frame.DataFrame
        Pandas Dataframe before transformation process.
    file_date : Str
        Str with file date extracted from the file's name

    Returns
    -------
    pandas.core.frame.DataFrame
        Pandas Datafram after transformation process.
    '''
    #create a timestamp
    data_f['timestamp'] = pd.to_datetime(file_date)
    
    # rename Pandas columns to lower case
    data_f.columns= data_f.columns.str.lower()
    
    #new id column
    id_parsed = data_f['id'].str.split('-',  expand = True)

    #get middle code
    data_f['id'] = id_parsed[len(id_parsed.columns)//2]

    # substitutes the old column named 'size' and filters the lines without integer
    data_f['size'] = pd.to_numeric(data_f['size'], errors='coerce', downcast=('integer'))
    data_f = data_f[data_f['size'].notnull()]
    
    # create magnitude column
    data_f['magnitude'] = data_f.apply (lambda row: label_size(row), axis=1)
   
                 
    
    
    return data_f;





if __name__ == "__main__":

    
    tablenames = ['lander_saturn', 'lander_venus', 'rocket_saturn', 'rocket_venus']
    path = "/Users/miguelcunha/Documents/GitHub/Lunar-Assignment/data"
    
    

    
    for name in tablenames:
        #gets all the files starting with tablenames
        all_files = glob.glob(os.path.join(path, name + "*.csv"))
    
        # list with iterable panda objects
        all_data = []
        for f in all_files:
            #reads the csv file
            data_f = pd.read_csv(f)
            #gets data from the filename
            file_date = getdate(f)
            #applies the transformations to the Dataframe
            data_f = transform(data_f,file_date)
            all_data.append(data_f)
        
            
        if all_data == []:
            print("There are no files to concatenate in for ", name)
        else:
            data_merged = pd.concat(all_data, ignore_index=True)
            #saves to new csv file with file name
            data_merged.to_csv(name + ".csv")     