#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  3 14:33:01 2021

@author: miguelcunha
"""
import unittest
import AssignmentCode as ac
from google.cloud import storage
import pandas as pd
from io import StringIO
import csv
import os
import glob
import numpy as np


os.chdir('/Users/miguelcunha/Documents/GitHub/Lunar-Assignment/testdata')


class TestGetDate(unittest.TestCase):
    def test1(self):
        self.assertEqual(ac.getdate('lander_saturn_20210301_023005.csv'), '20210301023005')
        self.assertEqual(ac.getdate('landesdadr_satfjsdifburn_1000000_999999.csv'), '1000000999999')
unittest.main()



if __name__ == "__main__":
    
    #gets all the filenames in the directory 'data'
    entries = os.listdir(os.getcwd())

    tablenames = ['lander_saturn', 'lander_venus', 'rocket_saturn', 'rocket_venus']
    path = "/Users/miguelcunha/Documents/GitHub/Lunar-Assignment/testdata"

    
    for name in tablenames:
        #gets all the files starting with tablenames
        all_files = glob.glob(os.path.join(path, name + "*.csv"))
    
        # list with iterable panda objects
        all_data = []
        for f in all_files:
            #reads the csv file
            data_f = pd.read_csv(f)
            #gets data from the filename
            file_date = ac.getdate(f)
            #applies the transformations to the Dataframe
            data_f = ac.transform(data_f,file_date)
            all_data.append(data_f)
        
            
        if all_data == []:
            print("There are no files to concatenate in for ", name)
        else:
            data_merged = pd.concat(all_data, ignore_index=True)
            #saves to new csv file with file name
            data_merged.to_csv(name + ".csv") ;
            
            
            
            
            
        