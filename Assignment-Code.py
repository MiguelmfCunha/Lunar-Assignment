#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 29 14:34:57 2021

This file contains all the code used in order to perform the ETL process.
As the process denotes, the first step consists in extracting the data from GCP.

"""


## First Step of ETL process -> Extract the Data ##
from google.cloud import storage
storage.client.Client.create_anonymous_client()