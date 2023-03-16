import random
import subprocess
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re
import requests
import time
import numpy as np
import os
import pandas as pd
import datetime
import calendar
import time
import warnings
import datetime as dt
import time
import gc
from collections import OrderedDict
import math
from collections import Counter
from io import StringIO
import csv

## Import Distributor CSV
df_dist = pd.read_csv('Codes/test_validation.csv', encoding='ISO-8859-1')


def csv_validation_dist(dist_name, df_dist):

  ## Required Columns
  dist_columns = ['item_code', 'brand', 'pack', 'manufacturer', 'catg', 'subcatg', 'mrp']
  df_col = (df_dist.columns).to_list()

  ## Check if columns are correct
  if(sorted(dist_columns) == sorted(df_col)):

    ## Filter NULL item_code or brand  
    df_error1 = df_dist[(df_dist['item_code'].isnull()) | (df_dist['brand'].isnull())]
    df_dist_new = df_dist[~((df_dist['item_code'].isnull()) | (df_dist['brand'].isnull()))]

    ## Filter Special Characters rows
    df_error2 = df_dist_new[~df_dist_new.item_code.str.isnumeric()]
    df_dist_new = df_dist_new[df_dist_new.item_code.str.isnumeric()]

    ## Saving required CSVs
    if df_error1.shape[0]:
      print("Saving NAN rows dataframe, please check!!!")
      df_error1.to_csv(dist_name+ '_NAN.csv', index = False)

    if df_error2.shape[0]:
      print("Saving special characters rows dataframe, please check!!!")
      df_error2.to_csv(dist_name+ '_special_char.csv', index = False)

    ## Returning the filtered CSV 
    return df_dist_new

  else:
    print("Please check the column names")


df = csv_validation_dist("Apollo",df_dist)

df_master = pd.read_csv('Codes/test_validation.csv', encoding='ISO-8859-1')

def csv_validation_master(df_master):

  master_columns = ['id', 'brand', 'medicine_type', 'sku_brand']
  df_col = (df_master.columns).to_list()

  if(sorted(df_master) == sorted(df_col)):
    
    df_error1 = df_master[(df_master['id'].isnull()) | (df_master['brand'].isnull()) | (df_master['sku_brand'].isnull())]
    df_master_new = df_master[~((df_master['id'].isnull()) | (df_master['brand'].isnull()) | (df_master['sku_brand'].isnull()))]

    df_error2 = df_master_new[~df_master_new.id.str.isnumeric()]
    df_master_new = df_master_new[df_master_new.id.str.isnumeric()]

    if df_error1.shape[0]:
      print("Saving NAN rows dataframe, please check!!!")
      df_error1.to_csv('Master_NAN.csv', index = False)

    if df_error2.shape[0]:
      print("Saving special characters rows dataframe, please check!!!")
      df_error2.to_csv('Master_special_char.csv', index = False)
    
    return df_master_new

  else:
    print("Please check the column names")


x = csv_validation_master(df_master)
print(x)

