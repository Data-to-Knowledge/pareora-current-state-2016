# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 13:42:09 2019

@author: michaelek
"""
import os
import pandas as pd
import geopandas as gpd
from pdsql import mssql
from allotools import AlloUsage
from gistools import vector

pd.options.display.max_columns = 10

############################################
### Parameters

catch_group = ['Pareora River']

from_date = '1982-07-01'
to_date = '2015-06-30'

source_path = r'E:\ecan\shared\base_data\usage'

usage_hdf = 'sd_est_all_mon_vol.h5'

py_path = os.path.realpath(os.path.dirname(__file__))
base_path = os.path.split(py_path)[0]

shp_dir = 'input_files'
output_dir = 'output_files'

catch_del_shp = 'pareora_catchments.shp'
catch_del_shp_path = os.path.join(base_path, shp_dir, catch_del_shp)

rec_site = 70105

usage_output = 'pareora_huts_usage_mon.csv'

############################################
### Extract data

site_filter = {'CatchmentGroupName': catch_group}

a1 = AlloUsage(from_date, to_date, site_filter=site_filter)

sites0 = a1.sites.reset_index().copy()

sites = vector.xy_to_gpd('wap', 'NZTMX', 'NZTMY', sites0)

catch_del = gpd.read_file(catch_del_shp_path)
catch_del.rename(columns={'SITENUMBER': 'site'}, inplace=True)

catch_del1 = catch_del[catch_del.site == rec_site]

sites1 = vector.sel_sites_poly(sites, catch_del1)


## Usage data

usage1 = pd.read_hdf(os.path.join(source_path, usage_hdf))

usage2 = usage1[usage1.wap.isin(sites1.wap)].copy()
usage2['time'] = pd.to_datetime(usage2['time'])

usage2.to_csv(os.path.join(base_path, shp_dir, usage_output), index=False)

usage3 = usage2.groupby('time')['sd_usage'].sum()

















