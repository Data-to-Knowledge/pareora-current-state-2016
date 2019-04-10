# -*- coding: utf-8 -*-
"""
Created on Fri Aug  3 15:28:28 2018

@author: MichaelEK
"""
import os
import pandas as pd
import geopandas as gpd
from gistools import util, rec

pd.options.display.max_columns = 10

####################################
### Parameters

py_path = os.path.realpath(os.path.dirname(__file__))
base_path = os.path.split(py_path)[0]

shp_dir = 'shapefiles'

sites_shp = 'flow_recorders_pareora.shp'
rec_streams_shp = 'rec_streams_pareora.shp'
rec_catch_shp = 'rec_catch_pareora.shp'
catch_shp = 'catchment_pareora.shp'

sites_shp_path = os.path.join(base_path, shp_dir, sites_shp)
rec_streams_shp_path = os.path.join(base_path, shp_dir, rec_streams_shp)
rec_catch_shp_path = os.path.join(base_path, shp_dir, rec_catch_shp)
catch_shp_path = os.path.join(base_path, shp_dir, catch_shp)

sites_col_name = 'SITENUMBER'
poly_col_name = 'Catchmen_1'
line_site_col = 'NZREACH'

export_shp = 'pareora_catchments.shp'

#######################################
### Catchment delineation

pts = util.load_geo_data(sites_shp_path)
pts['geometry'] = pts.geometry.simplify(1)

poly1 = rec.catch_delineate(sites_shp_path, rec_streams_shp_path, rec_catch_shp_path, sites_col=sites_col_name, buffer_dis=400)

poly1.to_file(os.path.join(base_path, shp_dir, export_shp))


